package org.pipeline.trips;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.bson.Document;
import org.pipeline.trips.avro.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertDocumentToTripFn extends DoFn<Document, Trip> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ConvertDocumentToTripFn.class);

    private final Counter successCounter = Metrics.counter(ConvertDocumentToTripFn.class, "success_count");
    private final Counter errorCounter = Metrics.counter(ConvertDocumentToTripFn.class, "error_count");
    private final Counter nullCounter = Metrics.counter(ConvertDocumentToTripFn.class, "null_document_count");
    private final Counter skippedCounter = Metrics.counter(ConvertDocumentToTripFn.class, "skipped_invalid_id_count");

    @ProcessElement
    public void processElement(@Element Document input, OutputReceiver<Trip> out) {
        if (input == null) {
            nullCounter.inc();
            LOG.warn("Received null document, skipping.");
            return;
        }

        String docIdForLogging = "unknown"; // default for logging
        try {
            Object idObject = input.get("_id");
            if (idObject != null) {
                docIdForLogging = idObject.toString();
            }

            Trip trip = TripBuilder.buildTrip(input); 

            if (trip == null) {
                skippedCounter.inc();
                LOG.warn("Skipping document with non-ObjectId _id: {}", docIdForLogging);
                return;
            }

            docIdForLogging = trip.getMongoId().toString();

            out.output(trip);
            successCounter.inc();

        } catch (Throwable t) {
            errorCounter.inc();

            if (t instanceof OutOfMemoryError) {
                LOG.error("!!! OUT OF MEMORY ERROR !!! processing document ID: {}. Skipping document. Error: {}",
                        docIdForLogging, t.getMessage());
            } else {
                LOG.error("CRITICAL ERROR processing document ID: {} - Error: {}", docIdForLogging, t.getMessage(), t);
            }
        }
    }
}