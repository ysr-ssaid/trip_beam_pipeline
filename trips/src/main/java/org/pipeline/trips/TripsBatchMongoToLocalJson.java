package org.pipeline.trips;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.io.mongodb.FindQuery;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.io.TextIO; // Import TextIO
import com.google.api.services.bigquery.model.TableRow; // Import TableRow
import com.google.gson.Gson; // Import Gson
import org.bson.Document;
import org.pipeline.trips.avro.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripsBatchMongoToLocalJson {

    private static final Logger LOG = LoggerFactory.getLogger(TripsBatchMongoToLocalJson.class);

    // --- NEW OPTIONS INTERFACE FOR LOCAL RUN ---
    // It inherits from your old one but adds a local output file
    // and removes the BigQuery table option.
    public interface MongoToLocalJsonOptions extends MongoToGcsOptions_scheduled {

        @Description("Path of the file to write to.")
        @Default.String("./local-output/trips") // Default local path
        String getOutputFile();
        void setOutputFile(String value);
    }
    // --- END OF NEW OPTIONS ---


    // Helper method (copied exactly from your original)
    private static Document buildIncrementalFilterDocument(String startDate, String endDate) {
        if (startDate == null || startDate.isEmpty() || endDate == null || endDate.isEmpty()) {
            return null;
        }
        
        try {
            java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd");
            
            // 1. Parse Start Date (Inclusive)
            java.time.LocalDate startLocalDate = java.time.LocalDate.parse(startDate, formatter);
            java.util.Date startDateObj = java.sql.Date.valueOf(startLocalDate);
            
            // 2. Parse End Date and add 1 day to make the range exclusive
            java.time.LocalDate endLocalDate = java.time.LocalDate.parse(endDate, formatter);
            java.time.LocalDate endDateExclusive = endLocalDate.plusDays(1);
            java.util.Date endDateObj = java.sql.Date.valueOf(endDateExclusive);

            // Build filter for the time window: { $gte: startDate, $lt: endDateExclusive }
            Document dateWindowFilter = new Document("$gte", startDateObj)
                                          .append("$lt", endDateObj);
            
            // Filter 1: updated_at is in the window
            Document updatedAtFilter = new Document("updated_at", dateWindowFilter);
            
            // Filter 2: updated_at is null AND created_at is in the window
            Document missingUpdatedAt = new Document("updated_at", 
                new Document("$exists", false));
            Document createdAtFilter = new Document("created_at", dateWindowFilter);
            
            Document andFilter = new Document("$and", 
                java.util.Arrays.asList(missingUpdatedAt, createdAtFilter));
            
            // Combine with $or
            return new Document("$or", 
                java.util.Arrays.asList(updatedAtFilter, andFilter));
            
        } catch (Exception e) {
            LOG.error("Error building incremental filter: {}", e.getMessage(), e);
            return null;
        }
    }

    public static void main(String[] args) {
        MongoToLocalJsonOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MongoToLocalJsonOptions.class);
        
        
        Pipeline pipeline = Pipeline.create(options);

        // Incremental logic (copied exactly)
        boolean incremental = options.getIncremental() != null && 
                              Boolean.TRUE.equals(options.getIncremental().get());
        
        String startDate = null;
        String endDate = null;
        
        if (incremental) {
            if (options.getStartDate() != null && options.getStartDate().get() != null) {
                startDate = options.getStartDate().get();
                LOG.info("Using provided start date: {}", startDate);
            }
            if (options.getEndDate() != null && options.getEndDate().get() != null) {
                endDate = options.getEndDate().get();
                LOG.info("Using provided end date: {}", endDate);
            }
            if (startDate == null || endDate == null) {
                LOG.warn("Incremental mode is true but startDate or endDate was not provided. Running a full refresh.");
                incremental = false;
            }
        }

        // Build MongoDB read (copied exactly)
        MongoDbIO.Read mongoRead = MongoDbIO.read()
                .withUri(options.getMongoUri().get())
                .withDatabase(options.getMongoDatabase().get())
                .withCollection(options.getMongoCollection().get())
                .withBucketAuto(false); 
        

        // Apply incremental filter if enabled (copied exactly)
        if (incremental && startDate != null && endDate != null) {
            Document filter = buildIncrementalFilterDocument(startDate, endDate);
            if (filter != null) {
                mongoRead = mongoRead.withQueryFn(
                        FindQuery.create().withFilters(filter).withLimit(1000)
                    );
                java.time.LocalDate parsedEndDate = java.time.LocalDate.parse(endDate, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                String exclusiveEnd = parsedEndDate.plusDays(1).toString();
                LOG.info("Applying incremental filter for window: [{}, {})", startDate, exclusiveEnd);
            } else {
                LOG.warn("Incremental filter was null, running full refresh.");
            }
        } else {
            LOG.info("Running full refresh");
        }
        
        // 1. Read from MongoDB and convert to Avro Trip
        PCollection<Trip> trips = pipeline
                .apply("ReadFromMongoDB", mongoRead)
                .apply("MapToAvro", ParDo.of(new ConvertDocumentToTripFn())); // Uses your logic

        // 2. Reshuffle
        PCollection<Trip> tripsReshuffled = trips
                .apply("BreakFusion", Reshuffle.viaRandomKey());
        
        // 3. Convert to TableRow
        PCollection<TableRow> tableRows = tripsReshuffled
                .apply("ConvertToTableRow", ParDo.of(new TripToTableRowConverter.ConvertToTableRowFn()));
        
        // --- START OF NEW SINK LOGIC ---
        
        // 4. Convert TableRow to JSON String
        tableRows.apply("ConvertToJsonString", MapElements.into(TypeDescriptors.strings())
                    .via((TableRow row) -> {
                        // We use Gson to ensure it's a standard JSON string
                        // This is what BigQueryIO *tries* to do
                        return new Gson().toJson(row);
                    }))
        
        // 5. Write to a single local file
        .apply("WriteToLocalJson", TextIO.write()
                .to(options.getOutputFile())
                .withSuffix(".jsonl")     // .jsonl for JSON-Lines
                .withoutSharding());      // Force a single output file
        
        // --- END OF NEW SINK LOGIC ---
        
        pipeline.run().waitUntilFinish(); // Use waitUntilFinish() for local runs
        
        LOG.info("Pipeline finished. Output written to {}.jsonl", options.getOutputFile());
    }
}