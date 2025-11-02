package org.pipeline.trips;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.pipeline.trips.avro.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.mongodb.FindQuery;


public class TripsBatchMongoToGcs_scheduled {

    private static final Logger LOG = LoggerFactory.getLogger(TripsBatchMongoToGcs_scheduled.class);

    // Helper method to build the filter document
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
    	MongoToGcsOptions_scheduled options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MongoToGcsOptions_scheduled.class);
        
        Pipeline pipeline = Pipeline.create(options);

        // Incremental logic
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

       // Build MongoDB read
       MongoDbIO.Read mongoRead = MongoDbIO.read()
               .withUri(options.getMongoUri().get())
               .withDatabase(options.getMongoDatabase().get())
               .withCollection(options.getMongoCollection().get())
               .withBucketAuto(false); 

       // Apply incremental filter if enabled
       if (incremental && startDate != null && endDate != null) {
           Document filter = buildIncrementalFilterDocument(startDate, endDate);
           if (filter != null) {
               mongoRead = mongoRead.withQueryFn(FindQuery.create().withFilters(filter));
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
                .apply("MapToAvro", ParDo.of(new ConvertDocumentToTripFn()));

        // 2. Reshuffle (This is critical to prevent timeouts)
        PCollection<Trip> tripsReshuffled = trips
                .apply("BreakFusion", Reshuffle.viaRandomKey());
        
        // 3. Convert to TableRow and write to BigQuery
        tripsReshuffled.apply("ConvertToTableRow", ParDo.of(new TripToTableRowConverter.ConvertToTableRowFn()))
        .apply("WriteToBigQueryStaging", BigQueryIO.writeTableRows()
                .to(options.getBigQueryTable())
                .withSchema(TripBigQuerySchema.getBigQuerySchema()) // Use the schema file
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
        );
        
        pipeline.run();
        LOG.info("Pipeline started. Reading from Mongo and writing to BigQuery.");
    }
}