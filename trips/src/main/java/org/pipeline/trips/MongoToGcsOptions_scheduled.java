package org.pipeline.trips;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Default;

public interface MongoToGcsOptions_scheduled extends DataflowPipelineOptions {

    @Description("MongoDB connection URI")
    ValueProvider<String> getMongoUri();
    void setMongoUri(ValueProvider<String> value);

    @Description("MongoDB database name")
    ValueProvider<String> getMongoDatabase();
    void setMongoDatabase(ValueProvider<String> value);

    @Description("MongoDB collection name")
    ValueProvider<String> getMongoCollection();
    void setMongoCollection(ValueProvider<String> value);

    @Description("GCS output path for Avro files")
    ValueProvider<String> getOutput();
    void setOutput(ValueProvider<String> value);

    @Description("BigQuery table spec. Format: PROJECT:DATASET.TABLE")
    ValueProvider<String> getBigQueryTable();
    void setBigQueryTable(ValueProvider<String> value);

    // enable incremental mode
    @Description("Enable incremental load (true/false)")
    @Default.Boolean(false) // It's good practice to default incremental to false
    ValueProvider<Boolean> getIncremental();
    void setIncremental(ValueProvider<Boolean> value);

    // Override the start date for incremental load
    @Description("Override start date for incremental load (yyyy-MM-dd). Required if incremental=true.")
    ValueProvider<String> getStartDate();
    void setStartDate(ValueProvider<String> value);

    // will be taken into consideration when in future implementation

    @Description("Number of shards per partition for GCS output. Set to 0 or leave blank for auto-calculation.")
    @Default.Integer(0)
    Integer getGcsShards(); // cast to Integer
    void setGcsShards(Integer value);

    @Description("Target file size in MB for GCS output auto-calculation")
    @Default.Integer(256)
    Integer getTargetFileSizeMB(); 
    void setTargetFileSizeMB(Integer value); 
    
    @Description("Estimated daily trips for auto-sharding calculation")
    @Default.Long(150000)
    Long getEstimatedDailyTrips();
    void setEstimatedDailyTrips(Long value);
    
    @Description("Override end date for incremental load (yyyy-MM-dd). This date is INCLUSIVE.")
    ValueProvider<String> getEndDate();
    void setEndDate(ValueProvider<String> value);
}