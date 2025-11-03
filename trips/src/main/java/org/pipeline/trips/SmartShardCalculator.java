//number of files in a single partition, calculate dynamically
//not fully tested on production
//needs more investigations
//default 0, if left empty
package org.pipeline.trips;

public class SmartShardCalculator {

    public static int calculateForTripsData(long dailyTrips, int targetFileSizeMB) {
        // specific trips data calculation
        int avgTripRecordSizeBytes = 2048; 

        return calculateShards(dailyTrips, avgTripRecordSizeBytes, targetFileSizeMB);
    }

    public static int calculateShards(long estimatedDailyRecords, int avgRecordSizeBytes,
                                      int targetFileSizeMB) {
        if (estimatedDailyRecords <= 0) {
            return 2; // Reasonable default
        }

        // Calculate daily data volume
        long dailyBytes = estimatedDailyRecords * avgRecordSizeBytes;
        long dailyDataSizeMB = dailyBytes / (1024 * 1024);

        // avoid division by 0, make sure targetFileSizeMB is at least 1
        int safeTargetFileSizeMB = Math.max(1, targetFileSizeMB);

        // calculate shards (ceiling division)
        int calculatedShards = (int) Math.ceil((double) dailyDataSizeMB / safeTargetFileSizeMB);

        // apply reasonable bounds (1-20 shards for better performance)
        int finalShards = Math.max(1, Math.min(calculatedShards, 20));

        System.out.println("Shard Calculation:");
        System.out.println("  - Daily records: " + estimatedDailyRecords);
        System.out.println("  - Daily data volume: " + dailyDataSizeMB + " MB");
        System.out.println("  - Target file size: " + safeTargetFileSizeMB + " MB");
        System.out.println("  - Calculated shards (bounded 1-20): " + finalShards);

        return finalShards;
    }
}
