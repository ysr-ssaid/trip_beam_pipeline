//helper function for data filtering using start and end dates if selected 
//check TripsBatchMongoToGcs_scheduled.java:
//if incremental is set to true in gcloud cli it'll take into consideration the start and end date 
//if incremental is set to true and no start or end dates are set, the pipeline start date (yesterdays +1)
//if incremental is set to false and start & end dates to empty string, the full refresh will be applied

package org.pipeline.trips;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class IncrementalFilterUtils {

    public static Bson buildIncrementalFilter(String startDate) {
        if (startDate == null || startDate.isEmpty()) {
            return null;
        }
        
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            
            // Parse the start date and create Date objects
            LocalDate startLocalDate = LocalDate.parse(startDate, formatter);
            Date startDateObj = java.sql.Date.valueOf(startLocalDate);
            
            return Filters.or(
                Filters.gte("updated_at", startDateObj),
                Filters.and(
                    Filters.exists("updated_at", false),
                    Filters.gte("created_at", startDateObj)
                )
            );
            
        } catch (Exception e) {
            System.err.println("Error building incremental filter: " + e.getMessage());
            return null;
        }
    }

    // Overloaded method that takes LocalDateTime for backward compatibility
    public static Bson buildIncrementalFilter(java.time.LocalDateTime startDateTime) {
        if (startDateTime == null) {
            return null;
        }
        
        Date startDateObj = java.sql.Date.valueOf(startDateTime.toLocalDate());
        
        return Filters.or(
            Filters.gte("updated_at", startDateObj),
            Filters.and(
                Filters.exists("updated_at", false),
                Filters.gte("created_at", startDateObj)
            )
        );
    }
}