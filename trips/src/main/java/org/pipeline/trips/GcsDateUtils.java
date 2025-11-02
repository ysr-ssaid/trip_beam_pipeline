package org.pipeline.trips;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GcsDateUtils {
    
    private static final Pattern DATE_PATTERN = Pattern.compile("updateDate=(\\d{4}-\\d{2}-\\d{2})");
    
    public static String findMaxDateDay(String gcsPath) {
        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            
            // Extract bucket and prefix from gs://bucket/path
            String[] parts = gcsPath.replace("gs://", "").split("/", 2);
            String bucketName = parts[0];
            String prefix = parts.length > 1 ? parts[1] : "";
            
            // List all blobs with the prefix
            Iterable<Blob> blobs = storage.list(bucketName, 
                Storage.BlobListOption.prefix(prefix),
                Storage.BlobListOption.currentDirectory()).iterateAll();
            
            String maxDate = null;
            
            for (Blob blob : blobs) {
                String blobName = blob.getName();
                Matcher matcher = DATE_PATTERN.matcher(blobName);
                
                if (matcher.find()) {
                    String dateStr = matcher.group(1);
                    
                    if (maxDate == null || dateStr.compareTo(maxDate) > 0) {
                        maxDate = dateStr;
                    }
                }
            }
            
            return maxDate;
            
        } catch (StorageException e) {
            System.err.println("Error accessing GCS: " + e.getMessage());
            return null;
        }
    }
}