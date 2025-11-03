package org.pipeline.trips;

import org.bson.Document;
import org.bson.types.ObjectId;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;


public class MongoAvroUtils {
    public static String safeGetString(Document doc, String key) {
        if (doc == null) return null;
        Object val = doc.get(key);
        if (val instanceof ObjectId) return ((ObjectId) val).toHexString();
        return (val instanceof String) ? (String) val : null;
    }

    public static Integer safeGetInt(Document doc, String key) {
         if (doc == null) return null;
        Object val = doc.get(key);
         if (val instanceof Long) { 
             long longVal = (Long) val;
             if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) return (int) longVal;
             else { return null; }
         }
        return (val instanceof Number) ? ((Number) val).intValue() : null;
    }

    public static Double safeGetDouble(Document doc, String key) {
        if (doc == null) return null;
        Object val = doc.get(key);
        return (val instanceof Number) ? ((Number) val).doubleValue() : null;
    }

    public static Boolean safeGetBoolean(Document doc, String key) {
        if (doc == null) return null;
        Object val = doc.get(key);
        if (val instanceof String) {
            String sVal = ((String) val).toLowerCase();
            if (sVal.equals("true")) return true;
            if (sVal.equals("false")) return false;
        }
        return (val instanceof Boolean) ? (Boolean) val : null;
    }

    public static Instant safeGetInstant(Document doc, String key) {
        if (doc == null) return null;
        Object val = doc.get(key);
        return (val instanceof Date) ? ((Date) val).toInstant() : null;
    }

 // safely get a list, checking the type
    @SuppressWarnings("unchecked") // Suppress unavoidable generic cast warnings
    public static <T> List<T> safeGetList(Document doc, String key, Class<T> elementType) {
        if (doc == null) {
            return null;
        }
        Object val = doc.get(key);
        if (val instanceof List) {
            List<?> rawList = (List<?>) val; // first cast to list of unknown type

            // check if the list is empty before trying to access elements
            if (!rawList.isEmpty()) {
                Object firstElement = rawList.get(0); // get the first

                // perform type check only if the first element is not null
                if (firstElement != null && !elementType.isInstance(firstElement)) {
                    // special handling for String vs CharSequence
                    boolean isStringCharSequenceMismatch = (elementType == String.class && firstElement instanceof CharSequence);
                    // special handling for avro record vs BSON Document
                    boolean isExpectingRecord = org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(elementType);
                    boolean isActualDocument = firstElement instanceof Document;
                    boolean isRecordDocumentMismatch = isExpectingRecord && isActualDocument;

                    // only log a warning if it's not one of the expected/handled mismatches
                    if (!isStringCharSequenceMismatch && !isRecordDocumentMismatch) {
                        System.err.println("Warning: List element type mismatch for key '" + key +
                                           "'. Expected List<" + elementType.getName() +
                                           "> but found List<" + firstElement.getClass().getName() + "> (based on first element).");
                    }
                }
            }
            try {
                return (List<T>) rawList;
            } catch (ClassCastException e) {
                System.err.println("CRITICAL WARNING: Could not cast list for key '" + key +
                                   "'. Expected List<" + elementType.getName() + "> but found mixed types. Returning empty list.");
                return Collections.emptyList(); // return empty list on critical cast failure
            }
        }
        return null; // return null if the field is not a list
    }

    // Helper to safely get a Document (nested object)
    public static Document safeGetDocument(Document doc, String key) {
        if (doc == null) return null;
        try {
             return doc.get(key, Document.class); // BSON driver handles type check
        } catch (ClassCastException e){
            System.err.println("Warning: Field '" + key + "' is not a Document/Object.");
            return null;
        }
    }

 
}