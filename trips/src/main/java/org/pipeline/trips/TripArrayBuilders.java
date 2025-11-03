//build the fields from avro schema with safe cast
//check MongoAvroUtils.java for safe cast helper functions
package org.pipeline.trips;

import org.bson.Document;
import org.pipeline.trips.avro.*;


import static org.pipeline.trips.MongoAvroUtils.*;
import static org.pipeline.trips.TripAvroBuilderUtils.*;

public class TripArrayBuilders {

    public static AdjustedTo buildAdjustedTo(Document doc) {
        if (doc == null) return null;
        return AdjustedTo.newBuilder()
            .setTripId(safeGetString(doc, "trip_id"))
            .setTripUid(safeGetString(doc, "trip_uid"))
            .setStatus(safeGetString(doc, "status"))
            .setAdjustedToId(safeGetString(doc, "_id"))
            .build();
    }

    public static Candidate buildCandidate(Document doc) {
        if (doc == null) return null;
        return Candidate.newBuilder()
            .setDriver(safeGetString(doc, "driver"))
            .setAssignedAt(safeGetInstant(doc, "assigned_at"))
            .setRejectedAt(safeGetInstant(doc, "rejected_at"))
            .setRejectionType(safeGetString(doc, "rejection_type"))
            .setCandidateId(safeGetString(doc, "_id"))
            .build();
    }

    public static CostBreakdownSubTrip buildCostBreakdownSubTrip(Document doc) {
        if (doc == null) return null;
        return CostBreakdownSubTrip.newBuilder()
            .setInitialCost(safeGetInt(doc, "initial_cost"))
            .setOriginalCost(safeGetInt(doc, "original_cost"))
            .setNetCostInitial(safeGetDouble(doc, "net_cost_initial"))
            .setNetCostPostDiscounts(safeGetInt(doc, "net_cost_post_discounts"))
            .setTotalTaxes(safeGetInt(doc, "total_taxes"))
            .setTotalTaxesRider(safeGetInt(doc, "total_taxes_rider"))
            .setTotalTaxesDriver(safeGetInt(doc, "total_taxes_driver"))
            .setCostBreakdownSubTripId(safeGetString(doc, "_id"))
            .build();
    }

    public static ReviewCount buildReviewCount(Document doc) {
        if (doc == null) return null;
        return ReviewCount.newBuilder()
            .setCode(safeGetString(doc, "code"))
            .setComment(safeGetString(doc, "comment"))
            .setCount(safeGetInt(doc, "count"))
            .setReviewCountId(safeGetString(doc, "_id"))
            .build();
    }

    public static StatusHistory buildStatusHistory(Document doc) {
        if (doc == null) return null;
        return StatusHistory.newBuilder()
            .setStatusHistoryId(safeGetString(doc, "_id"))
            .setDriver(safeGetString(doc, "driver"))
            .setInsertFrom(safeGetString(doc, "insertFrom"))
            .setLocation(buildDriverLocation(safeGetDocument(doc, "location")))
            .setStatus(safeGetString(doc, "status"))
            .setTime(safeGetInstant(doc, "time"))
            .build();
    }

    public static StopPointDetails buildStopPointDetails(Document doc) {
        if (doc == null) return null;
        return StopPointDetails.newBuilder()
            .setLvl0Id(safeGetString(doc, "lvl0_id"))
            .setLvl1Id(safeGetString(doc, "lvl1_id"))
            .setLvl2Id(safeGetString(doc, "lvl2_id"))
            .setLvl3Id(safeGetString(doc, "lvl3_id"))
            .setLvl0Label(safeGetString(doc, "lvl0_label"))
            .setLvl1Label(safeGetString(doc, "lvl1_label"))
            .setLvl2Label(safeGetString(doc, "lvl2_label"))
            .setLvl3Label(safeGetString(doc, "lvl3_label"))
            .setMapPath(safeGetString(doc, "map_path"))
            .setMapAddress(safeGetString(doc, "map_address"))
            .setLat(safeGetString(doc, "lat"))
            .setLng(safeGetString(doc, "lng"))
            .setZoneCode(safeGetString(doc, "zone_code"))
            .setStopPointDetailsId(safeGetString(doc, "_id"))
            .build();
    }

    public static PricingLineSimple buildPricingLineSimple(Document doc) {
        if (doc == null) return null;
        return PricingLineSimple.newBuilder()
            .setPickup(safeGetString(doc, "pickup"))
            .setDestination(safeGetString(doc, "destination"))
            .setTarifs(buildTarifsSimple(safeGetDocument(doc, "tarifs")))
            .setVariant(safeGetString(doc, "variant"))
            .build();
    }

    public static DriverTax buildDriverTax(Document doc) {
        if (doc == null) return null;
        return DriverTax.newBuilder()
            .setAmount(safeGetDouble(doc, "amount"))
            .setLabel(buildLabel(safeGetDocument(doc, "label")))
            .setCode(safeGetString(doc, "code"))
            .setDriverTaxId(safeGetString(doc, "_id"))
            .build();
    }

    public static CommissionHistory buildCommissionHistory(Document doc) {
        if (doc == null) return null;
        return CommissionHistory.newBuilder()
            .setNewPercentValue(safeGetDouble(doc, "newPercentValue"))
            .setOldPercentValue(safeGetDouble(doc, "oldPercentValue"))
            .setReductionId(safeGetString(doc, "reductionId"))
            .setReductionName(safeGetString(doc, "reductionName"))
            .setType(safeGetString(doc, "type"))
            .build();
    }

    public static StopData buildStopData(Document doc) {
        if (doc == null) return null;
        return StopData.newBuilder()
            .setEta(safeGetInt(doc, "eta"))
            .setDistance(safeGetInt(doc, "distance"))
            .setCost(safeGetInt(doc, "cost"))
            .setPickup(buildStopLocation(safeGetDocument(doc, "pickup")))
            .setDestination(buildStopLocation(safeGetDocument(doc, "destination")))
            .setStatus(safeGetString(doc, "status"))
            .setStopDataId(safeGetString(doc, "_id"))
            .build();
    }
    
    public static DriverLocation buildDriverLocation(Document doc) {
        if (doc == null) return null;
        return DriverLocation.newBuilder()
            .setLat(safeGetDouble(doc, "lat"))
            .setLng(safeGetDouble(doc, "lng"))
            .build();
    }

    public static StopLocation buildStopLocation(Document doc) {
        if (doc == null) return null;
        return StopLocation.newBuilder()
            .setLocation(buildDriverLocation(safeGetDocument(doc, "location")))
            .setFormattedAddress(safeGetString(doc, "formatted_address"))
            .build();
    }

    public static TarifsSimple buildTarifsSimple(Document doc) {
        if (doc == null) return null;
        return TarifsSimple.newBuilder()
            .setMinPrice(safeGetInt(doc, "min_price"))
            .setBasePrice(safeGetInt(doc, "base_price"))
            .setMinutePrice(safeGetInt(doc, "minute_price"))
            .setKmPrice(safeGetInt(doc, "km_price"))
            .build();
    }
}