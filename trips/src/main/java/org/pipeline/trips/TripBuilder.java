package org.pipeline.trips;

import org.bson.Document;
import org.pipeline.trips.avro.Trip;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.Objects; // Make sure this is imported

// --- Imports added for robust _id handling ---


// -------------------------------------------

import static org.pipeline.trips.MongoAvroUtils.*;
import static org.pipeline.trips.TripAvroBuilderUtils.*;
import static org.pipeline.trips.TripComplexBuilders.*;
import org.bson.types.ObjectId;
public class TripBuilder {
    
    // NO batching or System.gc() constants
    
	public static Trip buildTrip(Document input) {
        if (input == null) {
            throw new IllegalArgumentException("Input document cannot be null");
        }

        // --- START OF NEW LOGIC ---
        // Check the _id type BEFORE doing any work.
        Object idObject = input.get("_id");
        if (!(idObject instanceof ObjectId)) {
            // If _id is null or not an ObjectId, return null to skip it.
            return null; 
        }

        Trip.Builder tripBuilder = Trip.newBuilder();
        
        try {
 
            mapTopLevelFields(input, tripBuilder, (ObjectId) idObject);
            mapSimpleArrays(input, tripBuilder);
            mapNestedArrays(input, tripBuilder);
            mapNestedObjects(input, tripBuilder);
            mapComplexStructures(input, tripBuilder);
            
            return tripBuilder.build();
            
        } catch (Exception e) {
            // The docId will be a valid string here since we checked it
            String docId = ((ObjectId) idObject).toHexString();
            throw new RuntimeException("Error building trip for document ID: " + docId, e);
        }
    }
    
    private static void mapTopLevelFields(Document input, Trip.Builder builder, ObjectId idObject) {
    	builder.setMongoId(idObject.toHexString());
        builder.setSchemaVersion(safeGetInt(input, "__v"));
        builder.setAcceptedAt(safeGetInstant(input, "accepted_at"));
        builder.setArrivedAt(safeGetInstant(input, "arrived_at"));
        builder.setAssignedAt(safeGetInstant(input, "assigned_at"));
        builder.setBaseCost(safeGetDouble(input, "base_cost"));
        builder.setBookedFor(safeGetInstant(input, "booked_for"));
        builder.setBusiness(safeGetString(input, "business"));
        builder.setCampaignCode(safeGetString(input, "campaign_code"));
        builder.setCancelDateTime(safeGetInstant(input, "cancel_date_time"));
        builder.setCarpooling(safeGetString(input, "carpooling"));
        builder.setClient(safeGetString(input, "client"));
        builder.setCompany(safeGetString(input, "company"));
        builder.setCountry(safeGetString(input, "country"));
        builder.setCoupon(safeGetString(input, "coupon"));
        builder.setCouponId(safeGetString(input, "couponId"));
        builder.setCouponLineId(safeGetString(input, "couponLineId"));
        builder.setCreatedAt(safeGetInstant(input, "created_at"));
        builder.setCreatedFromDash(safeGetBoolean(input, "created_from_dash"));
        builder.setCurrency(safeGetString(input, "currency"));
        builder.setDeflationFactorCode(safeGetString(input, "deflation_factor_code"));
        builder.setDirectionsService(safeGetString(input, "directions_service"));
        builder.setDiscount(safeGetDouble(input, "discount"));
        builder.setDiscountedCost(safeGetDouble(input, "discounted_cost"));
        builder.setDispatchCounter(safeGetInt(input, "dispatchCounter"));
        builder.setDispatchTimeout(safeGetInt(input, "dispatch_timeout"));
        builder.setDispatchedAt(safeGetInstant(input, "dispatched_at"));
        builder.setDriver(safeGetString(input, "driver"));
        builder.setEstimatedArrivedAt(safeGetInstant(input, "estimated_arrived_at"));
        builder.setEstimatedCost(safeGetDouble(input, "estimated_cost"));
        builder.setEstimatedCostWithBoost(safeGetDouble(input, "estimated_cost_with_boost"));
        builder.setEstimatedDistance(safeGetInt(input, "estimated_distance"));
        builder.setEstimatedEta(safeGetInt(input, "estimated_eta"));
        builder.setFinalDestinationArrivalNotificationSent(safeGetBoolean(input, "finalDestinationArrivalNotificationSent"));
        builder.setFinishedAt(safeGetInstant(input, "finished_at"));
        builder.setFinishedBy(safeGetString(input, "finished_by"));
        builder.setFraudStatus(safeGetString(input, "fraud_status"));
        builder.setHiddenSurchargeRate(safeGetDouble(input, "hidden_surcharge_rate"));
        builder.setInflationFactorCode(safeGetString(input, "inflation_factor_code"));
        builder.setIsB2BRewardTrip(safeGetBoolean(input, "isB2BRewardTrip"));
        builder.setIsBackToBack(safeGetBoolean(input, "isBackToBack"));
        builder.setIsDelivery(safeGetBoolean(input, "isDelivery"));
        builder.setIsPending3DSAuthentication(safeGetBoolean(input, "isPending3DSAuthentication"));
        builder.setIsReward(safeGetBoolean(input, "isReward"));
        builder.setIsUsedCoupon(safeGetBoolean(input, "isUsedCoupon"));
        builder.setIsBooked(safeGetBoolean(input, "is_booked"));
        builder.setIsNearDestination(safeGetBoolean(input, "is_near_destination"));
        builder.setIsNearPickup(safeGetBoolean(input, "is_near_pickup"));
        builder.setManualStatusOverride(safeGetBoolean(input, "manualStatusOverride"));
        builder.setNoteInternal(safeGetString(input, "note_internal"));
        builder.setNoteRiderInternal(safeGetString(input, "note_rider_internal"));
        builder.setNoteRiderToDriver(safeGetString(input, "note_rider_to_driver"));
        builder.setOriginalCost(safeGetDouble(input, "original_cost"));
        builder.setOriginalCostWithBoost(safeGetDouble(input, "original_cost_with_boost"));
        builder.setOriginalEstimatedCost(safeGetDouble(input, "original_estimated_cost"));
        builder.setPaymentVariant(safeGetString(input, "payment_variant"));
        builder.setPlaceCount(safeGetInt(input, "place_count"));
        builder.setPlannedFor(safeGetInstant(input, "planned_for"));
        builder.setPolyline(safeGetString(input, "polyline"));
        builder.setPostpaidCost(safeGetInt(input, "postpaid_cost"));
        builder.setPrepaidCost(safeGetInt(input, "prepaid_cost"));
        builder.setRadarEnabled(safeGetBoolean(input, "radarEnabled"));
        builder.setRadius(safeGetInt(input, "radius"));
        builder.setRecov(safeGetString(input, "recov"));
        builder.setRecovAck(safeGetBoolean(input, "recov_ack"));
        builder.setRedispatchedFrom(safeGetString(input, "redispatched_from"));
        builder.setReductionFactorCode(safeGetString(input, "reduction_factor_code"));
        builder.setRelatedTo(safeGetString(input, "related_to"));
        builder.setRequested4B2bGuest(safeGetString(input, "requested_4_b2b_guest"));
        builder.setRequested4B2bUser(safeGetString(input, "requested_4_b2b_user"));
        builder.setRequested4B2cGuest(safeGetBoolean(input, "requested_4_b2c_guest"));
        builder.setRequestedAt(safeGetInstant(input, "requested_at"));
        builder.setRider(safeGetString(input, "rider"));
        builder.setRiderPay(safeGetDouble(input, "rider_pay"));
        builder.setRiderRedispatched(safeGetBoolean(input, "rider_redispatched"));
        builder.setService(safeGetString(input, "service"));
        builder.setServiceConfig(safeGetString(input, "service_config"));
        builder.setServiceFamily(safeGetString(input, "service_family"));
        builder.setServicetype(safeGetString(input, "servicetype"));
        builder.setShowPrice4B2bUser(safeGetBoolean(input, "showPrice4B2bUser"));
        builder.setStartedAt(safeGetInstant(input, "started_at"));
        builder.setStatus(safeGetString(input, "status"));
        builder.setSubscriber(safeGetBoolean(input, "subscriber"));
        builder.setTollCost(safeGetInt(input, "toll_cost"));
        builder.setTripUid(safeGetString(input, "trip_uid"));
        builder.setUpdatedAt(safeGetInstant(input, "updated_at"));
        builder.setUpdateDate(calculateUpdateDate(input));
    }
    
    private static void mapSimpleArrays(Document input, Trip.Builder builder) {
        List<String> assignedDriversList = safeGetList(input, "assigned_drivers", String.class);
        if (assignedDriversList != null) {
            builder.setAssignedDrivers(new ArrayList<>(assignedDriversList));
        }
        
        List<String> candidatesCanceledList = safeGetList(input, "candidates_canceled", String.class);
        if (candidatesCanceledList != null) {
            builder.setCandidatesCanceled(new ArrayList<>(candidatesCanceledList));
        }
        
        List<String> driverDevicesTokensList = safeGetList(input, "driver_devices_tokens", String.class);
        if (driverDevicesTokensList != null) {
            builder.setDriverDevicesTokens(new ArrayList<>(driverDevicesTokensList));
        }
        
        List<String> multiDeflateList = safeGetList(input, "multistops_deflation_factor_code", String.class);
        if (multiDeflateList != null) {
            builder.setMultistopsDeflationFactorCode(new ArrayList<>(multiDeflateList));
        }
        
        List<String> multiInflateList = safeGetList(input, "multistops_inflation_factor_code", String.class);
        if (multiInflateList != null) {
            builder.setMultistopsInflationFactorCode(new ArrayList<>(multiInflateList));
        }
        
        List<String> recentAssignedDriversList = safeGetList(input, "recent_assigned_drivers", String.class);
        if (recentAssignedDriversList != null) {
            builder.setRecentAssignedDrivers(new ArrayList<>(recentAssignedDriversList));
        }
        
        List<String> riderDevicesTokensList = safeGetList(input, "rider_devices_tokens", String.class);
        if (riderDevicesTokensList != null) {
            builder.setRiderDevicesTokens(new ArrayList<>(riderDevicesTokensList));
        }
        
        // Tags: Array of Array of String
        List<?> tagsRawOuter = safeGetList(input, "tags", List.class);
        if (tagsRawOuter != null) {
            List<List<CharSequence>> tagsTyped = tagsRawOuter.stream()
                .filter(innerListObj -> innerListObj instanceof List)
                .map(innerListObj -> {
                    List<?> innerListRaw = (List<?>) innerListObj;
                    return innerListRaw.stream()
                        .filter(Objects::nonNull)
                        .map(item -> {
                            // Handle ObjectId
                            if (item instanceof org.bson.types.ObjectId) {
                                return ((org.bson.types.ObjectId) item).toHexString();
                            }
                            return item.toString();
                        })
                        .map(item -> (CharSequence) item)
                        .collect(Collectors.toList());
                })
                .collect(Collectors.toList());
            builder.setTags(tagsTyped);
        }
    }
    
    private static void mapNestedArrays(Document input, Trip.Builder builder) {
        List<Document> adjustedToListRaw = safeGetList(input, "adjusted_to", Document.class);
        if (adjustedToListRaw != null) {
            builder.setAdjustedTo(adjustedToListRaw.stream()
                .map(TripArrayBuilders::buildAdjustedTo)
                .filter(Objects::nonNull) // Use Objects::nonNull
                .collect(Collectors.toList()));
        }

        List<Document> appliedTaxesListRaw = safeGetList(input, "applied_taxes", Document.class);
        if (appliedTaxesListRaw != null) {
            builder.setAppliedTaxes(appliedTaxesListRaw.stream()
                .map(TripAvroBuilderUtils::buildAppliedTax)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> appliedTaxesDriverListRaw = safeGetList(input, "applied_taxes_driver", Document.class);
        if (appliedTaxesDriverListRaw != null) {
            builder.setAppliedTaxesDriver(appliedTaxesDriverListRaw.stream()
                .map(TripAvroBuilderUtils::buildAppliedTax)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> appliedTaxesRiderListRaw = safeGetList(input, "applied_taxes_rider", Document.class);
        if (appliedTaxesRiderListRaw != null) {
            builder.setAppliedTaxesRider(appliedTaxesRiderListRaw.stream()
                .map(TripAvroBuilderUtils::buildAppliedTax)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> appliedTaxesRiderVisibleListRaw = safeGetList(input, "applied_taxes_rider_visible", Document.class);
        if (appliedTaxesRiderVisibleListRaw != null) {
            builder.setAppliedTaxesRiderVisible(appliedTaxesRiderVisibleListRaw.stream()
                .map(TripAvroBuilderUtils::buildAppliedTax)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> appliedTaxesRiderVisibleInvoicesListRaw = safeGetList(input, "applied_taxes_rider_visible_invoices", Document.class);
        if (appliedTaxesRiderVisibleInvoicesListRaw != null) {
            builder.setAppliedTaxesRiderVisibleInvoices(appliedTaxesRiderVisibleInvoicesListRaw.stream()
                .map(TripAvroBuilderUtils::buildAppliedTax)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> candidatesListRaw = safeGetList(input, "candidates", Document.class);
        if (candidatesListRaw != null) {
            builder.setCandidates(candidatesListRaw.stream()
                .map(TripArrayBuilders::buildCandidate)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> costBreakdownSubListRaw = safeGetList(input, "cost_breakdown_sub_trip", Document.class);
        if (costBreakdownSubListRaw != null) {
            builder.setCostBreakdownSubTrip(costBreakdownSubListRaw.stream()
                .map(TripArrayBuilders::buildCostBreakdownSubTrip)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> driverReviewCountsRaw = safeGetList(input, "driver_review_counts", Document.class);
        if (driverReviewCountsRaw != null) {
            builder.setDriverReviewCounts(driverReviewCountsRaw.stream()
                .map(TripArrayBuilders::buildReviewCount)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> statusHistoryRaw = safeGetList(input, "status_history", Document.class);
        if (statusHistoryRaw != null) {
            builder.setStatusHistory(statusHistoryRaw.stream()
                .map(TripArrayBuilders::buildStatusHistory)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        List<Document> stopPointsRaw = safeGetList(input, "stops_points_details", Document.class);
        if (stopPointsRaw != null) {
            builder.setStopsPointsDetails(stopPointsRaw.stream()
                .map(TripArrayBuilders::buildStopPointDetails)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }
    }
    
    private static void mapNestedObjects(Document input, Trip.Builder builder) {
        builder.setCurrencyLabel(buildLabel(safeGetDocument(input, "currency_label")));
        builder.setPickup(buildLocation(safeGetDocument(input, "pickup")));
        builder.setDestination(buildLocation(safeGetDocument(input, "destination")));
        builder.setPickupDetails(buildLocationDetails(safeGetDocument(input, "pickup_details")));
        builder.setDestinationDetails(buildLocationDetails(safeGetDocument(input, "destination_details")));
        builder.setDriverCancelReason(buildCancelReason(safeGetDocument(input, "driver_cancel_reason")));
        builder.setRiderCancelReason(buildCancelReason(safeGetDocument(input, "rider_cancel_reason")));
        builder.setDriverDevice(buildDeviceProfile(safeGetDocument(input, "driver_device")));
        builder.setRiderDevice(buildDeviceProfile(safeGetDocument(input, "rider_device")));
        builder.setDriverReview(buildReview(safeGetDocument(input, "driver_review")));
        builder.setEpay(buildEpay(safeGetDocument(input, "epay")));
    }
    
    private static void mapComplexStructures(Document input, Trip.Builder builder) {
        builder.setB2bTripRequestApproval(buildB2BTripRequestApproval(safeGetDocument(input, "b2b_trip_request_approval")));
        builder.setBookingConfigs(buildBookingConfigs(safeGetDocument(input, "bookingConfigs")));
        builder.setBoost(buildBoost(safeGetDocument(input, "boost")));
        builder.setChallenge(buildChallenge(safeGetDocument(input, "challenge")));
        builder.setCostBreakdown(buildCostBreakdown(safeGetDocument(input, "cost_breakdown")));
        builder.setDataSaveAllChangesOnTripPricing(buildDataSaveAllChangesOnTripPricing(safeGetDocument(input, "dataSaveAllChangesOnTripPricing")));
        builder.setDetails(buildTripDetails(safeGetDocument(input, "details")));
        builder.setDiscountRatio(buildDiscountRatio(safeGetDocument(input, "discount_ratio")));
        builder.setDispatchConfig(buildDispatchConfig(safeGetDocument(input, "dispatch_config")));
        builder.setDriverProfile(buildDriverProfile(safeGetDocument(input, "driver_profile")));
        builder.setDriverShares(buildDriverShares(safeGetDocument(input, "driver_shares")));
        builder.setDriverToRider(buildDriverToRiderProgress(safeGetDocument(input, "driver_to_rider")));
        builder.setFlags(buildFlags(safeGetDocument(input, "flags")));
        builder.setManaged(buildManagedInfo(safeGetDocument(input, "managed")));
        builder.setOriginalUser(buildOriginalUser(safeGetDocument(input, "original_user")));
        builder.setReDispatch(buildReDispatchInfo(safeGetDocument(input, "re_dispatch")));
        builder.setFullActiveTaxesDict(buildFullActiveTaxesDict(safeGetDocument(input, "full_active_taxes_dict")));
        builder.setRiderProfile(buildRiderProfile(safeGetDocument(input, "rider_profile")));
        builder.setRiderReview(buildRiderReview(safeGetDocument(input, "rider_review")));
        builder.setSubTripData(buildSubTripData(safeGetDocument(input, "sub_trip_data")));
        builder.setSurge(buildSurge(safeGetDocument(input, "surge")));
        builder.setSurgePrivate(buildSurgePrivate(safeGetDocument(input, "surgePrivate")));
    //  builder.setTip(buildTip(safeGetDocument(input, "tip")));
        builder.setTripExtension(buildTripExtension(safeGetDocument(input, "tripExtension")));
    }
    
    private static String calculateUpdateDate(Document input) {
        Date updateDateForPartition = null;
        if (input.get("updated_at") instanceof Date) {
            updateDateForPartition = input.getDate("updated_at");
        } else if (input.get("created_at") instanceof Date) {
            updateDateForPartition = input.getDate("created_at");
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return updateDateForPartition != null ? dateFormat.format(updateDateForPartition) : "1970-01-01";
    }
 

    /**
     * Extract simple values from complex documents - NO JSON OUTPUT
     */
  
}