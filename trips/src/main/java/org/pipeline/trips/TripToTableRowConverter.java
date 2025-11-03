package org.pipeline.trips;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

import org.pipeline.trips.avro.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripToTableRowConverter {
  // add logger
  private static final Logger LOG = LoggerFactory.getLogger(TripToTableRowConverter.class);

  public static class ConvertToTableRowFn extends DoFn < Trip, TableRow > {
    private static final long serialVersionUID = 1L;


    private final Counter successCounter = Metrics.counter(ConvertToTableRowFn.class, "success_count");
    private final Counter errorCounter = Metrics.counter(ConvertToTableRowFn.class, "error_count");
    private final Counter nullCounter = Metrics.counter(ConvertToTableRowFn.class, "null_trip_count");

    @ProcessElement
    public void processElement(@Element Trip trip, OutputReceiver < TableRow > out) {
      String docId = "unknown"; // default for logging

      try {
        if (trip == null) {
          nullCounter.inc();
          LOG.warn("Received null Trip object, skipping.");
          return;
        }

        if (trip.getMongoId() != null) {
          docId = trip.getMongoId().toString();
        }

        // convert Trip → TableRow 
        TableRow row = convertTripToTableRow(trip);
        out.output(row);
        successCounter.inc();

      } catch (OutOfMemoryError oom) {
        // handle critical memory failure separately
        errorCounter.inc();
        LOG.error("!!! OUT OF MEMORY ERROR !!! converting Trip to TableRow for ID: {}. Skipping this record.",
          docId, oom);
        // rethrow to ensure Dataflow fails cleanly for OOM
        throw oom;

      } catch (Throwable t) {
        // aatch all other unexpected errors to avoid pipeline crash
        errorCounter.inc();
        LOG.error("CRITICAL ERROR converting Trip to TableRow for ID: {} - Error: {}",
          docId, t.getMessage(), t);
        // continue pipeline safely
      }
    }
  }
  public static TableRow convertTripToTableRow(Trip trip) {
	  TableRow row = new TableRow();

    // --- Top Level Simple Fields ---
    row.set("_id", trip.getMongoId() != null ? trip.getMongoId().toString() : null);
    // row.set("schema_version", trip.getSchemaVersion());
    row.set("accepted_at", trip.getAcceptedAt());
    row.set("arrived_at", trip.getArrivedAt());
    row.set("assigned_at", trip.getAssignedAt());
    row.set("base_cost", trip.getBaseCost());
    row.set("booked_for", trip.getBookedFor());
    row.set("business", trip.getBusiness() != null ? trip.getBusiness().toString() : null);
    row.set("campaign_code", trip.getCampaignCode() != null ? trip.getCampaignCode().toString() : null);
    row.set("cancel_date_time", trip.getCancelDateTime());
    row.set("carpooling", trip.getCarpooling() != null ? trip.getCarpooling().toString() : null);
    row.set("client", trip.getClient() != null ? trip.getClient().toString() : null);
    row.set("company", trip.getCompany() != null ? trip.getCompany().toString() : null);
    row.set("country", trip.getCountry() != null ? trip.getCountry().toString() : null);
    row.set("coupon", trip.getCoupon() != null ? trip.getCoupon().toString() : null);
    // row.set("coupon_id", trip.getCouponId() != null ? trip.getCouponId().toString() : null);
    // row.set("coupon_line_id", trip.getCouponLineId() != null ? trip.getCouponLineId().toString() : null);
    row.set("created_at", trip.getCreatedAt());
    row.set("created_from_dash", trip.getCreatedFromDash());
    row.set("currency", trip.getCurrency() != null ? trip.getCurrency().toString() : null);
    row.set("deflation_factor_code", trip.getDeflationFactorCode() != null ? trip.getDeflationFactorCode().toString() : null);
    row.set("directions_service", trip.getDirectionsService() != null ? trip.getDirectionsService().toString() : null);
    row.set("discount", trip.getDiscount());
    row.set("discounted_cost", trip.getDiscountedCost());
    row.set("dispatch_counter", trip.getDispatchCounter());
    row.set("dispatch_timeout", trip.getDispatchTimeout());
    row.set("dispatched_at", trip.getDispatchedAt());
    row.set("driver", trip.getDriver() != null ? trip.getDriver().toString() : null);
    row.set("estimated_arrived_at", trip.getEstimatedArrivedAt());
    row.set("estimated_cost", trip.getEstimatedCost());
    row.set("estimated_cost_with_boost", trip.getEstimatedCostWithBoost());
    row.set("estimated_distance", trip.getEstimatedDistance());
    row.set("estimated_eta", trip.getEstimatedEta());
    row.set("final_destination_arrival_notification_sent", trip.getFinalDestinationArrivalNotificationSent());
    row.set("finished_at", trip.getFinishedAt());
    row.set("finished_by", trip.getFinishedBy() != null ? trip.getFinishedBy().toString() : null);
    row.set("fraud_status", trip.getFraudStatus() != null ? trip.getFraudStatus().toString() : null);
    row.set("hidden_surcharge_rate", trip.getHiddenSurchargeRate());
    row.set("inflation_factor_code", trip.getInflationFactorCode() != null ? trip.getInflationFactorCode().toString() : null);
    row.set("is_b2b_reward_trip", trip.getIsB2BRewardTrip());
    row.set("is_back_to_back", trip.getIsBackToBack());
    row.set("is_delivery", trip.getIsDelivery());
    row.set("is_pending3ds_authentication", trip.getIsPending3DSAuthentication());
    row.set("is_reward", trip.getIsReward());
    row.set("is_used_coupon", trip.getIsUsedCoupon());
    row.set("is_booked", trip.getIsBooked());
    row.set("is_near_destination", trip.getIsNearDestination());
    row.set("is_near_pickup", trip.getIsNearPickup());
    row.set("manual_status_override", trip.getManualStatusOverride());
    row.set("note_internal", trip.getNoteInternal() != null ? trip.getNoteInternal().toString() : null);
    row.set("note_rider_internal", trip.getNoteRiderInternal() != null ? trip.getNoteRiderInternal().toString() : null);
    row.set("note_rider_to_driver", trip.getNoteRiderToDriver() != null ? trip.getNoteRiderToDriver().toString() : null);
    row.set("original_cost", trip.getOriginalCost());
    row.set("original_cost_with_boost", trip.getOriginalCostWithBoost());
    row.set("original_estimated_cost", trip.getOriginalEstimatedCost());
    row.set("payment_variant", trip.getPaymentVariant() != null ? trip.getPaymentVariant().toString() : null);
    row.set("place_count", trip.getPlaceCount());
    row.set("planned_for", trip.getPlannedFor());
    row.set("polyline", trip.getPolyline() != null ? trip.getPolyline().toString() : null);
    row.set("postpaid_cost", trip.getPostpaidCost());
    row.set("prepaid_cost", trip.getPrepaidCost());
    row.set("radar_enabled", trip.getRadarEnabled());
    row.set("radius", trip.getRadius());
    row.set("recov", trip.getRecov() != null ? trip.getRecov().toString() : null);
    row.set("recov_ack", trip.getRecovAck());
    row.set("redispatched_from", trip.getRedispatchedFrom() != null ? trip.getRedispatchedFrom().toString() : null);
    row.set("reduction_factor_code", trip.getReductionFactorCode() != null ? trip.getReductionFactorCode().toString() : null);
    row.set("related_to", trip.getRelatedTo() != null ? trip.getRelatedTo().toString() : null);
    row.set("requested_4_b2b_guest", trip.getRequested4B2bGuest() != null ? trip.getRequested4B2bGuest().toString() : null);
    row.set("requested_4_b2b_user", trip.getRequested4B2bUser() != null ? trip.getRequested4B2bUser().toString() : null);
    row.set("requested_4_b2c_guest", trip.getRequested4B2cGuest());
    row.set("requested_at", trip.getRequestedAt());
    row.set("rider", trip.getRider() != null ? trip.getRider().toString() : null);
    row.set("rider_pay", trip.getRiderPay());
    row.set("rider_redispatched", trip.getRiderRedispatched());
    row.set("service", trip.getService() != null ? trip.getService().toString() : null);
    row.set("service_config", trip.getServiceConfig() != null ? trip.getServiceConfig().toString() : null);
    row.set("service_family", trip.getServiceFamily() != null ? trip.getServiceFamily().toString() : null);
    row.set("servicetype", trip.getServicetype() != null ? trip.getServicetype().toString() : null);
    row.set("show_price4_b2b_user", trip.getShowPrice4B2bUser());
    row.set("started_at", trip.getStartedAt());
    row.set("status", trip.getStatus() != null ? trip.getStatus().toString() : null);
    row.set("subscriber", trip.getSubscriber());
    row.set("toll_cost", trip.getTollCost());
    row.set("trip_uid", trip.getTripUid() != null ? trip.getTripUid().toString() : null);
    row.set("updated_at", trip.getUpdatedAt());
    row.set("update_date", trip.getUpdateDate() != null ? trip.getUpdateDate().toString() : null);

    // --- Simple Arrays ---
    List<String> assignedDrivers = null;
    if (trip.getAssignedDrivers() != null) {
        assignedDrivers = trip.getAssignedDrivers().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toList());
    }
    row.set("assigned_drivers", assignedDrivers);

    List<String> candidatesCanceled = null;
    if (trip.getCandidatesCanceled() != null) {
        candidatesCanceled = trip.getCandidatesCanceled().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toList());
    }
    row.set("candidates_canceled", candidatesCanceled);

    List<String> driverDevicesTokens = null;
    if (trip.getDriverDevicesTokens() != null) {
        driverDevicesTokens = trip.getDriverDevicesTokens().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toList());
    }
    row.set("driver_devices_tokens", driverDevicesTokens);

    List<String> multistopsDeflationFactorCode = null;
    if (trip.getMultistopsDeflationFactorCode() != null) {
        multistopsDeflationFactorCode = trip.getMultistopsDeflationFactorCode().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toList());
    }
    row.set("multistops_deflation_factor_code", multistopsDeflationFactorCode);

    List<String> multistopsInflationFactorCode = null;
    if (trip.getMultistopsInflationFactorCode() != null) {
        multistopsInflationFactorCode = trip.getMultistopsInflationFactorCode().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toList());
    }
    row.set("multistops_inflation_factor_code", multistopsInflationFactorCode);

    List<String> recentAssignedDrivers = null;
    if (trip.getRecentAssignedDrivers() != null) {
        recentAssignedDrivers = trip.getRecentAssignedDrivers().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toList());
    }
    row.set("recent_assigned_drivers", recentAssignedDrivers);

    List<String> riderDevicesTokens = null;
    if (trip.getRiderDevicesTokens() != null) {
        riderDevicesTokens = trip.getRiderDevicesTokens().stream()
            .map(CharSequence::toString)
            .collect(Collectors.toList());
    }
    row.set("rider_devices_tokens", riderDevicesTokens);

    // Tags: Convert List<List<CharSequence>> to List<List<String>>
    if (trip.getTags() != null) {
        // Use flatMap to flatten the list of lists into a single list
        List<String> tagsConverted = trip.getTags().stream()
            .flatMap(innerList -> innerList.stream())
            .map(CharSequence::toString)
            .collect(Collectors.toList());
        row.set("tags", tagsConverted);
    }


    // --- Nested Arrays ---
    row.set("adjusted_to", convertAdjustedToList(trip.getAdjustedTo()));
    row.set("applied_taxes", convertAppliedTaxesList(trip.getAppliedTaxes()));
    row.set("applied_taxes_driver", convertAppliedTaxesList(trip.getAppliedTaxesDriver()));
    row.set("applied_taxes_rider", convertAppliedTaxesList(trip.getAppliedTaxesRider()));
    row.set("applied_taxes_rider_visible", convertAppliedTaxesList(trip.getAppliedTaxesRiderVisible()));
    row.set("applied_taxes_rider_visible_invoices", convertAppliedTaxesList(trip.getAppliedTaxesRiderVisibleInvoices()));
    row.set("candidates", convertCandidatesList(trip.getCandidates()));
    row.set("cost_breakdown_sub_trip", convertCostBreakdownSubTripList(trip.getCostBreakdownSubTrip()));
    row.set("driver_review_counts", convertReviewCountsList(trip.getDriverReviewCounts()));
    row.set("status_history", convertStatusHistoryList(trip.getStatusHistory()));
    row.set("stops_points_details", convertStopPointDetailsList(trip.getStopsPointsDetails()));

    // --- Nested Objects ---
    row.set("currency_label", convertLabel(trip.getCurrencyLabel()));
    row.set("pickup", convertLocation(trip.getPickup()));
    row.set("destination", convertLocation(trip.getDestination()));
    row.set("pickup_details", convertLocationDetails(trip.getPickupDetails()));
    row.set("destination_details", convertLocationDetails(trip.getDestinationDetails()));
    row.set("driver_cancel_reason", convertCancelReason(trip.getDriverCancelReason()));
    row.set("rider_cancel_reason", convertCancelReason(trip.getRiderCancelReason()));
    row.set("driver_device", convertDeviceProfile(trip.getDriverDevice()));
    row.set("rider_device", convertDeviceProfile(trip.getRiderDevice()));
    row.set("driver_review", convertReview(trip.getDriverReview()));
    row.set("epay", convertEpay(trip.getEpay()));

    // --- Complex Structures ---
    row.set("b2b_trip_request_approval", convertB2BTripRequestApproval(trip.getB2bTripRequestApproval()));
    row.set("booking_configs", convertBookingConfigs(trip.getBookingConfigs()));
    row.set("boost", convertBoost(trip.getBoost()));
    row.set("challenge", convertChallenge(trip.getChallenge()));
    row.set("cost_breakdown", convertCostBreakdown(trip.getCostBreakdown()));
    row.set("data_save_all_changes_on_trip_pricing", convertDataSaveAllChangesOnTripPricing(trip.getDataSaveAllChangesOnTripPricing()));
    row.set("details", convertTripDetails(trip.getDetails()));
    row.set("discount_ratio", convertDiscountRatio(trip.getDiscountRatio()));
    row.set("dispatch_config", convertDispatchConfig(trip.getDispatchConfig()));
    row.set("driver_profile", convertDriverProfile(trip.getDriverProfile()));
    row.set("driver_shares", convertDriverShares(trip.getDriverShares()));
    row.set("driver_to_rider", convertDriverToRiderProgress(trip.getDriverToRider()));
    row.set("flags", convertFlags(trip.getFlags()));
    row.set("managed", convertManagedInfo(trip.getManaged()));
    row.set("original_user", convertOriginalUser(trip.getOriginalUser()));
    row.set("re_dispatch", convertReDispatchInfo(trip.getReDispatch()));
    row.set("full_active_taxes_dict", convertFullActiveTaxesDict(trip.getFullActiveTaxesDict()));
    row.set("rider_profile", convertRiderProfile(trip.getRiderProfile()));
    row.set("rider_review", convertRiderReview(trip.getRiderReview()));
    row.set("sub_trip_data", convertSubTripData(trip.getSubTripData()));
    row.set("surge", convertSurge(trip.getSurge()));
    row.set("surge_private", convertSurgePrivate(trip.getSurgePrivate()));
    //  row.set("tip", convertTip(trip.getTip()));
    row.set("trip_extension", convertTripExtension(trip.getTripExtension()));

    return row;
  }

  // --- Conversion methods for nested objects ---

  private static TableRow convertLabel(Label label) {
    if (label == null) return null;
    TableRow row = new TableRow();
    row.set("ar", label.getAr() != null ? label.getAr().toString() : null);
    row.set("en", label.getEn() != null ? label.getEn().toString() : null);
    row.set("fr", label.getFr() != null ? label.getFr().toString() : null);
    return row;
  }

  private static TableRow convertLocation(Location location) {
    if (location == null) return null;
    TableRow row = new TableRow();
    row.set("type", location.getType() != null ? location.getType().toString() : null);
    row.set("coordinates", location.getCoordinates());
    row.set("formatted_address", location.getFormattedAddress() != null ? location.getFormattedAddress().toString() : null);
    return row;
  }

  private static TableRow convertLocationDetails(LocationDetails locationDetails) {
    if (locationDetails == null) return null;
    TableRow row = new TableRow();
    row.set("geohashs_h3", convertGeoHashH3(locationDetails.getGeohashsH3()));
    row.set("lat", locationDetails.getLat() != null ? locationDetails.getLat().toString() : null);
    row.set("lng", locationDetails.getLng() != null ? locationDetails.getLng().toString() : null);
    row.set("lvl0_id", locationDetails.getLvl0Id() != null ? locationDetails.getLvl0Id().toString() : null);
    row.set("lvl0_label", locationDetails.getLvl0Label() != null ? locationDetails.getLvl0Label().toString() : null);
    row.set("lvl1_id", locationDetails.getLvl1Id() != null ? locationDetails.getLvl1Id().toString() : null);
    row.set("lvl1_label", locationDetails.getLvl1Label() != null ? locationDetails.getLvl1Label().toString() : null);
    row.set("lvl2_id", locationDetails.getLvl2Id() != null ? locationDetails.getLvl2Id().toString() : null);
    row.set("lvl2_label", locationDetails.getLvl2Label() != null ? locationDetails.getLvl2Label().toString() : null);
    row.set("lvl3_id", locationDetails.getLvl3Id() != null ? locationDetails.getLvl3Id().toString() : null);
    row.set("lvl3_label", locationDetails.getLvl3Label() != null ? locationDetails.getLvl3Label().toString() : null);
    row.set("map_address", locationDetails.getMapAddress() != null ? locationDetails.getMapAddress().toString() : null);
    row.set("map_path", locationDetails.getMapPath() != null ? locationDetails.getMapPath().toString() : null);
    row.set("zone_code", locationDetails.getZoneCode() != null ? locationDetails.getZoneCode().toString() : null);
    return row;
  }

  private static TableRow convertGeoHashH3(GeoHashH3 geoHashH3) {
    if (geoHashH3 == null) return null;
    TableRow row = new TableRow();
    row.set("level4", geoHashH3.getLevel4() != null ? geoHashH3.getLevel4().toString() : null);
    row.set("level5", geoHashH3.getLevel5() != null ? geoHashH3.getLevel5().toString() : null);
    row.set("level6", geoHashH3.getLevel6() != null ? geoHashH3.getLevel6().toString() : null);
    row.set("level7", geoHashH3.getLevel7() != null ? geoHashH3.getLevel7().toString() : null);
    row.set("level8", geoHashH3.getLevel8() != null ? geoHashH3.getLevel8().toString() : null);
    row.set("level9", geoHashH3.getLevel9() != null ? geoHashH3.getLevel9().toString() : null);
    return row;
  }

  private static TableRow convertCancelReason(CancelReason cancelReason) {
    if (cancelReason == null) return null;
    TableRow row = new TableRow();
    row.set("comment", cancelReason.getComment() != null ? cancelReason.getComment().toString() : null);
    row.set("reason", cancelReason.getReason() != null ? cancelReason.getReason().toString() : null);
    //  row.set("reason_id", cancelReason.getReasonId() != null ? cancelReason.getReasonId().toString() : null);_
    return row;
  }

  private static TableRow convertDeviceProfile(DeviceProfile deviceProfile) {
    if (deviceProfile == null) return null;
    TableRow row = new TableRow();
    row.set("active", deviceProfile.getActive());
    row.set("app_lang", deviceProfile.getAppLang() != null ? deviceProfile.getAppLang().toString() : null);
    row.set("app_version", deviceProfile.getAppVersion() != null ? deviceProfile.getAppVersion().toString() : null);
    row.set("brand", deviceProfile.getBrand() != null ? deviceProfile.getBrand().toString() : null);
    row.set("created_at", deviceProfile.getCreatedAt());
    row.set("device_id", deviceProfile.getDeviceId() != null ? deviceProfile.getDeviceId().toString() : null);
    row.set("device_lang", deviceProfile.getDeviceLang() != null ? deviceProfile.getDeviceLang().toString() : null);
    row.set("fcm_token", deviceProfile.getFcmToken() != null ? deviceProfile.getFcmToken().toString() : null);
    row.set("last_used", deviceProfile.getLastUsed());
    row.set("location", convertDeviceLocation(deviceProfile.getLocation()));
    row.set("model", deviceProfile.getModel() != null ? deviceProfile.getModel().toString() : null);
    row.set("os_version", deviceProfile.getOsVersion() != null ? deviceProfile.getOsVersion().toString() : null);
    row.set("platform", deviceProfile.getPlatform() != null ? deviceProfile.getPlatform().toString() : null);
    row.set("retired_at", deviceProfile.getRetiredAt() != null ? deviceProfile.getRetiredAt().toString() : null);
    row.set("ya_device_id", deviceProfile.getYaDeviceId() != null ? deviceProfile.getYaDeviceId().toString() : null);
    return row;
  }

  private static TableRow convertDeviceLocation(DeviceLocation deviceLocation) {
    if (deviceLocation == null) return null;
    TableRow row = new TableRow();
    row.set("coordinates", deviceLocation.getCoordinates());
    return row;
  }

  private static TableRow convertReview(Review review) {
    if (review == null) return null;
    TableRow row = new TableRow();
    row.set("comment", review.getComment() != null ? review.getComment().toString() : null);
    row.set("rating", review.getRating());
    return row;
  }

  private static TableRow convertEpay(Epay epay) {
    if (epay == null) return null;
    TableRow row = new TableRow();
    row.set("amount", epay.getAmount());
    row.set("method", epay.getMethod() != null ? epay.getMethod().toString() : null);
    row.set("order_id", epay.getOrderId() != null ? epay.getOrderId().toString() : null);
    row.set("second_payment_method", epay.getSecondPaymentMethod() != null ? epay.getSecondPaymentMethod().toString() : null);
    row.set("status", convertEpayStatus(epay.getStatus()));
    row.set("type", epay.getType() != null ? epay.getType().toString() : null);
    return row;
  }

  private static TableRow convertEpayStatus(EpayStatus epayStatus) {
    if (epayStatus == null) return null;
    TableRow row = new TableRow();
    row.set("code", epayStatus.getCode());
    row.set("created_at", epayStatus.getCreatedAt());
    row.set("current", epayStatus.getCurrent() != null ? epayStatus.getCurrent().toString() : null);
    row.set("note", epayStatus.getNote() != null ? epayStatus.getNote().toString() : null);
    row.set("updated_at", epayStatus.getUpdatedAt());
    return row;
  }

  // --- Conversion methods for arrays ---

  private static List < TableRow > convertAdjustedToList(List < AdjustedTo > adjustedToList) {
    if (adjustedToList == null) return null;
    return adjustedToList.stream()
      .map(adjustedTo -> {
        TableRow row = new TableRow();
        row.set("trip_id", adjustedTo.getTripId() != null ? adjustedTo.getTripId().toString() : null);
        row.set("trip_uid", adjustedTo.getTripUid() != null ? adjustedTo.getTripUid().toString() : null);
        row.set("status", adjustedTo.getStatus() != null ? adjustedTo.getStatus().toString() : null);
        row.set("adjusted_to_id", adjustedTo.getAdjustedToId() != null ? adjustedTo.getAdjustedToId().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static List < TableRow > convertAppliedTaxesList(List < AppliedTax > appliedTaxes) {
    if (appliedTaxes == null) return null;
    return appliedTaxes.stream()
      .map(appliedTax -> {
        TableRow row = new TableRow();
        row.set("type", appliedTax.getType() != null ? appliedTax.getType().toString() : null);
        row.set("label", convertLabel(appliedTax.getLabel()));
        row.set("code", appliedTax.getCode() != null ? appliedTax.getCode().toString() : null);
        row.set("amount", appliedTax.getAmount());
        row.set("reversable_amount", appliedTax.getReversableAmount());
        row.set("on_yassir_benefit", appliedTax.getOnYassirBenefit());
        // row.set("tax_id", appliedTax.getTaxId() != null ? appliedTax.getTaxId().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static List < TableRow > convertCandidatesList(List < Candidate > candidates) {
    if (candidates == null) return null;
    return candidates.stream()
      .map(candidate -> {
        TableRow row = new TableRow();
        row.set("driver", candidate.getDriver() != null ? candidate.getDriver().toString() : null);
        row.set("assigned_at", candidate.getAssignedAt());
        row.set("rejected_at", candidate.getRejectedAt());
        row.set("rejection_type", candidate.getRejectionType() != null ? candidate.getRejectionType().toString() : null);
        //  row.set("candidate_id", candidate.getCandidateId() != null ? candidate.getCandidateId().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static List < TableRow > convertCostBreakdownSubTripList(List < CostBreakdownSubTrip > costBreakdownSubTrips) {
    if (costBreakdownSubTrips == null) return null;
    return costBreakdownSubTrips.stream()
      .map(costBreakdown -> {
        TableRow row = new TableRow();
        row.set("initial_cost", costBreakdown.getInitialCost());
        row.set("original_cost", costBreakdown.getOriginalCost());
        row.set("net_cost_initial", costBreakdown.getNetCostInitial());
        row.set("net_cost_post_discounts", costBreakdown.getNetCostPostDiscounts());
        row.set("total_taxes", costBreakdown.getTotalTaxes());
        row.set("total_taxes_rider", costBreakdown.getTotalTaxesRider());
        row.set("total_taxes_driver", costBreakdown.getTotalTaxesDriver());
        // row.set("cost_breakdown_sub_trip_id", costBreakdown.getCostBreakdownSubTripId() != null ? costBreakdown.getCostBreakdownSubTripId().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static List < TableRow > convertReviewCountsList(List < ReviewCount > reviewCounts) {
    if (reviewCounts == null) return null;
    return reviewCounts.stream()
      .map(reviewCount -> {
        TableRow row = new TableRow();
        row.set("code", reviewCount.getCode() != null ? reviewCount.getCode().toString() : null);
        row.set("comment", reviewCount.getComment() != null ? reviewCount.getComment().toString() : null);
        row.set("count", reviewCount.getCount());
        //  row.set("review_count_id", reviewCount.getReviewCountId() != null ? reviewCount.getReviewCountId().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static List < TableRow > convertStatusHistoryList(List < StatusHistory > statusHistories) {
    if (statusHistories == null) return null;
    return statusHistories.stream()
      .map(statusHistory -> {
        TableRow row = new TableRow();
        //   row.set("status_history_id", statusHistory.getStatusHistoryId() != null ? statusHistory.getStatusHistoryId().toString() : null);
        row.set("driver", statusHistory.getDriver() != null ? statusHistory.getDriver().toString() : null);
        row.set("insert_from", statusHistory.getInsertFrom() != null ? statusHistory.getInsertFrom().toString() : null);
        row.set("location", convertDriverLocation(statusHistory.getLocation()));
        row.set("status", statusHistory.getStatus() != null ? statusHistory.getStatus().toString() : null);
        row.set("time", statusHistory.getTime());
        return row;
      })
      .collect(Collectors.toList());
  }

  private static List <TableRow> convertStopPointDetailsList(List <StopPointDetails> stopPointDetails) {
    if (stopPointDetails == null) return null;
    return stopPointDetails.stream().map(stopPoint -> {
    	TableRow row = new TableRow();
        row.set("lvl0_id", stopPoint.getLvl0Id() != null ? stopPoint.getLvl0Id().toString() : null);
        row.set("lvl1_id", stopPoint.getLvl1Id() != null ? stopPoint.getLvl1Id().toString() : null);
        row.set("lvl2_id", stopPoint.getLvl2Id() != null ? stopPoint.getLvl2Id().toString() : null);
        row.set("lvl3_id", stopPoint.getLvl3Id() != null ? stopPoint.getLvl3Id().toString() : null);
        row.set("lvl0_label", stopPoint.getLvl0Label() != null ? stopPoint.getLvl0Label().toString() : null);
        row.set("lvl1_label", stopPoint.getLvl1Label() != null ? stopPoint.getLvl1Label().toString() : null);
        row.set("lvl2_label", stopPoint.getLvl2Label() != null ? stopPoint.getLvl2Label().toString() : null);
        row.set("lvl3_label", stopPoint.getLvl3Label() != null ? stopPoint.getLvl3Label().toString() : null);
        row.set("map_path", stopPoint.getMapPath() != null ? stopPoint.getMapPath().toString() : null);
        row.set("map_address", stopPoint.getMapAddress() != null ? stopPoint.getMapAddress().toString() : null);
        row.set("lat", stopPoint.getLat() != null ? stopPoint.getLat().toString() : null);
        row.set("lng", stopPoint.getLng() != null ? stopPoint.getLng().toString() : null);
        row.set("zone_code", stopPoint.getZoneCode() != null ? stopPoint.getZoneCode().toString() : null);
        //   row.set("stop_point_details_id", stopPoint.getStopPointDetailsId() != null ? stopPoint.getStopPointDetailsId().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  // --- Conversion methods for complex structures ---

  private static TableRow convertB2BTripRequestApproval(B2BTripRequestApproval approval) {
    if (approval == null) return null;
    TableRow row = new TableRow();
    row.set("approval_needed", approval.getApprovalNeeded());
    return row;
  }

  private static TableRow convertBookingConfigs(BookingConfigs bookingConfigs) {
    if (bookingConfigs == null) return null;
    TableRow row = new TableRow();
    row.set("driver_confirmation_timeout_minutes", bookingConfigs.getDriverConfirmationTimeoutMinutes());
    row.set("ops_manual_dispatch_window_minutes", bookingConfigs.getOpsManualDispatchWindowMinutes());
    row.set("ops_manual_rider_confirmation_window_minutes", bookingConfigs.getOpsManualRiderConfirmationWindowMinutes());
    row.set("post_rider_confirmation_dispatch_minutes", bookingConfigs.getPostRiderConfirmationDispatchMinutes());
    row.set("pre_booking_driver_confirmation_minutes", bookingConfigs.getPreBookingDriverConfirmationMinutes());
    row.set("pre_booking_rider_confirmation_minutes", bookingConfigs.getPreBookingRiderConfirmationMinutes());
    row.set("rider_confirmation_timeout_minutes", bookingConfigs.getRiderConfirmationTimeoutMinutes());
    return row;
  }

  private static TableRow convertBoost(Boost boost) {
    if (boost == null) return null;
    TableRow row = new TableRow();
    row.set("value", boost.getValue());
    row.set("history", boost.getHistory());
    return row;
  }

  private static TableRow convertChallenge(Challenge challenge) {
    if (challenge == null) return null;
    TableRow row = new TableRow();
    row.set("id", challenge.getId() != null ? challenge.getId().toString() : null);
    row.set("uid", challenge.getUid() != null ? challenge.getUid().toString() : null);
    return row;
  }

  private static TableRow convertCostBreakdown(CostBreakdown costBreakdown) {
    if (costBreakdown == null) return null;
    TableRow row = new TableRow();
    row.set("initial_cost", costBreakdown.getInitialCost());
    row.set("original_cost", costBreakdown.getOriginalCost());
    row.set("net_cost_initial", costBreakdown.getNetCostInitial());
    row.set("net_cost_post_discounts", costBreakdown.getNetCostPostDiscounts());
    row.set("total_taxes", costBreakdown.getTotalTaxes());
    row.set("total_taxes_rider", costBreakdown.getTotalTaxesRider());
    row.set("total_taxes_driver", costBreakdown.getTotalTaxesDriver());
    row.set("yassir_share", costBreakdown.getYassirShare());
    row.set("b2b_commission_rate", costBreakdown.getB2bCommissionRate());
    row.set("b2b_commission_amount", costBreakdown.getB2bCommissionAmount());
    return row;
  }

  private static TableRow convertDataSaveAllChangesOnTripPricing(DataSaveAllChangesOnTripPricing dataSave) {
    if (dataSave == null) return null;
    TableRow row = new TableRow();
    row.set("coupon", convertCouponDetails(dataSave.getCoupon()));
    row.set("coupon_line_obj", convertCouponLineObj(dataSave.getCouponLineObj()));
    row.set("discount_values", convertDiscountValues(dataSave.getDiscountValues()));
    row.set("inflation_values", convertInflationValues(dataSave.getInflationValues()));
    row.set("pricing_line", convertPricingLine(dataSave.getPricingLine()));
    row.set("pricing_lines", convertPricingLinesList(dataSave.getPricingLines()));
    return row;
  }

  private static TableRow convertCouponDetails(CouponDetails coupon) {
    if (coupon == null) return null;
    TableRow row = new TableRow();
    row.set("code", coupon.getCode() != null ? coupon.getCode().toString() : null);
    row.set("is_used", coupon.getIsUsed());
    row.set("is_valid", coupon.getIsValid());
    // row.set("v", coupon.getV());
    return row;
  }

  private static TableRow convertCouponLineObj(CouponLineObj couponLineObj) {
    if (couponLineObj == null) return null;
    TableRow row = new TableRow();
    row.set("discount_ratio", couponLineObj.getDiscountRatio());
    row.set("discount_flat", couponLineObj.getDiscountFlat());
    row.set("max_amount", couponLineObj.getMaxAmount());
    //  row.set("couponline_id", couponLineObj.getCouponlineID() != null ? couponLineObj.getCouponlineID().toString() : null);
    //  row.set("coupon_name", couponLineObj.getCouponName() != null ? couponLineObj.getCouponName().toString() : null);
    row.set("service", couponLineObj.getService() != null ? couponLineObj.getService().toString() : null);
    row.set("max_price", couponLineObj.getMaxPrice());
    row.set("min_price", couponLineObj.getMinPrice());
    //  row.set("coupon_id", couponLineObj.getCouponId() != null ? couponLineObj.getCouponId().toString() : null);
    return row;
  }

  private static TableRow convertDiscountValues(DiscountValues discountValues) {
    if (discountValues == null) return null;
    TableRow row = new TableRow();
    row.set("auto_discount_ratio", discountValues.getAutoDiscountRatio());
    row.set("code_name", discountValues.getCodeName() != null ? discountValues.getCodeName().toString() : null);
    row.set("discount_ratio", discountValues.getDiscountRatio());
    row.set("reduction_discounted_amount", discountValues.getReductionDiscountedAmount());
    return row;
  }

  private static TableRow convertInflationValues(InflationValues inflationValues) {
    if (inflationValues == null) return null;
    TableRow row = new TableRow();
    //  row.set("inflation_values_id", inflationValues.getInflationValuesId() != null ? inflationValues.getInflationValuesId().toString() : null);
    row.set("code_name", inflationValues.getCodeName() != null ? inflationValues.getCodeName().toString() : null);
    row.set("inflation_max_amount", inflationValues.getInflationMaxAmount());
    row.set("inflation_min_amount", inflationValues.getInflationMinAmount());
    row.set("inflation_ratio", inflationValues.getInflationRatio());
    return row;
  }

  private static TableRow convertPricingLine(PricingLine pricingLine) {
    if (pricingLine == null) return null;
    TableRow row = new TableRow();
    row.set("destination", pricingLine.getDestination() != null ? pricingLine.getDestination().toString() : null);
    row.set("destination_zone", pricingLine.getDestinationZone() != null ? pricingLine.getDestinationZone().toString() : null);
    row.set("pickup", pricingLine.getPickup() != null ? pricingLine.getPickup().toString() : null);
    row.set("pickup_zone", pricingLine.getPickupZone() != null ? pricingLine.getPickupZone().toString() : null);
    row.set("service", pricingLine.getService() != null ? pricingLine.getService().toString() : null);
    row.set("st", pricingLine.getSt() != null ? pricingLine.getSt().toString() : null);
    row.set("tarifs", convertTarifs(pricingLine.getTarifs()));
    row.set("variant", pricingLine.getVariant() != null ? pricingLine.getVariant().toString() : null);
    return row;
  }

  private static TableRow convertTarifs(Tarifs tarifs) {
    if (tarifs == null) return null;
    TableRow row = new TableRow();
    row.set("min_price", tarifs.getMinPrice());
    row.set("base_price", tarifs.getBasePrice());
    row.set("minute_price", tarifs.getMinutePrice());
    row.set("km_price", tarifs.getKmPrice());
    return row;
  }

  private static List < TableRow > convertPricingLinesList(List < PricingLineSimple > pricingLines) {
    if (pricingLines == null) return null;
    return pricingLines.stream()
      .map(pricingLine -> {
        TableRow row = new TableRow();
        row.set("pickup", pricingLine.getPickup() != null ? pricingLine.getPickup().toString() : null);
        row.set("destination", pricingLine.getDestination() != null ? pricingLine.getDestination().toString() : null);
        row.set("tarifs", convertTarifsSimple(pricingLine.getTarifs()));
        row.set("variant", pricingLine.getVariant() != null ? pricingLine.getVariant().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static TableRow convertTarifsSimple(TarifsSimple tarifs) {
    if (tarifs == null) return null;
    TableRow row = new TableRow();
    row.set("min_price", tarifs.getMinPrice());
    row.set("base_price", tarifs.getBasePrice());
    row.set("minute_price", tarifs.getMinutePrice());
    row.set("km_price", tarifs.getKmPrice());
    return row;
  }

  private static TableRow convertTripDetails(TripDetails tripDetails) {
    if (tripDetails == null) return null;
    TableRow row = new TableRow();
    row.set("service", convertService(tripDetails.getService()));
    row.set("servicetype", convertServiceType(tripDetails.getServicetype()));
    return row;
  }

  private static TableRow convertService(Service service) {
    if (service == null) return null;
    TableRow row = new TableRow();
    row.set("id", service.getId() != null ? service.getId().toString() : null);
    row.set("label", convertLabel(service.getLabel()));
    return row;
  }

  private static TableRow convertServiceType(ServiceType serviceType) {
    if (serviceType == null) return null;
    TableRow row = new TableRow();
    row.set("id", serviceType.getId() != null ? serviceType.getId().toString() : null);
    row.set("label", convertLabel(serviceType.getLabel()));
    return row;
  }

  private static TableRow convertDiscountRatio(DiscountRatio discountRatio) {
    if (discountRatio == null) return null;
    TableRow row = new TableRow();
    row.set("percent", discountRatio.getPercent());
    row.set("amount", discountRatio.getAmount());
    return row;
  }

  private static TableRow convertDispatchConfig(DispatchConfig dispatchConfig) {
    if (dispatchConfig == null) return null;
    TableRow row = new TableRow();
    row.set("max_candidates", dispatchConfig.getMaxCandidates());
    row.set("max_requests", dispatchConfig.getMaxRequests());
    row.set("drivers_batch", dispatchConfig.getDriversBatch());
    return row;
  }

  private static TableRow convertDriverProfile(DriverProfile driverProfile) {
    if (driverProfile == null) return null;
    TableRow row = new TableRow();
    row.set("app_platform", driverProfile.getAppPlatform() != null ? driverProfile.getAppPlatform().toString() : null);
    row.set("app_version", driverProfile.getAppVersion() != null ? driverProfile.getAppVersion().toString() : null);
    row.set("car", convertCar(driverProfile.getCar()));
    row.set("company", driverProfile.getCompany() != null ? driverProfile.getCompany().toString() : null);
    row.set("country", driverProfile.getCountry() != null ? driverProfile.getCountry().toString() : null);
    row.set("display_name", driverProfile.getDisplayName() != null ? driverProfile.getDisplayName().toString() : null);
    row.set("driver", driverProfile.getDriver() != null ? driverProfile.getDriver().toString() : null);
    row.set("driver_uid", driverProfile.getDriverUid() != null ? driverProfile.getDriverUid().toString() : null);
    row.set("full_name", driverProfile.getFullName() != null ? driverProfile.getFullName().toString() : null);
    row.set("gender", driverProfile.getGender() != null ? driverProfile.getGender().toString() : null);
    row.set("is_verified", driverProfile.getIsVerified());
    row.set("location", convertDriverLocation(driverProfile.getLocation()));
    row.set("phone", driverProfile.getPhone() != null ? driverProfile.getPhone().toString() : null);
    row.set("picture", driverProfile.getPicture() != null ? driverProfile.getPicture().toString() : null);
    row.set("rating", driverProfile.getRating());
    row.set("status", driverProfile.getStatus() != null ? driverProfile.getStatus().toString() : null);
    row.set("wilaya", driverProfile.getWilaya());
    return row;
  }

  private static TableRow convertDriverLocation(DriverLocation driverLocation) {
    if (driverLocation == null) return null;
    TableRow row = new TableRow();
    row.set("lat", driverLocation.getLat());
    row.set("lng", driverLocation.getLng());
    return row;
  }

  private static TableRow convertCar(Car car) {
    if (car == null) return null;
    TableRow row = new TableRow();
    row.set("model", car.getModel() != null ? car.getModel().toString() : null);
    row.set("marque", car.getMarque() != null ? car.getMarque().toString() : null);
    row.set("immat", car.getImmat() != null ? car.getImmat().toString() : null);
    row.set("color", car.getColor() != null ? car.getColor().toString() : null);
    row.set("energie", car.getEnergie() != null ? car.getEnergie().toString() : null);
    row.set("year", car.getYear());
    return row;
  }

  private static TableRow convertDriverShares(DriverShares driverShares) {
    if (driverShares == null) return null;
    TableRow row = new TableRow();
    row.set("balance", driverShares.getBalance());
    row.set("benefit", driverShares.getBenefit());
    row.set("benefit_net", driverShares.getBenefitNet());
    row.set("driver2yassir", driverShares.getDriver2yassir());
    row.set("driver_benefit_tax", driverShares.getDriverBenefitTax());
    row.set("yassir2driver", driverShares.getYassir2driver());
    row.set("driver_taxes_list_visible", convertDriverTaxesList(driverShares.getDriverTaxesListVisible()));
    row.set("yassir_commission", convertYassirCommission(driverShares.getYassirCommission()));
    return row;
  }

  private static List < TableRow > convertDriverTaxesList(List < DriverTax > driverTaxes) {
    if (driverTaxes == null) return null;
    return driverTaxes.stream()
      .map(driverTax -> {
        TableRow row = new TableRow();
        row.set("amount", driverTax.getAmount());
        row.set("label", convertLabel(driverTax.getLabel()));
        row.set("code", driverTax.getCode() != null ? driverTax.getCode().toString() : null);
        //   row.set("driver_tax_id", driverTax.getDriverTaxId() != null ? driverTax.getDriverTaxId().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static TableRow convertYassirCommission(YassirCommission yassirCommission) {
    if (yassirCommission == null) return null;
    TableRow row = new TableRow();
    row.set("is_reduced", yassirCommission.getIsReduced());
    row.set("percent", yassirCommission.getPercent());
    row.set("history", convertCommissionHistoryList(yassirCommission.getHistory()));
    return row;
  }

  private static List < TableRow > convertCommissionHistoryList(List < CommissionHistory > commissionHistory) {
    if (commissionHistory == null) return null;
    return commissionHistory.stream()
      .map(history -> {
        TableRow row = new TableRow();
        row.set("new_percent_value", history.getNewPercentValue());
        row.set("old_percent_value", history.getOldPercentValue());
        //   row.set("reduction_id", history.getReductionId() != null ? history.getReductionId().toString() : null);
        row.set("reduction_name", history.getReductionName() != null ? history.getReductionName().toString() : null);
        row.set("type", history.getType() != null ? history.getType().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static TableRow convertDriverToRiderProgress(DriverToRiderProgress driverToRider) {
    if (driverToRider == null) return null;
    TableRow row = new TableRow();
    row.set("driver_progress", convertProgressDetails(driverToRider.getDriverProgress()));
    row.set("estimated_arrival_time", driverToRider.getEstimatedArrivalTime());
    row.set("estimated_distance", driverToRider.getEstimatedDistance());
    row.set("estimated_time", driverToRider.getEstimatedTime());
    row.set("polyline", driverToRider.getPolyline() != null ? driverToRider.getPolyline().toString() : null);
    return row;
  }

  private static TableRow convertProgressDetails(ProgressDetails progress) {
    if (progress == null) return null;
    TableRow row = new TableRow();
    row.set("accuracy", progress.getAccuracy());
    row.set("bearing", progress.getBearing());
    row.set("direction_updated_at", progress.getDirectionUpdatedAt());
    row.set("estimated_arrival_time", progress.getEstimatedArrivalTime());
    row.set("estimated_distance", progress.getEstimatedDistance());
    row.set("estimated_time", progress.getEstimatedTime());
    row.set("polyline", progress.getPolyline() != null ? progress.getPolyline().toString() : null);
    row.set("speed", progress.getSpeed());
    return row;
  }

  private static TableRow convertFlags(Flags flags) {
    if (flags == null) return null;
    TableRow row = new TableRow();
    row.set("should_reduce_balance", flags.getShouldReduceBalance());
    return row;
  }

  private static TableRow convertManagedInfo(ManagedInfo managed) {
    if (managed == null) return null;
    TableRow row = new TableRow();
    row.set("by", managed.getBy() != null ? managed.getBy().toString() : null);
    row.set("at", managed.getAt() != null ? managed.getAt().toString() : null);
    return row;
  }

  private static TableRow convertOriginalUser(OriginalUser originalUser) {
    if (originalUser == null) return null;
    TableRow row = new TableRow();
    row.set("full_name", originalUser.getFullName() != null ? originalUser.getFullName().toString() : null);
    row.set("last_name", originalUser.getLastName() != null ? originalUser.getLastName().toString() : null);
    row.set("first_name", originalUser.getFirstName() != null ? originalUser.getFirstName().toString() : null);
    row.set("phone", originalUser.getPhone() != null ? originalUser.getPhone().toString() : null);
    return row;
  }

  private static TableRow convertReDispatchInfo(ReDispatchInfo reDispatch) {
    if (reDispatch == null) return null;
    TableRow row = new TableRow();
    row.set("initial", reDispatch.getInitial() != null ? reDispatch.getInitial().toString() : null);
    row.set("previous", reDispatch.getPrevious() != null ? reDispatch.getPrevious().toString() : null);
    row.set("retries", reDispatch.getRetries());
    return row;
  }

  private static List<TableRow> convertFullActiveTaxesDict(Map<CharSequence, List<Tax>> taxesDict) {
	    if (taxesDict == null) return null;

	    // Convert the Map<String, List<Tax>> into a List<TableRow>
	    // This will create the [{"key": "...", "value": [...]}] structure
	    return taxesDict.entrySet().stream()
	        .map(entry -> {
	            TableRow kvPairRow = new TableRow();
	            kvPairRow.set("key", entry.getKey().toString()); // Set the "key" field
	            kvPairRow.set("value", convertTaxList(entry.getValue())); // Set the "value" field
	            return kvPairRow;
	        })
	        .collect(Collectors.toList());
	}


  private static List<TableRow> convertTaxList(List<Tax> taxes) {
	    if (taxes == null) return null;

	    return taxes.stream()
	        .map(tax -> {
	            TableRow row = new TableRow();
	            // row.set("tax_id", tax.getTaxId() != null ? tax.getTaxId().toString() : null);
	            row.set("code", tax.getCode() != null ? tax.getCode().toString() : null);
	            row.set("country", tax.getCountry() != null ? tax.getCountry().toString() : null);
	            row.set("label", convertLabel(tax.getLabel()));
	            row.set("level_path", tax.getLevelPath() != null ? tax.getLevelPath().toString() : null);
	            row.set("max_tax_value", tax.getMaxTaxValue());
	            row.set("max_value", tax.getMaxValue());
	            row.set("min_tax_value", tax.getMinTaxValue());
	            row.set("min_value", tax.getMinValue());
	            row.set("order", tax.getOrder());
	            row.set("percent", tax.getPercent());
	            row.set("reverse", tax.getReverse());
	            row.set("service", tax.getService() != null ? tax.getService().toString() : null);
	            row.set("servicetype", tax.getServicetype() != null ? tax.getServicetype().toString() : null);
	            row.set("st", tax.getSt() != null ? tax.getSt().toString() : null);
	            row.set("type", tax.getType() != null ? tax.getType().toString() : null);
	            row.set("value", tax.getValue() != null ? tax.getValue().toString() : null);
	            row.set("value_type", tax.getValueType() != null ? tax.getValueType().toString() : null);

	            // Convert List<CharSequence> to List<String> for all REPEATED string fields

	            List<String> paymentType = null;
	            if (tax.getPaymentType() != null) {
	                paymentType = tax.getPaymentType().stream()
	                    .map(CharSequence::toString)
	                    .collect(Collectors.toList());
	            }
	            row.set("payment_type", paymentType);

	            List<String> tripCat = null;
	            if (tax.getTripCat() != null) {
	                tripCat = tax.getTripCat().stream()
	                    .map(CharSequence::toString)
	                    .collect(Collectors.toList());
	            }
	            row.set("trip_cat", tripCat);

	            List<String> tripType = null;
	            if (tax.getTripType() != null) {
	                tripType = tax.getTripType().stream()
	                    .map(CharSequence::toString)
	                    .collect(Collectors.toList());
	            }
	            row.set("trip_type", tripType);

	            List<String> visibilityInvoice = null;
	            if (tax.getVisibilityInvoice() != null) {
	                visibilityInvoice = tax.getVisibilityInvoice().stream()
	                    .map(CharSequence::toString)
	                    .collect(Collectors.toList());
	            }
	            row.set("visibility_invoice", visibilityInvoice);

	            List<String> visibilityUi = null;
	            if (tax.getVisibilityUi() != null) {
	                visibilityUi = tax.getVisibilityUi().stream()
	                    .map(CharSequence::toString)
	                    .collect(Collectors.toList());
	            }
	            row.set("visibility_ui", visibilityUi);

	            // --- END OF FIXES ---

	            return row;
	        })
	        .collect(Collectors.toList());
	}


  private static TableRow convertRiderProfile(RiderProfile riderProfile) {
    if (riderProfile == null) return null;
    TableRow row = new TableRow();
    // row.set("rider_profile_id", riderProfile.getRiderProfileId() != null ? riderProfile.getRiderProfileId().toString() : null);
    row.set("business", riderProfile.getBusiness() != null ? riderProfile.getBusiness().toString() : null);
    row.set("business_partner", riderProfile.getBusinessPartner() != null ? riderProfile.getBusinessPartner().toString() : null);
    row.set("email", riderProfile.getEmail() != null ? riderProfile.getEmail().toString() : null);
    row.set("finished_trips", riderProfile.getFinishedTrips());
    row.set("first_name", riderProfile.getFirstName() != null ? riderProfile.getFirstName().toString() : null);
    row.set("full_name", riderProfile.getFullName() != null ? riderProfile.getFullName().toString() : null);
    row.set("gender", riderProfile.getGender() != null ? riderProfile.getGender().toString() : null);
    row.set("last_name", riderProfile.getLastName() != null ? riderProfile.getLastName().toString() : null);
    row.set("phone", riderProfile.getPhone() != null ? riderProfile.getPhone().toString() : null);
    row.set("rating", riderProfile.getRating());
    row.set("review_count", riderProfile.getReviewCount());
    row.set("rider", riderProfile.getRider() != null ? riderProfile.getRider().toString() : null);
    row.set("group", convertGroup(riderProfile.getGroup()));
    return row;
  }

  private static TableRow convertGroup(Group group) {
    if (group == null) return null;
    TableRow row = new TableRow();
    //row.set("group_id", group.getGroupId() != null ? group.getGroupId().toString() : null);
    row.set("name", group.getName() != null ? group.getName().toString() : null);
    row.set("business", group.getBusiness() != null ? group.getBusiness().toString() : null);
    row.set("is_deleted", group.getIsDeleted());
    row.set("program", group.getProgram() != null ? group.getProgram().toString() : null);
    row.set("created_at", group.getCreatedAt() != null ? group.getCreatedAt().toString() : null);
    row.set("updated_at", group.getUpdatedAt() != null ? group.getUpdatedAt().toString() : null);
    row.set("group_version", group.getGroupVersion());
    row.set("budget", convertBudget(group.getBudget()));
    row.set("permissions", convertGroupPermissions(group.getPermissions()));
    return row;
  }

  private static TableRow convertBudget(Budget budget) {
    if (budget == null) return null;
    TableRow row = new TableRow();
    row.set("total", budget.getTotal());
    row.set("monthly_total", budget.getMonthlyTotal());
    row.set("consumed", budget.getConsumed());
    return row;
  }

  private static TableRow convertGroupPermissions(GroupPermissions permissions) {
    if (permissions == null) return null;
    TableRow row = new TableRow();
    row.set("by_pass_balance", permissions.getByPassBalance());
    row.set("auto_approval", permissions.getAutoApproval());
    return row;
  }

  private static TableRow convertRiderReview(RiderReview riderReview) {
	    if (riderReview == null) return null;

	    TableRow row = new TableRow();
	    row.set("comment", riderReview.getComment() != null ? riderReview.getComment().toString() : null);
	    row.set("rating", riderReview.getRating());

	    List<String> predefinedComments = null;
	    if (riderReview.getPredefinedComments() != null) {
	        predefinedComments = riderReview.getPredefinedComments().stream()
	            .map(CharSequence::toString)
	            .collect(Collectors.toList());
	    }
	    row.set("predefined_comments", predefinedComments);

	    return row;
	}


  private static TableRow convertSubTripData(SubTripData subTripData) {
    if (subTripData == null) return null;
    TableRow row = new TableRow();
    row.set("caution", subTripData.getCaution());
    row.set("current_sub_trip_status", subTripData.getCurrentSubTripStatus() != null ? subTripData.getCurrentSubTripStatus().toString() : null);
    row.set("sub_trip_index", subTripData.getSubTripIndex());
    row.set("stops_data", convertStopDataList(subTripData.getStopsData()));
    return row;
  }

  private static List < TableRow > convertStopDataList(List < StopData > stopDataList) {

    if (stopDataList == null) return null;
    return stopDataList.stream()
      .map(stopData -> {
        TableRow row = new TableRow();
        row.set("eta", stopData.getEta());
        row.set("distance", stopData.getDistance());
        row.set("cost", stopData.getCost());
        row.set("pickup", convertStopLocation(stopData.getPickup()));
        row.set("destination", convertStopLocation(stopData.getDestination()));
        row.set("status", stopData.getStatus() != null ? stopData.getStatus().toString() : null);
        // row.set("stop_data_id", stopData.getStopDataId() != null ? stopData.getStopDataId().toString() : null);
        return row;
      })
      .collect(Collectors.toList());
  }

  private static TableRow convertStopLocation(StopLocation stopLocation) {
    if (stopLocation == null) return null;
    TableRow row = new TableRow();
    row.set("location", convertDriverLocation(stopLocation.getLocation()));
    row.set("formatted_address", stopLocation.getFormattedAddress() != null ? stopLocation.getFormattedAddress().toString() : null);
    return row;
  }

  private static TableRow convertSurge(Surge surge) {
    if (surge == null) return null;
    TableRow row = new TableRow();
    row.set("amount", surge.getAmount());
    row.set("flag", surge.getFlag());
    row.set("factor", surge.getFactor());
    return row;
  }

  private static TableRow convertSurgePrivate(SurgePrivate surgePrivate) {
    if (surgePrivate == null) return null;
    TableRow row = new TableRow();
    row.set("amount", surgePrivate.getAmount());
    row.set("flag", surgePrivate.getFlag());
    row.set("rate", surgePrivate.getRate());
    row.set("rule", convertSurgeRule(surgePrivate.getRule()));
    return row;
  }

  private static TableRow convertSurgeRule(SurgeRule surgeRule) {
    if (surgeRule == null) return null;
    TableRow row = new TableRow();
    //  row.set("surge_rule_id", surgeRule.getSurgeRuleId() != null ? surgeRule.getSurgeRuleId().toString() : null);
    row.set("version", surgeRule.getVersion() != null ? surgeRule.getVersion().toString() : null);
    return row;
  }

  /* private static TableRow convertTip(Tip tip) {
        if (tip == null) return null;
        TableRow row = new TableRow();
        row.set("epay", convertTipEpay(tip.getEpay()));
        row.set("ignored", tip.getIgnored());
        row.set("original_value", tip.getOriginalValue());
    s      row.set("payment_method", tip.getPaymentMethod());
        row.set("taxe", convertTaxe(tip.getTaxe()));
        row.set("type", tip.getType());
        row.set("value", tip.getValue());
        return row;
    }

    private static TableRow convertTipEpay(TipEpay tipEpay) {
        if (tipEpay == null) return null;
        TableRow row = new TableRow();
        row.set("order_id", tipEpay.getOrderId());
s        row.set("status", convertTipEpayStatus(tipEpay.getStatus()));
        row.set("type", tipEpay.getType());
        return row;
    }

    private static TableRow convertTipEpayStatus(TipEpayStatus tipEpayStatus) {
        if (tipEpayStatus == null) return null;
        TableRow row = new TableRow();
        row.set("current", tipEpayStatus.getCurrent());
Indentation error        row.set("code", tipEpayStatus.getCode());
        row.set("note", tipEpayStatus.getNote());
        row.set("created_at", tipEpayStatus.getCreatedAt());
        row.set("updated_at", tipEpayStatus.getUpdatedAt());
        return row;
    }

    private static TableRow convertTaxe(Taxe taxe) {
Why        if (taxe == null) return null;
        TableRow row = new TableRow();
        row.set("total", taxe.getTotal());
        row.set("details", taxe.getDetails());
        return row;
    }*/

  private static TableRow convertTripExtension(TripExtension tripExtension) {
    if (tripExtension == null) return null;
    TableRow row = new TableRow();
    row.set("rider_request_extension", tripExtension.getRiderRequestExtension());
    row.set("estimated_cost_before_extension", tripExtension.getEstimatedCostBeforeExtension() != null ? tripExtension.getEstimatedCostBeforeExtension().toString() : null);
    row.set("original_estimated_cost_before_extension", tripExtension.getOriginalEstimatedCostBeforeExtension() != null ? tripExtension.getOriginalEstimatedCostBeforeExtension().toString() : null);
    row.set("enable_trip_extension", tripExtension.getEnableTripExtension());
    return row;
  }
}