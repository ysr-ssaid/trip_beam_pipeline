package org.pipeline.trips;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;

public class TripBigQuerySchema {

    public static TableSchema getBigQuerySchema() {
        List<TableFieldSchema> fields = new ArrayList<>();

        // --- Top Level Simple Fields ---
        fields.add(new TableFieldSchema().setName("_id").setType("STRING"));
     //F   fields.add(new TableFieldSchema().setName("schema_version").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("accepted_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("arrived_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("assigned_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("base_cost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("booked_for").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("business").setType("STRING"));
        fields.add(new TableFieldSchema().setName("campaign_code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("cancel_date_time").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("carpooling").setType("STRING"));
        fields.add(new TableFieldSchema().setName("client").setType("STRING"));
        fields.add(new TableFieldSchema().setName("company").setType("STRING"));
        fields.add(new TableFieldSchema().setName("country").setType("STRING"));
        fields.add(new TableFieldSchema().setName("coupon").setType("STRING"));
       // fields.add(new TableFieldSchema().setName("coupon_id").setType("STRING"));
       // fields.add(new TableFieldSchema().setName("coupon_line_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("created_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("created_from_dash").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("currency").setType("STRING"));
        fields.add(new TableFieldSchema().setName("deflation_factor_code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("directions_service").setType("STRING"));
        fields.add(new TableFieldSchema().setName("discount").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("discounted_cost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("dispatch_counter").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("dispatch_timeout").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("dispatched_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("driver").setType("STRING"));
        fields.add(new TableFieldSchema().setName("estimated_arrived_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("estimated_cost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("estimated_cost_with_boost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("estimated_distance").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("estimated_eta").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("final_destination_arrival_notification_sent").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("finished_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("finished_by").setType("STRING"));
        fields.add(new TableFieldSchema().setName("fraud_status").setType("STRING"));
        fields.add(new TableFieldSchema().setName("hidden_surcharge_rate").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("inflation_factor_code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("is_b2b_reward_trip").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_back_to_back").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_delivery").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_pending3ds_authentication").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_reward").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_used_coupon").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_booked").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_near_destination").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_near_pickup").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("manual_status_override").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("note_internal").setType("STRING"));
        fields.add(new TableFieldSchema().setName("note_rider_internal").setType("STRING"));
        fields.add(new TableFieldSchema().setName("note_rider_to_driver").setType("STRING"));
        fields.add(new TableFieldSchema().setName("original_cost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("original_cost_with_boost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("original_estimated_cost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("payment_variant").setType("STRING"));
        fields.add(new TableFieldSchema().setName("place_count").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("planned_for").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("polyline").setType("STRING"));
        fields.add(new TableFieldSchema().setName("postpaid_cost").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("prepaid_cost").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("radar_enabled").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("radius").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("recov").setType("STRING"));
        fields.add(new TableFieldSchema().setName("recov_ack").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("redispatched_from").setType("STRING"));
        fields.add(new TableFieldSchema().setName("reduction_factor_code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("related_to").setType("STRING"));
        fields.add(new TableFieldSchema().setName("requested_4_b2b_guest").setType("STRING"));
        fields.add(new TableFieldSchema().setName("requested_4_b2b_user").setType("STRING"));
        fields.add(new TableFieldSchema().setName("requested_4_b2c_guest").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("requested_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("rider").setType("STRING"));
        fields.add(new TableFieldSchema().setName("rider_pay").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("rider_redispatched").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("service").setType("STRING"));
        fields.add(new TableFieldSchema().setName("service_config").setType("STRING"));
        fields.add(new TableFieldSchema().setName("service_family").setType("STRING"));
        fields.add(new TableFieldSchema().setName("servicetype").setType("STRING"));
        fields.add(new TableFieldSchema().setName("show_price4_b2b_user").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("started_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("status").setType("STRING"));
        fields.add(new TableFieldSchema().setName("subscriber").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("toll_cost").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("trip_uid").setType("STRING"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("update_date").setType("DATE"));

        // --- Simple Arrays ---
        fields.add(new TableFieldSchema().setName("assigned_drivers").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("candidates_canceled").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("driver_devices_tokens").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("multistops_deflation_factor_code").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("multistops_inflation_factor_code").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("recent_assigned_drivers").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("rider_devices_tokens").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("tags").setType("STRING").setMode("REPEATED"));

        // --- Nested Arrays ---
        fields.add(new TableFieldSchema().setName("adjusted_to").setType("RECORD").setMode("REPEATED").setFields(getAdjustedToSchema()));
        fields.add(new TableFieldSchema().setName("applied_taxes").setType("RECORD").setMode("REPEATED").setFields(getAppliedTaxSchema()));
        fields.add(new TableFieldSchema().setName("applied_taxes_driver").setType("RECORD").setMode("REPEATED").setFields(getAppliedTaxSchema()));
        fields.add(new TableFieldSchema().setName("applied_taxes_rider").setType("RECORD").setMode("REPEATED").setFields(getAppliedTaxSchema()));
        fields.add(new TableFieldSchema().setName("applied_taxes_rider_visible").setType("RECORD").setMode("REPEATED").setFields(getAppliedTaxSchema()));
        fields.add(new TableFieldSchema().setName("applied_taxes_rider_visible_invoices").setType("RECORD").setMode("REPEATED").setFields(getAppliedTaxSchema()));
        fields.add(new TableFieldSchema().setName("candidates").setType("RECORD").setMode("REPEATED").setFields(getCandidateSchema()));
        fields.add(new TableFieldSchema().setName("cost_breakdown_sub_trip").setType("RECORD").setMode("REPEATED").setFields(getCostBreakdownSubTripSchema()));
        fields.add(new TableFieldSchema().setName("driver_review_counts").setType("RECORD").setMode("REPEATED").setFields(getReviewCountSchema()));
        fields.add(new TableFieldSchema().setName("status_history").setType("RECORD").setMode("REPEATED").setFields(getStatusHistorySchema()));
        fields.add(new TableFieldSchema().setName("stops_points_details").setType("RECORD").setMode("REPEATED").setFields(getStopPointDetailsSchema()));

        // --- Nested Objects ---
        fields.add(new TableFieldSchema().setName("currency_label").setType("RECORD").setFields(getLabelSchema()));
        fields.add(new TableFieldSchema().setName("pickup").setType("RECORD").setFields(getLocationSchema()));
        fields.add(new TableFieldSchema().setName("destination").setType("RECORD").setFields(getLocationSchema()));
        fields.add(new TableFieldSchema().setName("pickup_details").setType("RECORD").setFields(getLocationDetailsSchema()));
        fields.add(new TableFieldSchema().setName("destination_details").setType("RECORD").setFields(getLocationDetailsSchema()));
        fields.add(new TableFieldSchema().setName("driver_cancel_reason").setType("RECORD").setFields(getCancelReasonSchema()));
        fields.add(new TableFieldSchema().setName("rider_cancel_reason").setType("RECORD").setFields(getCancelReasonSchema()));
        fields.add(new TableFieldSchema().setName("driver_device").setType("RECORD").setFields(getDeviceProfileSchema()));
        fields.add(new TableFieldSchema().setName("rider_device").setType("RECORD").setFields(getDeviceProfileSchema()));
        fields.add(new TableFieldSchema().setName("driver_review").setType("RECORD").setFields(getReviewSchema()));
        fields.add(new TableFieldSchema().setName("epay").setType("RECORD").setFields(getEpaySchema()));

        // --- Complex Structures ---
        fields.add(new TableFieldSchema().setName("b2b_trip_request_approval").setType("RECORD").setFields(getB2BTripRequestApprovalSchema()));
        fields.add(new TableFieldSchema().setName("booking_configs").setType("RECORD").setFields(getBookingConfigsSchema()));
        fields.add(new TableFieldSchema().setName("boost").setType("RECORD").setFields(getBoostSchema()));
        fields.add(new TableFieldSchema().setName("challenge").setType("RECORD").setFields(getChallengeSchema()));
        fields.add(new TableFieldSchema().setName("cost_breakdown").setType("RECORD").setFields(getCostBreakdownSchema()));        
        fields.add(new TableFieldSchema().setName("data_save_all_changes_on_trip_pricing").setType("RECORD").setFields(getDataSaveAllChangesOnTripPricingSchema()));
        fields.add(new TableFieldSchema().setName("details").setType("RECORD").setFields(getTripDetailsSchema()));
        fields.add(new TableFieldSchema().setName("discount_ratio").setType("RECORD").setFields(getDiscountRatioSchema()));
        fields.add(new TableFieldSchema().setName("dispatch_config").setType("RECORD").setFields(getDispatchConfigSchema()));
        fields.add(new TableFieldSchema().setName("driver_profile").setType("RECORD").setFields(getDriverProfileSchema()));
        fields.add(new TableFieldSchema().setName("driver_shares").setType("RECORD").setFields(getDriverSharesSchema()));
        fields.add(new TableFieldSchema().setName("driver_to_rider").setType("RECORD").setFields(getDriverToRiderProgressSchema()));
        fields.add(new TableFieldSchema().setName("flags").setType("RECORD").setFields(getFlagsSchema()));
        fields.add(new TableFieldSchema().setName("managed").setType("RECORD").setFields(getManagedInfoSchema()));
        fields.add(new TableFieldSchema().setName("original_user").setType("RECORD").setFields(getOriginalUserSchema()));
        fields.add(new TableFieldSchema().setName("re_dispatch").setType("RECORD").setFields(getReDispatchInfoSchema()));
        fields.add(new TableFieldSchema().setName("full_active_taxes_dict").setType("RECORD").setMode("REPEATED").setFields(getTaxMapSchema()));
        fields.add(new TableFieldSchema().setName("rider_profile").setType("RECORD").setFields(getRiderProfileSchema()));
        fields.add(new TableFieldSchema().setName("rider_review").setType("RECORD").setFields(getRiderReviewSchema()));
        fields.add(new TableFieldSchema().setName("sub_trip_data").setType("RECORD").setFields(getSubTripDataSchema()));
        fields.add(new TableFieldSchema().setName("surge").setType("RECORD").setFields(getSurgeSchema()));
        fields.add(new TableFieldSchema().setName("surge_private").setType("RECORD").setFields(getSurgePrivateSchema()));
       // fields.add(new TableFieldSchema().setName("tip").setType("RECORD").setFields(getTipSchema()));
        fields.add(new TableFieldSchema().setName("trip_extension").setType("RECORD").setFields(getTripExtensionSchema()));

        return new TableSchema().setFields(fields);
    }

    // --- Schema definitions for nested structures ---

    private static List<TableFieldSchema> getLabelSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ar").setType("STRING"));
        fields.add(new TableFieldSchema().setName("en").setType("STRING"));
        fields.add(new TableFieldSchema().setName("fr").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getLocationSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("coordinates").setType("FLOAT").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("formatted_address").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getLocationDetailsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("geohashs_h3").setType("RECORD").setFields(getGeoHashH3Schema()));
        fields.add(new TableFieldSchema().setName("lat").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lng").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl0_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl0_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl1_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl1_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl2_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl2_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl3_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl3_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("map_address").setType("STRING"));
        fields.add(new TableFieldSchema().setName("map_path").setType("STRING"));
        fields.add(new TableFieldSchema().setName("zone_code").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getGeoHashH3Schema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("level4").setType("STRING"));
        fields.add(new TableFieldSchema().setName("level5").setType("STRING"));
        fields.add(new TableFieldSchema().setName("level6").setType("STRING"));
        fields.add(new TableFieldSchema().setName("level7").setType("STRING"));
        fields.add(new TableFieldSchema().setName("level8").setType("STRING"));
        fields.add(new TableFieldSchema().setName("level9").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getCancelReasonSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("comment").setType("STRING"));
        fields.add(new TableFieldSchema().setName("reason").setType("STRING"));
      //  fields.add(new TableFieldSchema().setName("reason_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getDeviceProfileSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("active").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("app_lang").setType("STRING"));
        fields.add(new TableFieldSchema().setName("app_version").setType("STRING"));
        fields.add(new TableFieldSchema().setName("brand").setType("STRING"));
        fields.add(new TableFieldSchema().setName("created_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("device_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("device_lang").setType("STRING"));
        fields.add(new TableFieldSchema().setName("fcm_token").setType("STRING"));
        fields.add(new TableFieldSchema().setName("last_used").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("location").setType("RECORD").setFields(getDeviceLocationSchema()));
        fields.add(new TableFieldSchema().setName("model").setType("STRING"));
        fields.add(new TableFieldSchema().setName("os_version").setType("STRING"));
        fields.add(new TableFieldSchema().setName("platform").setType("STRING"));
        fields.add(new TableFieldSchema().setName("retired_at").setType("STRING"));
        fields.add(new TableFieldSchema().setName("ya_device_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getDeviceLocationSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("coordinates").setType("STRING").setMode("REPEATED"));
        return fields;
    }

    private static List<TableFieldSchema> getReviewSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("comment").setType("STRING"));
        fields.add(new TableFieldSchema().setName("rating").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getEpaySchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("amount").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("method").setType("STRING"));
        fields.add(new TableFieldSchema().setName("order_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("second_payment_method").setType("STRING"));
        fields.add(new TableFieldSchema().setName("status").setType("RECORD").setFields(getEpayStatusSchema()));
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getEpayStatusSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("code").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("created_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("current").setType("STRING"));
        fields.add(new TableFieldSchema().setName("note").setType("STRING"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("TIMESTAMP"));
        return fields;
    }

    // --- Schema for arrays ---
    private static List<TableFieldSchema> getAdjustedToSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("trip_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("trip_uid").setType("STRING"));
        fields.add(new TableFieldSchema().setName("status").setType("STRING"));
        fields.add(new TableFieldSchema().setName("adjusted_to_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getAppliedTaxSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("label").setType("RECORD").setFields(getLabelSchema()));
        fields.add(new TableFieldSchema().setName("code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("amount").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("reversable_amount").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("on_yassir_benefit").setType("INTEGER"));
       // fields.add(new TableFieldSchema().setName("tax_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getCandidateSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("driver").setType("STRING"));
        fields.add(new TableFieldSchema().setName("assigned_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("rejected_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("rejection_type").setType("STRING"));
       // fields.add(new TableFieldSchema().setName("candidate_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getCostBreakdownSubTripSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("initial_cost").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("original_cost").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("net_cost_initial").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("net_cost_post_discounts").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("total_taxes").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("total_taxes_rider").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("total_taxes_driver").setType("INTEGER"));
      //  fields.add(new TableFieldSchema().setName("cost_breakdown_sub_trip_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getReviewCountSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("comment").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
       // fields.add(new TableFieldSchema().setName("review_count_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getStatusHistorySchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
       // fields.add(new TableFieldSchema().setName("status_history_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("driver").setType("STRING"));
        fields.add(new TableFieldSchema().setName("insert_from").setType("STRING"));
        fields.add(new TableFieldSchema().setName("location").setType("RECORD").setFields(getDriverLocationSchema()));
        fields.add(new TableFieldSchema().setName("status").setType("STRING"));
        fields.add(new TableFieldSchema().setName("time").setType("TIMESTAMP"));
        return fields;
    }

    private static List<TableFieldSchema> getStopPointDetailsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("lvl0_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl1_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl2_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl3_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl0_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl1_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl2_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lvl3_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("map_path").setType("STRING"));
        fields.add(new TableFieldSchema().setName("map_address").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lat").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lng").setType("STRING"));
        fields.add(new TableFieldSchema().setName("zone_code").setType("STRING"));
       // fields.add(new TableFieldSchema().setName("stop_point_details_id").setType("STRING"));
        return fields;
    }

    // --- Schema for complex structures ---
    private static List<TableFieldSchema> getB2BTripRequestApprovalSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("approval_needed").setType("BOOLEAN"));
        return fields;
    }

    private static List<TableFieldSchema> getBookingConfigsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("driver_confirmation_timeout_minutes").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("ops_manual_dispatch_window_minutes").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("ops_manual_rider_confirmation_window_minutes").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("post_rider_confirmation_dispatch_minutes").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("pre_booking_driver_confirmation_minutes").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("pre_booking_rider_confirmation_minutes").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("rider_confirmation_timeout_minutes").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getBoostSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("value").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("history").setType("STRING").setMode("REPEATED"));
        return fields;
    }

    private static List<TableFieldSchema> getChallengeSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("uid").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getCostBreakdownSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("initial_cost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("original_cost").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("net_cost_initial").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("net_cost_post_discounts").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("total_taxes").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("total_taxes_rider").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("total_taxes_driver").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("yassir_share").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("b2b_commission_rate").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("b2b_commission_amount").setType("FLOAT"));
        return fields;
    }

    // --- START OF MODIFIED SECTION ---

    private static List<TableFieldSchema> getDataSaveAllChangesOnTripPricingSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("coupon").setType("RECORD").setFields(getCouponDetailsSchema()));
        fields.add(new TableFieldSchema().setName("coupon_line_obj").setType("RECORD").setFields(getCouponLineObjSchema()));
        fields.add(new TableFieldSchema().setName("discount_values").setType("RECORD").setFields(getDiscountValuesSchema()));
        fields.add(new TableFieldSchema().setName("inflation_values").setType("RECORD").setFields(getInflationValuesSchema()));
        fields.add(new TableFieldSchema().setName("pricing_line").setType("RECORD").setFields(getPricingLineSchema()));
        fields.add(new TableFieldSchema().setName("pricing_lines").setType("RECORD").setMode("REPEATED").setFields(getPricingLineSimpleSchema()));
        fields.add(new TableFieldSchema().setName("show_applied_reduction").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_reduction_accumulative").setType("BOOLEAN"));
        return fields;
    }

  
    
    private static List<TableFieldSchema> getCouponDetailsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("is_used").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_valid").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("v").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getCouponLineObjSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("discount_ratio").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("discount_flat").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("max_amount").setType("INTEGER"));
       // fields.add(new TableFieldSchema().setName("couponline_id").setType("STRING"));
      //  fields.add(new TableFieldSchema().setName("coupon_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("service").setType("STRING"));
        fields.add(new TableFieldSchema().setName("max_price").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("min_price").setType("INTEGER"));
     //   fields.add(new TableFieldSchema().setName("coupon_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getDiscountValuesSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("auto_discount_ratio").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("code_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("discount_ratio").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("reduction_discounted_amount").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("discount_flat").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("max_amount").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("campaign_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("show_applied_reduction").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("is_reduction_accumulative").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("min_price").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("max_price").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getInflationValuesSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        //fields.add(new TableFieldSchema().setName("inflation_values_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("code_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("inflation_max_amount").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("inflation_min_amount").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("inflation_ratio").setType("FLOAT"));
        return fields;
    }
    

    private static List<TableFieldSchema> getPricingLineSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("destination").setType("STRING"));
        fields.add(new TableFieldSchema().setName("destination_zone").setType("STRING"));
        fields.add(new TableFieldSchema().setName("pickup").setType("STRING"));
        fields.add(new TableFieldSchema().setName("pickup_zone").setType("STRING"));
        fields.add(new TableFieldSchema().setName("service").setType("STRING"));
        fields.add(new TableFieldSchema().setName("st").setType("STRING"));
        fields.add(new TableFieldSchema().setName("tarifs").setType("RECORD").setFields(getTarifsSchema()));
        fields.add(new TableFieldSchema().setName("variant").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getTarifsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("min_price").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("base_price").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("minute_price").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("km_price").setType("FLOAT"));
        return fields;
    }

    private static List<TableFieldSchema> getPricingLineSimpleSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("pickup").setType("STRING"));
        fields.add(new TableFieldSchema().setName("destination").setType("STRING"));
        fields.add(new TableFieldSchema().setName("tarifs").setType("RECORD").setFields(getTarifsSimpleSchema()));
        fields.add(new TableFieldSchema().setName("variant").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getTarifsSimpleSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("min_price").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("base_price").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("minute_price").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("km_price").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("variant").setType("STRING"));
        return fields;
    }
    
    // --- END OF MODIFIED SECTION ---

    private static List<TableFieldSchema> getTripDetailsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("service").setType("RECORD").setFields(getServiceSchema()));
        fields.add(new TableFieldSchema().setName("servicetype").setType("RECORD").setFields(getServiceTypeSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getServiceSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("label").setType("RECORD").setFields(getLabelSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getServiceTypeSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("label").setType("RECORD").setFields(getLabelSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getDiscountRatioSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("percent").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("amount").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getDispatchConfigSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("max_candidates").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("max_requests").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("drivers_batch").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getDriverProfileSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("app_platform").setType("STRING"));
        fields.add(new TableFieldSchema().setName("app_version").setType("STRING"));
        fields.add(new TableFieldSchema().setName("car").setType("RECORD").setFields(getCarSchema()));
        fields.add(new TableFieldSchema().setName("company").setType("STRING"));
        fields.add(new TableFieldSchema().setName("country").setType("STRING"));
        fields.add(new TableFieldSchema().setName("display_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("driver").setType("STRING"));
        fields.add(new TableFieldSchema().setName("driver_uid").setType("STRING"));
        fields.add(new TableFieldSchema().setName("full_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("gender").setType("STRING"));
        fields.add(new TableFieldSchema().setName("is_verified").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("location").setType("RECORD").setFields(getDriverLocationSchema()));
        fields.add(new TableFieldSchema().setName("phone").setType("STRING"));
        fields.add(new TableFieldSchema().setName("picture").setType("STRING"));
        fields.add(new TableFieldSchema().setName("rating").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("status").setType("STRING"));
        fields.add(new TableFieldSchema().setName("wilaya").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getDriverLocationSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("lat").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("lng").setType("FLOAT"));
        return fields;
    }

    private static List<TableFieldSchema> getCarSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("model").setType("STRING"));
        fields.add(new TableFieldSchema().setName("marque").setType("STRING"));
        fields.add(new TableFieldSchema().setName("immat").setType("STRING"));
        fields.add(new TableFieldSchema().setName("color").setType("STRING"));
        fields.add(new TableFieldSchema().setName("energie").setType("STRING"));
        fields.add(new TableFieldSchema().setName("year").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getDriverSharesSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("balance").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("benefit").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("benefit_net").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("driver2yassir").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("driver_benefit_tax").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("yassir2driver").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("driver_taxes_list_visible").setType("RECORD").setMode("REPEATED").setFields(getDriverTaxSchema()));
        fields.add(new TableFieldSchema().setName("yassir_commission").setType("RECORD").setFields(getYassirCommissionSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getDriverTaxSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("amount").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("label").setType("RECORD").setFields(getLabelSchema()));
        fields.add(new TableFieldSchema().setName("code").setType("STRING"));
      //  fields.add(new TableFieldSchema().setName("driver_tax_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getYassirCommissionSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("is_reduced").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("percent").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("history").setType("RECORD").setMode("REPEATED").setFields(getCommissionHistorySchema()));
        return fields;
    }

    private static List<TableFieldSchema> getCommissionHistorySchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("new_percent_value").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("old_percent_value").setType("FLOAT"));
     //   fields.add(new TableFieldSchema().setName("reduction_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("reduction_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getDriverToRiderProgressSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("driver_progress").setType("RECORD").setFields(getProgressDetailsSchema()));
        fields.add(new TableFieldSchema().setName("estimated_arrival_time").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("estimated_distance").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("estimated_time").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("polyline").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getProgressDetailsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("accuracy").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("bearing").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("direction_updated_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("estimated_arrival_time").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("estimated_distance").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("estimated_time").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("polyline").setType("STRING"));
        fields.add(new TableFieldSchema().setName("speed").setType("FLOAT"));
        return fields;
    }

    private static List<TableFieldSchema> getFlagsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("should_reduce_balance").setType("BOOLEAN"));
        return fields;
    }

    private static List<TableFieldSchema> getManagedInfoSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("by").setType("STRING"));
        fields.add(new TableFieldSchema().setName("at").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getOriginalUserSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("full_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("last_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("first_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("phone").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getReDispatchInfoSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("initial").setType("STRING"));
        fields.add(new TableFieldSchema().setName("previous").setType("STRING"));
        fields.add(new TableFieldSchema().setName("retries").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getTaxMapSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("key").setType("STRING"));
        fields.add(new TableFieldSchema().setName("value").setType("RECORD").setMode("REPEATED").setFields(getTaxSchema()));
        return fields;
    }
    

    private static List<TableFieldSchema> getTaxSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
      //  fields.add(new TableFieldSchema().setName("tax_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("country").setType("STRING"));
        fields.add(new TableFieldSchema().setName("label").setType("RECORD").setFields(getLabelSchema()));
        fields.add(new TableFieldSchema().setName("level_path").setType("STRING"));
        fields.add(new TableFieldSchema().setName("max_tax_value").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("max_value").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("min_tax_value").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("min_value").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("order").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("payment_type").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("percent").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("reverse").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("service").setType("STRING"));
        fields.add(new TableFieldSchema().setName("servicetype").setType("STRING"));
        fields.add(new TableFieldSchema().setName("st").setType("STRING"));
        fields.add(new TableFieldSchema().setName("trip_cat").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("trip_type").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("value").setType("STRING"));
        fields.add(new TableFieldSchema().setName("value_type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("visibility_invoice").setType("STRING").setMode("REPEATED"));
        fields.add(new TableFieldSchema().setName("visibility_ui").setType("STRING").setMode("REPEATED"));
        return fields;
    }

    private static List<TableFieldSchema> getRiderProfileSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        //fields.add(new TableFieldSchema().setName("rider_profile_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("business").setType("STRING"));
        fields.add(new TableFieldSchema().setName("business_partner").setType("STRING"));
        fields.add(new TableFieldSchema().setName("email").setType("STRING"));
        fields.add(new TableFieldSchema().setName("finished_trips").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("first_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("full_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("gender").setType("STRING"));
        fields.add(new TableFieldSchema().setName("last_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("phone").setType("STRING"));
        fields.add(new TableFieldSchema().setName("rating").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("review_count").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("rider").setType("STRING"));
        fields.add(new TableFieldSchema().setName("group").setType("RECORD").setFields(getGroupSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getGroupSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
       // fields.add(new TableFieldSchema().setName("group_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("business").setType("STRING"));
        fields.add(new TableFieldSchema().setName("is_deleted").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("program").setType("STRING"));
        fields.add(new TableFieldSchema().setName("created_at").setType("STRING"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("STRING"));
        fields.add(new TableFieldSchema().setName("group_version").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("budget").setType("RECORD").setFields(getBudgetSchema()));
        fields.add(new TableFieldSchema().setName("permissions").setType("RECORD").setFields(getGroupPermissionsSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getBudgetSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("total").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("monthly_total").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("consumed").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getGroupPermissionsSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("by_pass_balance").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("auto_approval").setType("BOOLEAN"));
        return fields;
    }

    private static List<TableFieldSchema> getRiderReviewSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("comment").setType("STRING"));
        fields.add(new TableFieldSchema().setName("rating").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("predefined_comments").setType("STRING").setMode("REPEATED"));
        return fields;
    }

    private static List<TableFieldSchema> getSubTripDataSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("caution").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("current_sub_trip_status").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sub_trip_index").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("stops_data").setType("RECORD").setMode("REPEATED").setFields(getStopDataSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getStopDataSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("eta").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("distance").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("cost").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("pickup").setType("RECORD").setFields(getStopLocationSchema()));
        fields.add(new TableFieldSchema().setName("destination").setType("RECORD").setFields(getStopLocationSchema()));
        fields.add(new TableFieldSchema().setName("status").setType("STRING"));
     //   fields.add(new TableFieldSchema().setName("stop_data_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getStopLocationSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("location").setType("RECORD").setFields(getDriverLocationSchema()));
        fields.add(new TableFieldSchema().setName("formatted_address").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getSurgeSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("amount").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("flag").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("factor").setType("FLOAT"));
        return fields;
    }

    private static List<TableFieldSchema> getSurgePrivateSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("amount").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("flag").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("rate").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("rule").setType("RECORD").setFields(getSurgeRuleSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getSurgeRuleSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
      //  fields.add(new TableFieldSchema().setName("surge_rule_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("version").setType("STRING"));
        return fields;
    }
    
    //tip schema is disabled for now

 /*   private static List<TableFieldSchema> getTipSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("epay").setType("RECORD").setFields(getTipEpaySchema()));
        fields.add(new TableFieldSchema().setName("ignored").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("original_value").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("payment_method").setType("STRING"));
        fields.add(new TableFieldSchema().setName("taxe").setType("RECORD").setFields(getTaxeSchema()));
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("value").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getTipEpaySchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("amount").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("order_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("status").setType("RECORD").setFields(getTipEpayStatusSchema()));
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getTipEpayStatusSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("current").setType("STRING"));
        fields.add(new TableFieldSchema().setName("code").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("note").setType("STRING"));
        fields.add(new TableFieldSchema().setName("created_at").setType("STRING"));
        fields.add(new TableFieldSchema().setName("updated_at").setType("STRING"));
        fields.add(new TableFieldSchema().setName("order_id").setType("STRING"));
        return fields;
    }

    private static List<TableFieldSchema> getTaxeSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("total").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("details").setType("RECORD").setMode("REPEATED").setFields(getTipTaxDetailSchema()));
        return fields;
    }

    private static List<TableFieldSchema> getTipTaxDetailSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("amount").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("label").setType("RECORD").setFields(getLabelSchema()));
      //  fields.add(new TableFieldSchema().setName("tip_tax_detail_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("originalValue").setType("INTEGER"));
        return fields;
    }
*/
    private static List<TableFieldSchema> getTripExtensionSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("rider_request_extension").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("estimated_cost_before_extension").setType("STRING"));
        fields.add(new TableFieldSchema().setName("original_estimated_cost_before_extension").setType("STRING"));
        fields.add(new TableFieldSchema().setName("enable_trip_extension").setType("BOOLEAN"));
        return fields;
    }
}