package org.pipeline.trips;

import org.bson.Document;
import org.pipeline.trips.avro.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.util.Objects;

import static org.pipeline.trips.MongoAvroUtils.*;
import static org.pipeline.trips.TripAvroBuilderUtils.*;

public class TripComplexBuilders {

    public static B2BTripRequestApproval buildB2BTripRequestApproval(Document doc) {
        if (doc == null) return null;
        return B2BTripRequestApproval.newBuilder()
            .setApprovalNeeded(safeGetBoolean(doc, "approval_needed"))
            .build();
    }

    public static BookingConfigs buildBookingConfigs(Document doc) {
        if (doc == null) return null;
        return BookingConfigs.newBuilder()
            .setDriverConfirmationTimeoutMinutes(safeGetInt(doc, "driverConfirmationTimeoutMinutes"))
            .setOpsManualDispatchWindowMinutes(safeGetInt(doc, "opsManualDispatchWindowMinutes"))
            .setOpsManualRiderConfirmationWindowMinutes(safeGetInt(doc, "opsManualRiderConfirmationWindowMinutes"))
            .setPostRiderConfirmationDispatchMinutes(safeGetInt(doc, "postRiderConfirmationDispatchMinutes"))
            .setPreBookingDriverConfirmationMinutes(safeGetInt(doc, "preBookingDriverConfirmationMinutes"))
            .setPreBookingRiderConfirmationMinutes(safeGetInt(doc, "preBookingRiderConfirmationMinutes"))
            .setRiderConfirmationTimeoutMinutes(safeGetInt(doc, "riderConfirmationTimeoutMinutes"))
            .build();
    }

    public static Boost buildBoost(Document doc) {
        if (doc == null) return null;
        Boost.Builder builder = Boost.newBuilder().setValue(safeGetInt(doc, "value"));
        List<String> historyList = safeGetList(doc, "history", String.class);
        if (historyList != null) {
            builder.setHistory(new ArrayList<>(historyList));
        }
        return builder.build();
    }

    public static Challenge buildChallenge(Document doc) {
        if (doc == null) return null;
        return Challenge.newBuilder()
            .setId(safeGetString(doc, "id"))
            .setUid(safeGetString(doc, "uid"))
            .build();
    }

    public static CostBreakdown buildCostBreakdown(Document doc) {
        if (doc == null) return null;
        return CostBreakdown.newBuilder()
            .setInitialCost(safeGetDouble(doc, "initial_cost"))
            .setOriginalCost(safeGetDouble(doc, "original_cost"))
            .setNetCostInitial(safeGetDouble(doc, "net_cost_initial"))
            .setNetCostPostDiscounts(safeGetDouble(doc, "net_cost_post_discounts"))
            .setTotalTaxes(safeGetDouble(doc, "total_taxes"))
            .setTotalTaxesRider(safeGetDouble(doc, "total_taxes_rider"))
            .setTotalTaxesDriver(safeGetDouble(doc, "total_taxes_driver"))
            .setYassirShare(safeGetInt(doc, "yassirShare"))
            .setB2bCommissionRate(safeGetDouble(doc, "b2b_commission_rate"))
            .setB2bCommissionAmount(safeGetDouble(doc, "b2b_commission_amount"))
            .build();
    }

    public static DataSaveAllChangesOnTripPricing buildDataSaveAllChangesOnTripPricing(Document dataSaveDoc) {
        if (dataSaveDoc == null) return null;
        DataSaveAllChangesOnTripPricing.Builder builder = DataSaveAllChangesOnTripPricing.newBuilder();

        Document couponDetailsDoc = safeGetDocument(dataSaveDoc, "coupon");
        if (couponDetailsDoc != null) {
            builder.setCoupon(buildCouponDetails(couponDetailsDoc));
        }

        Document couponLineObjDoc = safeGetDocument(dataSaveDoc, "couponLineObj");
        if (couponLineObjDoc != null) {
            builder.setCouponLineObj(buildCouponLineObj(couponLineObjDoc));
        }

        Document discountValuesDoc = safeGetDocument(dataSaveDoc, "discountValues");
        if (discountValuesDoc != null) {
            builder.setDiscountValues(buildDiscountValues(discountValuesDoc));
        }

        Document inflationValuesDoc = safeGetDocument(dataSaveDoc, "inflationValues");
        if (inflationValuesDoc != null) {
            builder.setInflationValues(buildInflationValues(inflationValuesDoc));
        }

        Document pricingLineDoc = safeGetDocument(dataSaveDoc, "pricingLine");
        if (pricingLineDoc != null) {
            builder.setPricingLine(buildPricingLine(pricingLineDoc));
        }

        List<Document> pricingLinesRaw = safeGetList(dataSaveDoc, "pricingLines", Document.class);
        if (pricingLinesRaw != null) {
            builder.setPricingLines(pricingLinesRaw.stream()
                .map(TripArrayBuilders::buildPricingLineSimple)
                .collect(Collectors.toList()));
        }

        return builder.build();
    }

    private static CouponDetails buildCouponDetails(Document doc) {
        if (doc == null) return null;
        return CouponDetails.newBuilder()
            .setCode(safeGetString(doc, "code"))
            .setIsUsed(safeGetBoolean(doc, "isUsed"))
            .setIsValid(safeGetBoolean(doc, "isValid"))
            .setV(safeGetInt(doc, "v"))
            .build();
    }

    private static CouponLineObj buildCouponLineObj(Document doc) {
        if (doc == null) return null;
        return CouponLineObj.newBuilder()
            .setDiscountRatio(safeGetDouble(doc, "discountRatio"))
            .setDiscountFlat(safeGetInt(doc, "discountFlat"))
            .setMaxAmount(safeGetInt(doc, "maxAmount"))
            .setCouponlineID(safeGetString(doc, "couponlineID"))
            .setCouponName(safeGetString(doc, "couponName"))
            .setService(safeGetString(doc, "service"))
            .setMaxPrice(safeGetDouble(doc, "maxPrice"))
            .setMinPrice(safeGetInt(doc, "minPrice"))
            .setCouponId(safeGetString(doc, "couponId"))
            .build();
    }

    private static DiscountValues buildDiscountValues(Document doc) {
        if (doc == null) return null;
        return DiscountValues.newBuilder()
            .setAutoDiscountRatio(safeGetDouble(doc, "autoDiscountRatio"))
            .setCodeName(safeGetString(doc, "codeName"))
            .setDiscountRatio(safeGetDouble(doc, "discountRatio"))
            .setReductionDiscountedAmount(safeGetDouble(doc, "reductionDiscountedAmount"))
            .setDiscountFlat(safeGetInt(doc, "discountFlat"))
            .setMaxAmount(safeGetInt(doc, "maxAmount"))
            .setCampaignName(safeGetString(doc, "campaignName"))
            .setShowAppliedReduction(safeGetBoolean(doc, "showAppliedReduction"))
            .setIsReductionAccumulative(safeGetBoolean(doc, "isReductionAccumulative"))
            .setMinPrice(safeGetInt(doc, "minPrice"))
            .setMaxPrice(safeGetInt(doc, "maxPrice"))
            .build();
    }

    private static InflationValues buildInflationValues(Document doc) {
        if (doc == null) return null;
        return InflationValues.newBuilder()
            .setInflationValuesId(safeGetString(doc, "inflation_values_id"))
            .setCodeName(safeGetString(doc, "codeName"))
            .setInflationMaxAmount(safeGetInt(doc, "inflationMaxAmount"))
            .setInflationMinAmount(safeGetInt(doc, "inflationMinAmount"))
            .setInflationRatio(safeGetDouble(doc, "inflationRatio"))
            .build();
    }

    private static PricingLine buildPricingLine(Document doc) {
        if (doc == null) return null;
        return PricingLine.newBuilder()
            .setDestination(safeGetString(doc, "destination"))
            .setDestinationZone(safeGetString(doc, "destination_zone"))
            .setPickup(safeGetString(doc, "pickup"))
            .setPickupZone(safeGetString(doc, "pickup_zone"))
            .setService(safeGetString(doc, "service"))
            .setSt(safeGetString(doc, "st"))
            .setTarifs(buildTarifs(safeGetDocument(doc, "tarifs")))
            .setVariant(safeGetString(doc, "variant"))
            .build();
    }

    private static Tarifs buildTarifs(Document doc) {
        if (doc == null) return null;
        return Tarifs.newBuilder()
            .setMinPrice(safeGetInt(doc, "min_price"))
            .setBasePrice(safeGetDouble(doc, "base_price"))
            .setMinutePrice(safeGetDouble(doc, "minute_price"))
            .setKmPrice(safeGetDouble(doc, "km_price"))
            .build();
    }

    public static TripDetails buildTripDetails(Document doc) {
        if (doc == null) return null;
        TripDetails.Builder builder = TripDetails.newBuilder();

        Document serviceDoc = safeGetDocument(doc, "service");
        if (serviceDoc != null) {
            builder.setService(buildService(serviceDoc));
        }

        Document serviceTypeDoc = safeGetDocument(doc, "servicetype");
        if (serviceTypeDoc != null) {
            builder.setServicetype(buildServiceType(serviceTypeDoc));
        }

        return builder.build();
    }

    private static Service buildService(Document doc) {
        if (doc == null) return null;
        return Service.newBuilder()
            .setId(safeGetString(doc, "id"))
            .setLabel(buildLabel(safeGetDocument(doc, "label")))
            .build();
    }

    private static ServiceType buildServiceType(Document doc) {
        if (doc == null) return null;
        return ServiceType.newBuilder()
            .setId(safeGetString(doc, "id"))
            .setLabel(buildLabel(safeGetDocument(doc, "label")))
            .build();
    }

    public static DiscountRatio buildDiscountRatio(Document doc) {
        if (doc == null) return null;
        return DiscountRatio.newBuilder()
            .setPercent(safeGetDouble(doc, "percent"))
            .setAmount(safeGetInt(doc, "amount"))
            .build();
    }

    public static DispatchConfig buildDispatchConfig(Document doc) {
        if (doc == null) return null;
        return DispatchConfig.newBuilder()
            .setMaxCandidates(safeGetInt(doc, "max_candidates"))
            .setMaxRequests(safeGetInt(doc, "max_requests"))
            .setDriversBatch(safeGetInt(doc, "drivers_batch"))
            .build();
    }

    public static DriverProfile buildDriverProfile(Document doc) {
        if (doc == null) return null;
        DriverProfile.Builder builder = DriverProfile.newBuilder()
            .setAppPlatform(safeGetString(doc, "app_platform"))
            .setAppVersion(safeGetString(doc, "app_version"))
            .setCompany(safeGetString(doc, "company"))
            .setCountry(safeGetString(doc, "country"))
            .setDisplayName(safeGetString(doc, "display_name"))
            .setDriver(safeGetString(doc, "driver"))
            .setDriverUid(safeGetString(doc, "driver_uid"))
            .setFullName(safeGetString(doc, "full_name"))
            .setGender(safeGetString(doc, "gender"))
            .setIsVerified(safeGetBoolean(doc, "is_verified"))
            .setLocation(buildDriverLocation(safeGetDocument(doc, "location")))
            .setPhone(safeGetString(doc, "phone"))
            .setPicture(safeGetString(doc, "picture"))
            .setRating(safeGetDouble(doc, "rating"))
            .setStatus(safeGetString(doc, "status"))
            .setWilaya(safeGetInt(doc, "wilaya"));

        Document carDoc = safeGetDocument(doc, "car");
        if (carDoc != null) {
            builder.setCar(buildCar(carDoc));
        }

        return builder.build();
    }

    private static Car buildCar(Document doc) {
        if (doc == null) return null;
        return Car.newBuilder()
            .setModel(safeGetString(doc, "model"))
            .setMarque(safeGetString(doc, "marque"))
            .setImmat(safeGetString(doc, "immat"))
            .setColor(safeGetString(doc, "color"))
            .setEnergie(safeGetString(doc, "energie"))
            .setYear(safeGetInt(doc, "year"))
            .build();
    }

    public static DriverShares buildDriverShares(Document doc) {
        if (doc == null) return null;
        DriverShares.Builder builder = DriverShares.newBuilder()
            .setBalance(safeGetDouble(doc, "balance"))
            .setBenefit(safeGetDouble(doc, "benefit"))
            .setBenefitNet(safeGetDouble(doc, "benefit_net"))
            .setDriver2yassir(safeGetDouble(doc, "driver2yassir"))
            .setDriverBenefitTax(safeGetInt(doc, "driver_benefit_tax"))
            .setYassir2driver(safeGetDouble(doc, "yassir2driver"));

        List<Document> taxesVisibleRaw = safeGetList(doc, "driver_taxes_list_visible", Document.class);
        if (taxesVisibleRaw != null) {
            builder.setDriverTaxesListVisible(taxesVisibleRaw.stream()
                .map(TripArrayBuilders::buildDriverTax)
                .collect(Collectors.toList()));
        }

        Document commissionDoc = safeGetDocument(doc, "yassirCommission");
        if (commissionDoc != null) {
            builder.setYassirCommission(buildYassirCommission(commissionDoc));
        }

        return builder.build();
    }

    private static YassirCommission buildYassirCommission(Document doc) {
        if (doc == null) return null;
        YassirCommission.Builder builder = YassirCommission.newBuilder()
            .setIsReduced(safeGetBoolean(doc, "isReduced"))
            .setPercent(safeGetDouble(doc, "percent"));

        List<Document> historyRaw = safeGetList(doc, "history", Document.class);
        if (historyRaw != null) {
            builder.setHistory(historyRaw.stream()
                .map(TripArrayBuilders::buildCommissionHistory)
                .collect(Collectors.toList()));
        }

        return builder.build();
    }

    public static DriverToRiderProgress buildDriverToRiderProgress(Document doc) {
        if (doc == null) return null;
        DriverToRiderProgress.Builder builder = DriverToRiderProgress.newBuilder()
            .setEstimatedArrivalTime(safeGetInstant(doc, "estimated_arrival_time"))
            .setEstimatedDistance(safeGetInt(doc, "estimated_distance"))
            .setEstimatedTime(safeGetInt(doc, "estimated_time"))
            .setPolyline(safeGetString(doc, "polyline"));

        Document progressDoc = safeGetDocument(doc, "driver_progress");
        if (progressDoc != null) {
            builder.setDriverProgress(buildProgressDetails(progressDoc));
        }

        return builder.build();
    }

    private static ProgressDetails buildProgressDetails(Document doc) {
        if (doc == null) return null;
        return ProgressDetails.newBuilder()
            .setAccuracy(safeGetDouble(doc, "accuracy"))
            .setBearing(safeGetDouble(doc, "bearing"))
            .setDirectionUpdatedAt(safeGetInstant(doc, "direction_updated_at"))
            .setEstimatedArrivalTime(safeGetInstant(doc, "estimated_arrival_time"))
            .setEstimatedDistance(safeGetInt(doc, "estimated_distance"))
            .setEstimatedTime(safeGetInt(doc, "estimated_time"))
            .setPolyline(safeGetString(doc, "polyline"))
            .setSpeed(safeGetDouble(doc, "speed"))
            .build();
    }

    public static Flags buildFlags(Document doc) {
        if (doc == null) return null;
        return Flags.newBuilder()
            .setShouldReduceBalance(safeGetBoolean(doc, "shouldReduceBalance"))
            .build();
    }

    public static ManagedInfo buildManagedInfo(Document doc) {
        if (doc == null) return null;
        return ManagedInfo.newBuilder()
            .setBy(safeGetString(doc, "by"))
            .setAt(safeGetString(doc, "at"))
            .build();
    }

    public static OriginalUser buildOriginalUser(Document doc) {
        if (doc == null) return null;
        return OriginalUser.newBuilder()
            .setFullName(safeGetString(doc, "full_name"))
            .setLastName(safeGetString(doc, "lastName"))
            .setFirstName(safeGetString(doc, "firstName"))
            .setPhone(safeGetString(doc, "phone"))
            .build();
    }

    public static ReDispatchInfo buildReDispatchInfo(Document doc) {
        if (doc == null) return null;
        return ReDispatchInfo.newBuilder()
            .setInitial(safeGetString(doc, "initial"))
            .setPrevious(safeGetString(doc, "previous"))
            .setRetries(safeGetInt(doc, "retries"))
            .build();
    }

    public static Map<CharSequence, List<Tax>> buildFullActiveTaxesDict(Document doc) {
        if (doc == null) return null;
        Map<CharSequence, List<Tax>> avroTaxMap = new HashMap<>();
        
        for (String taxCategoryKey : doc.keySet()) {
            List<Document> taxListRaw = safeGetList(doc, taxCategoryKey, Document.class);
            if (taxListRaw != null) {
                List<Tax> avroTaxList = taxListRaw.stream()
                    .map(TripAvroBuilderUtils::buildTax)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                avroTaxMap.put(taxCategoryKey, avroTaxList);
            }
        }
        return avroTaxMap;
    }

    public static RiderProfile buildRiderProfile(Document doc) {
        if (doc == null) return null;
        RiderProfile.Builder builder = RiderProfile.newBuilder()
            .setRiderProfileId(safeGetString(doc, "_id"))
            .setBusiness(safeGetString(doc, "business"))
            .setBusinessPartner(safeGetString(doc, "business_partner"))
            .setEmail(safeGetString(doc, "email"))
            .setFinishedTrips(safeGetInt(doc, "finished_trips"))
            .setFirstName(safeGetString(doc, "first_name"))
            .setFullName(safeGetString(doc, "full_name"))
            .setGender(safeGetString(doc, "gender"))
            .setLastName(safeGetString(doc, "last_name"))
            .setPhone(safeGetString(doc, "phone"))
            .setRating(safeGetDouble(doc, "rating"))
            .setReviewCount(safeGetInt(doc, "review_count"))
            .setRider(safeGetString(doc, "rider"));

        Document groupDoc = safeGetDocument(doc, "group");
        if (groupDoc != null) {
            builder.setGroup(buildGroup(groupDoc));
        }

        return builder.build();
    }

    private static Group buildGroup(Document doc) {
        if (doc == null) return null;
        Group.Builder builder = Group.newBuilder()
            .setGroupId(safeGetString(doc, "_id"))
            .setName(safeGetString(doc, "name"))
            .setBusiness(safeGetString(doc, "business"))
            .setIsDeleted(safeGetBoolean(doc, "isDeleted"))
            .setProgram(safeGetString(doc, "program"))
            .setCreatedAt(safeGetString(doc, "created_at"))
            .setUpdatedAt(safeGetString(doc, "updated_at"))
            .setGroupVersion(safeGetInt(doc, "__v"));

        Document budgetDoc = safeGetDocument(doc, "budget");
        if (budgetDoc != null) {
            builder.setBudget(buildBudget(budgetDoc));
        }

        Document groupPermsDoc = safeGetDocument(doc, "permissions");
        if (groupPermsDoc != null) {
            builder.setPermissions(buildGroupPermissions(groupPermsDoc));
        }

        return builder.build();
    }

    private static Budget buildBudget(Document doc) {
        if (doc == null) return null;
        return Budget.newBuilder()
            .setTotal(safeGetInt(doc, "total"))
            .setMonthlyTotal(safeGetInt(doc, "monthlyTotal"))
            .setConsumed(safeGetInt(doc, "consumed"))
            .build();
    }

    private static GroupPermissions buildGroupPermissions(Document doc) {
        if (doc == null) return null;
        return GroupPermissions.newBuilder()
            .setByPassBalance(safeGetBoolean(doc, "byPassBalance"))
            .setAutoApproval(safeGetBoolean(doc, "autoApproval"))
            .build();
    }

    public static RiderReview buildRiderReview(Document doc) {
        if (doc == null) return null;
        RiderReview.Builder builder = RiderReview.newBuilder()
            .setComment(safeGetString(doc, "comment"))
            .setRating(safeGetInt(doc, "rating"));

        List<String> predefined = safeGetList(doc, "predefined_comments", String.class);
        if (predefined != null) {
            builder.setPredefinedComments(new ArrayList<>(predefined));
        }

        return builder.build();
    }

    public static SubTripData buildSubTripData(Document doc) {
        if (doc == null) return null;
        SubTripData.Builder builder = SubTripData.newBuilder()
            .setCaution(safeGetInt(doc, "caution"))
            .setCurrentSubTripStatus(safeGetString(doc, "current_sub_trip_status"))
            .setSubTripIndex(safeGetInt(doc, "sub_trip_index"));

        List<Document> stopsDataRaw = safeGetList(doc, "stops_data", Document.class);
        if (stopsDataRaw != null) {
            builder.setStopsData(stopsDataRaw.stream()
                .map(TripArrayBuilders::buildStopData)
                .collect(Collectors.toList()));
        }

        return builder.build();
    }

    public static Surge buildSurge(Document doc) {
        if (doc == null) return null;
        return Surge.newBuilder()
            .setAmount(safeGetDouble(doc, "amount"))
            .setFlag(safeGetBoolean(doc, "flag"))
            .setFactor(safeGetDouble(doc, "factor"))
            .build();
    }

    public static SurgePrivate buildSurgePrivate(Document doc) {
        if (doc == null) return null;
        SurgePrivate.Builder builder = SurgePrivate.newBuilder()
            .setAmount(safeGetDouble(doc, "amount"))
            .setFlag(safeGetBoolean(doc, "flag"))
            .setRate(safeGetDouble(doc, "rate"));

        Document ruleDoc = safeGetDocument(doc, "rule");
        if (ruleDoc != null) {
            builder.setRule(buildSurgeRule(ruleDoc));
        }

        return builder.build();
    }

    private static SurgeRule buildSurgeRule(Document doc) {
        if (doc == null) return null;
        return SurgeRule.newBuilder()
            .setSurgeRuleId(safeGetString(doc, "_id"))
            .setVersion(safeGetString(doc, "version"))
            .build();
    }

    public static TripExtension buildTripExtension(Document doc) {
        if (doc == null) return null;
        return TripExtension.newBuilder()
            .setRiderRequestExtension(safeGetBoolean(doc, "riderRequestExtension"))
            .setEstimatedCostBeforeExtension(safeGetString(doc, "estimatedCostBeforeExtension"))
            .setOriginalEstimatedCostBeforeExtension(safeGetString(doc, "originalEstimatedCostBeforeExtension"))
            .setEnableTripExtension(safeGetBoolean(doc, "enableTripExtension"))
            .build();
    }
}