package org.pipeline.trips;



import static org.pipeline.trips.MongoAvroUtils.*;

import org.bson.Document;

import org.pipeline.trips.avro.*;

import java.util.List;

import java.util.stream.Collectors;

import java.util.ArrayList;



public class TripAvroBuilderUtils {



    

    public static Label buildLabel(Document labelDoc) {

        if (labelDoc == null) return null;

        return Label.newBuilder()

                .setAr(safeGetString(labelDoc, "ar"))

                .setEn(safeGetString(labelDoc, "en"))

                .setFr(safeGetString(labelDoc, "fr"))

                .build();

    }



    public static AppliedTax buildAppliedTax(Document taxDoc) {

        if (taxDoc == null) return null;

        return AppliedTax.newBuilder()

                .setType(safeGetString(taxDoc, "type"))

                .setLabel(buildLabel(safeGetDocument(taxDoc, "label")))

                .setCode(safeGetString(taxDoc, "code"))

                .setAmount(safeGetDouble(taxDoc, "amount"))

                .setReversableAmount(safeGetInt(taxDoc, "reversableAmount"))

                .setOnYassirBenefit(safeGetInt(taxDoc, "onYassirBenefit"))

                .setTaxId(safeGetString(taxDoc, "_id"))

                .build();

    }



    public static Location buildLocation(Document locDoc) {

        if (locDoc == null) return null;

        List<Double> coords = null;

        List<?> coordsRaw = safeGetList(locDoc, "coordinates", Object.class);

        if (coordsRaw != null) {

            coords = coordsRaw.stream()

                .filter(c -> c instanceof Number)

                .map(c -> ((Number) c).doubleValue())

                .collect(Collectors.toList());

        }

        return Location.newBuilder()

                .setType(safeGetString(locDoc, "type"))

                .setCoordinates(coords)

                .setFormattedAddress(safeGetString(locDoc, "formatted_address"))

                .build();

    }



    public static GeoHashH3 buildGeoHashH3(Document geoDoc) {

        if (geoDoc == null) return null;

        return GeoHashH3.newBuilder()

            .setLevel4(safeGetString(geoDoc, "4"))

            .setLevel5(safeGetString(geoDoc, "5"))

            .setLevel6(safeGetString(geoDoc, "6"))

            .setLevel7(safeGetString(geoDoc, "7"))

            .setLevel8(safeGetString(geoDoc, "8"))

            .setLevel9(safeGetString(geoDoc, "9"))

            .build();

    }



    public static LocationDetails buildLocationDetails(Document locDetDoc) {

        if (locDetDoc == null) return null;

        return LocationDetails.newBuilder()

            .setGeohashsH3(buildGeoHashH3(safeGetDocument(locDetDoc, "geohashs_h3")))

            .setLat(safeGetString(locDetDoc, "lat"))

            .setLng(safeGetString(locDetDoc, "lng"))

            .setLvl0Id(safeGetString(locDetDoc, "lvl0_id"))

            .setLvl0Label(safeGetString(locDetDoc, "lvl0_label"))

            .setLvl1Id(safeGetString(locDetDoc, "lvl1_id"))

            .setLvl1Label(safeGetString(locDetDoc, "lvl1_label"))

            .setLvl2Id(safeGetString(locDetDoc, "lvl2_id"))

            .setLvl2Label(safeGetString(locDetDoc, "lvl2_label"))

            .setLvl3Id(safeGetString(locDetDoc, "lvl3_id"))

            .setLvl3Label(safeGetString(locDetDoc, "lvl3_label"))

            .setMapAddress(safeGetString(locDetDoc, "map_address"))

            .setMapPath(safeGetString(locDetDoc, "map_path"))

            .setZoneCode(safeGetString(locDetDoc, "zone_code"))

            .build();

    }



    public static Tarifs buildTarifs(Document tarifsDoc) {

        if (tarifsDoc == null) return null;

        return Tarifs.newBuilder()

                .setMinPrice(safeGetInt(tarifsDoc, "min_price"))

                .setBasePrice(safeGetDouble(tarifsDoc, "base_price"))

                .setMinutePrice(safeGetDouble(tarifsDoc, "minute_price"))

                .setKmPrice(safeGetDouble(tarifsDoc, "km_price"))

                .build();

    }



    public static TarifsSimple buildTarifsSimple(Document tarifsDoc) {

        if (tarifsDoc == null) return null;

        return TarifsSimple.newBuilder()

                .setMinPrice(safeGetInt(tarifsDoc, "min_price"))

                .setBasePrice(safeGetInt(tarifsDoc, "base_price"))

                .setMinutePrice(safeGetInt(tarifsDoc, "minute_price"))

                .setKmPrice(safeGetInt(tarifsDoc, "km_price"))

                .build();

    }



    public static CancelReason buildCancelReason(Document reasonDoc) {

         if (reasonDoc == null) return null;

         return CancelReason.newBuilder()

                 .setComment(safeGetString(reasonDoc, "comment"))

                 .setReason(safeGetString(reasonDoc, "reason"))

                 .setReasonId(safeGetString(reasonDoc, "reasonId"))

                 .build();

    }



    public static DeviceLocation buildDeviceLocation(Document locDoc) {

         if (locDoc == null) return null;

         List<String> coords = safeGetList(locDoc, "coordinates", String.class);

         return DeviceLocation.newBuilder().setCoordinates(coords != null ? new ArrayList<>(coords): null).build();

     }



    public static DeviceProfile buildDeviceProfile(Document devProfileDoc) {

         if (devProfileDoc == null) return null;

         return DeviceProfile.newBuilder()

                 .setActive(safeGetBoolean(devProfileDoc, "active"))

                 .setAppLang(safeGetString(devProfileDoc, "app_lang"))

                 .setAppVersion(safeGetString(devProfileDoc, "app_version"))

                 .setBrand(safeGetString(devProfileDoc, "brand"))

                 .setCreatedAt(safeGetInstant(devProfileDoc, "created_at"))

                 .setDeviceId(safeGetString(devProfileDoc, "device_id"))

                 .setDeviceLang(safeGetString(devProfileDoc, "device_lang"))

                 .setFcmToken(safeGetString(devProfileDoc, "fcm_token"))

                 .setLastUsed(safeGetInstant(devProfileDoc, "last_used"))

                 .setLocation(buildDeviceLocation(safeGetDocument(devProfileDoc, "location")))

                 .setModel(safeGetString(devProfileDoc, "model"))

                 .setOsVersion(safeGetString(devProfileDoc, "os_version"))

                 .setPlatform(safeGetString(devProfileDoc, "platform"))

                 .setRetiredAt(safeGetString(devProfileDoc, "retired_at"))

                 .setYaDeviceId(safeGetString(devProfileDoc, "ya_device_id"))

                 .build();

    }



    public static DriverLocation buildDriverLocation(Document locDoc){

        if (locDoc == null) return null;

        return DriverLocation.newBuilder()

                .setLat(safeGetDouble(locDoc, "lat"))

                .setLng(safeGetDouble(locDoc, "lng"))

                .build();

    }



    public static Review buildReview(Document reviewDoc) {

         if (reviewDoc == null) return null;

         return Review.newBuilder()

                 .setComment(safeGetString(reviewDoc, "comment"))

                 .setRating(safeGetInt(reviewDoc, "rating"))

                 .build();

     }



    public static EpayStatus buildEpayStatus(Document statusDoc) {

         if (statusDoc == null) return null;

         return EpayStatus.newBuilder()

                 .setCode(safeGetInt(statusDoc, "code"))

                 .setCreatedAt(safeGetInstant(statusDoc, "created_at"))

                 .setCurrent(safeGetString(statusDoc, "current"))

                 .setNote(safeGetString(statusDoc, "note"))

                 .setUpdatedAt(safeGetInstant(statusDoc, "updated_at"))

                 .build();

     }



    public static Epay buildEpay(Document epayDoc){

        if (epayDoc == null) return null;

        return Epay.newBuilder()

                .setAmount(safeGetInt(epayDoc, "amount"))

                .setMethod(safeGetString(epayDoc, "method"))

                .setOrderId(safeGetString(epayDoc, "order_id"))

                .setSecondPaymentMethod(safeGetString(epayDoc, "second_payment_method"))

                .setStatus(buildEpayStatus(safeGetDocument(epayDoc, "status")))

                .setType(safeGetString(epayDoc, "type"))

                .build();

     }



    public static Tax buildTax(Document taxDoc){

         if (taxDoc == null) return null;

          Tax.Builder builder = Tax.newBuilder()

              .setTaxId(safeGetString(taxDoc, "_id"))

              .setCode(safeGetString(taxDoc, "code"))

              .setCountry(safeGetString(taxDoc, "country"))

              .setLabel(buildLabel(safeGetDocument(taxDoc, "label")))

              .setLevelPath(safeGetString(taxDoc, "level_path"))

              .setMaxTaxValue(safeGetDouble(taxDoc, "max_tax_value"))

              .setMaxValue(safeGetDouble(taxDoc, "max_value"))

              .setMinTaxValue(safeGetInt(taxDoc, "min_tax_value"))

              .setMinValue(safeGetInt(taxDoc, "min_value"))

              .setOrder(safeGetInt(taxDoc, "order"))

              .setPercent(safeGetDouble(taxDoc, "percent"))

              .setReverse(safeGetInt(taxDoc, "reverse"))

              .setService(safeGetString(taxDoc, "service"))

              .setServicetype(safeGetString(taxDoc, "servicetype"))

              .setSt(safeGetString(taxDoc, "st"))

              .setType(safeGetString(taxDoc, "type"))

              .setValue(safeGetString(taxDoc, "value"))

              .setValueType(safeGetString(taxDoc, "value_type"));



          List<String> paymentTypeList = safeGetList(taxDoc, "paymentType", String.class);

          if (paymentTypeList != null) {

              builder.setPaymentType(new ArrayList<>(paymentTypeList));

          }



          List<String> tripCatList = safeGetList(taxDoc, "tripCat", String.class);

          if (tripCatList != null) {

              builder.setTripCat(new ArrayList<>(tripCatList));

          }



          List<String> tripTypeList = safeGetList(taxDoc, "tripType", String.class);

          if (tripTypeList != null) {

              builder.setTripType(new ArrayList<>(tripTypeList));

          }



          List<String> visibilityInvoiceList = safeGetList(taxDoc, "visibility_invoice", String.class);

          if (visibilityInvoiceList != null) {

              builder.setVisibilityInvoice(new ArrayList<>(visibilityInvoiceList));

          }



          List<String> visibilityUiList = safeGetList(taxDoc, "visibility_ui", String.class);

          if (visibilityUiList != null) {

              builder.setVisibilityUi(new ArrayList<>(visibilityUiList));

          }



          return builder.build();

     }



    public static TipEpayStatus buildTipEpayStatus(Document statusDoc) {

        if (statusDoc == null) return null;

        return TipEpayStatus.newBuilder()

                .setCurrent(safeGetString(statusDoc, "current"))

                .setCode(safeGetInt(statusDoc, "code"))

                .setNote(safeGetString(statusDoc, "note"))

                .setCreatedAt(safeGetString(statusDoc, "created_at"))

                .setUpdatedAt(safeGetString(statusDoc, "updated_at"))

                .build();

    }



    public static TipEpay buildTipEpay(Document epayDoc){

        if(epayDoc == null) return null;

        return TipEpay.newBuilder()

                .setOrderId(safeGetString(epayDoc, "order_id"))

                .setStatus(buildTipEpayStatus(safeGetDocument(epayDoc, "status")))

                .setType(safeGetString(epayDoc, "type"))

                .build();

    }



    public static Taxe buildTaxe(Document taxeDoc){

        if(taxeDoc == null) return null;

        List<String> details = safeGetList(taxeDoc, "details", String.class);

        return Taxe.newBuilder()

                .setTotal(safeGetInt(taxeDoc, "total"))

                .setDetails(details != null ? new ArrayList<>(details) : null)

                .build();

    }

    

    public static StopLocation buildStopLocation(Document stopLocDoc){

        if(stopLocDoc == null) return null;

        return StopLocation.newBuilder()

            .setLocation(buildDriverLocation(safeGetDocument(stopLocDoc, "location")))

            .setFormattedAddress(safeGetString(stopLocDoc, "formatted_address"))

            .build();

    }

}
