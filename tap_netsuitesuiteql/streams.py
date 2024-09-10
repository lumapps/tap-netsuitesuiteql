"""Stream type classes for tap-netsuitesuiteql."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_netsuitesuiteql.client import NetsuiteSuiteQLStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources


class ArrHistoryStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "arr_history"
    path = ""
    primary_keys = ["unique_key"]
    query = "SELECT TL.uniqueKey unique_key, T.id id, T.trandate arr_date, TL.netamount, T.currency arr_currency, AT.name arr_type, CE.id enduser_id, CE.custentity_lum_cus_sfid enduser_sfid, LT.tranid so_tranid, LTC.symbol so_currency, LT.exchangerate so_exchange_rate, CR.custentity_lum_cus_sfid reseller_sfid, CR.companyname reseller_name, CP.custentity_lum_cus_sfid partner_sfid, CP.companyname partner_name, S.name subsidiary_name, SC.symbol subsidiary_currency, CT.name contract_name, CT.id contract_id, CEP.custentity_lum_cus_sfid enduser_parent_sfid, CEGP.custentity_lum_cus_sfid enduser_grandparent_sfid, S.custrecord_lum_fixedfxusd_sub subsidiary_exchange_rate, RR.name report_region FROM transactionline TL  LEFT JOIN transaction T ON TL.transaction = T.id  LEFT JOIN customlist_lum_arrh_arrtype AT ON AT.id = T.custbody_lum_arrh_arrtype  LEFT JOIN customer CE ON CE.id = T.custbody_lum_arrh_enduser LEFT JOIN customrecord_prq_contract CT ON CT.id = T.custbody_lum_arrh_contract LEFT JOIN customer CR ON CR.id = T.custbody_lum_arrh_reseller LEFT JOIN customer CP ON CP.id = T.custbody_lum_arrh_partner LEFT JOIN subsidiary S ON S.id = TL.subsidiary LEFT JOIN transaction LT ON LT.id = T.custbody_lum_arrh_transaction LEFT JOIN customer CEP ON CEP.id = CE.parent LEFT JOIN customer CEGP ON CEGP.id = CEP.parent LEFT JOIN CUSTOMLIST_LUM_ARRH_REGION RR ON RR.id=T. custbody_lum_arrh_region LEFT JOIN currency LTC ON LTC.id=LT.currency LEFT JOIN currency SC ON SC.id=S.currency WHERE AT.name IN ('ARR', 'cARR') AND T.recordtype = 'customtransaction_lum_arrh'  AND TL.mainline = 'F'"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("unique_key", th.IntegerType),
        th.Property("id", th.IntegerType),
        th.Property("arr_date", th.DateType),
        th.Property("netamount", th.NumberType),
        th.Property("arr_currency", th.IntegerType),
        th.Property("arr_type", th.StringType),
        th.Property("enduser_id", th.IntegerType),
        th.Property("enduser_sfid", th.StringType),
        th.Property("so_tranid", th.StringType),
        th.Property("so_currency", th.StringType),
        th.Property("so_exchange_rate", th.NumberType),
        th.Property("reseller_sfid", th.StringType),
        th.Property("reseller_name", th.StringType),
        th.Property("partner_sfid", th.StringType),
        th.Property("partner_name", th.StringType),
        th.Property("subsidiary_name", th.StringType),
        th.Property("subsidiary_currency", th.StringType),
        th.Property("contract_name", th.StringType),
        th.Property("contract_id", th.IntegerType),
        th.Property("enduser_parent_sfid", th.StringType),
        th.Property("enduser_grandparent_sfid", th.StringType),
        th.Property("subsidiary_exchange_rate", th.NumberType),
        th.Property("report_region", th.StringType),

    ).to_dict()


class EndusersStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "endusers"
    path = ""
    primary_keys = ["id"]
    query = "SELECT C.id, C.custentity_lum_cus_sfid enduser_sfid, C.companyName as companyname, CY.id as country_code, CY.name as country, state.shortname state_shortname, state.fullname state_fullname, C.firstOrderDate as first_order_date, S.id as subsidiary_id, S.name as subsidiary_name  FROM customer C  LEFT JOIN customerSubsidiaryRelationship SR ON SR.entity=C.id  LEFT JOIN Subsidiary S ON S.id=SR.subsidiary  LEFT JOIN EntityAddress ADD ON C. defaultBillingAddress=ADD. nKey LEFT JOIN Country CY ON ADD.country=CY.id LEFT JOIN state ON ADD.country = state.country AND ADD.dropdownstate = state.shortname WHERE C.custentity_prq_end_user='T'"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("enduser_sfid", th.StringType),
        th.Property("companyname", th.StringType),
        th.Property("country_code", th.StringType),
        th.Property("country", th.StringType),
        th.Property("state_shortname", th.StringType),
        th.Property("state_fullname", th.StringType),
        th.Property("subsidiary_id", th.IntegerType),
        th.Property("subsidiary_name", th.StringType),
        th.Property("first_order_date", th.DateType),

    ).to_dict()

class GeographicalHierarchyStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "geography"
    path = ""
    primary_keys = ["id"]
    query = "SELECT R.recordid as id, R.name, RT.name as type, R.parent as parent FROM CUSTOMRECORD_LUM_REGION_CORPORATE R LEFT JOIN CUSTOMLIST_LUM_GEOGRAPHY_TYPE RT ON RT.id = R.custrecord_lum_region_corporate_type WHERE R.isinactive='F'"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("parent", th.IntegerType),

    ).to_dict()

class LicensesCountStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "licenses_count"
    path = ""
    primary_keys = ["id"]
    query = "SELECT customrecord_lum_licenses_count.id id, custrecord_lum_licenses_count_date count_date, LT.name as license_count_type, custrecord_lum_licenses_count_enduser enduser_id, custrecord_lum_licenses_count_licenses licenses FROM customrecord_lum_licenses_count LEFT JOIN CUSTOMLIST_LUM_LICENSES_COUNT_TYPE LT ON customrecord_lum_licenses_count.custrecord_lum_licenses_count_type = LT.id WHERE LT.name='Core Package' ORDER BY custrecord_lum_licenses_count_date DESC,  custrecord_lum_licenses_count_type, custrecord_lum_licenses_count_enduser"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("countDate", th.DateType),
        th.Property("license_count_type", th.StringType),
        th.Property("enduser_id", th.IntegerType),
        th.Property("licenses", th.IntegerType),

    ).to_dict()


class TransactionLineStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "transaction_line"
    path = ""
    primary_keys = ["unique_key"]
    query = "SELECT TL.uniqueKey unique_key, T.id, BUILTIN.DF(T.type) type, BUILTIN.DF(T.status) status, BUILTIN.DF(T.custbody_prq_approval_custom_status) approvalStatus, T.tranId, T.tranDate, T.custbody_prq_end_user endUserId, BUILTIN.DF(T.custbody_prq_end_user) endUser, T.entity billToCustomerId, BUILTIN.DF(T.entity) billToCustomer, T.custbody_prq_contract contractId, BUILTIN.DF(T.custbody_prq_contract) contract, BUILTIN.DF(C.custrecord_prq_ct_partner) partner, BUILTIN.DF(T.custbody_prq_contract_type) contractType, BUILTIN.DF(C.custrecord_prq_ct_geographical_area) contractArea, BUILTIN.DF(T.currency) currency, T.exchangeRate, BUILTIN.DF(TL.subsidiary) subsidiary, S.custrecord_lum_fixedfxusd_sub fixedExchangeRateUsd, T.startDate tranStartDate, T.endDate tranEndDate, T.custbody_prq_months tranMonths, BUILTIN.DF(T.cseg_prq_revenue) revenue, BUILTIN.DF(TL.location) location, T.custbody_prq_carr carr, T.custbody_lum_incrementalcarr incrementalCarr, T.custbody_prq_carr_start_date carrStartDate, T.custbody_prq_carr_end_date carrEndDate, BUILTIN.DF(T.custbody_prq_product) mainProduct, BUILTIN.DF(T.custbody_prq_customer_success_manager) csm, BUILTIN.DF(T.employee) salesRep, T.custbody_prq_renewal renewal, T.custbody_prq_renewal_start_date renewalStartDate, T.custbody_prq_renewal_end_date renewalEndDate, T.custbody_prq_renewal_months renewalMonths, T.custbody_prq_renewed renewed, BUILTIN.DF(T.custbody_prq_renewal_linked_trans) renewalEstimate, TL.id lineId, TL.custcol_prq_billing_so_line line, BUILTIN.DF(TL.cseg_prq_revenue) lineRevenue, TL.custcol_lum_hubspot_line hubspotLine, TL.item itemId, BUILTIN.DF(TL.item) item, BUILTIN.DF(I.custitem_lum_productfamily_ma) productFamily, TL.custcol_prq_users users, TL.quantity amountExcludingTax, TL.rate quantity, TL.netAmount, TL.memo description, TL.isclosed, TL.custcol_prq_start_date lineStartDate, TL.custcol_prq_end_date lineEndDate, TL.custcol_prq_months lineMonths, TL.custcol_prq_renewal lineRenewal, TL.custcol_prq_arr_calculation arrCalc, BUILTIN.DF(TL.custcolcustcol_typeheyaxel) typeHeyAxel, T.createdDate, TL.custcol_prq_billing_period_from billingPeriodFrom, TL.custcol_prq_billing_period_to billingPeriodTo FROM transaction T LEFT JOIN transactionline TL ON TL.transaction = T.id LEFT JOIN customer CE ON CE.id = T.custbody_prq_end_user LEFT JOIN item I ON I.id = TL.item LEFT JOIN subsidiary S ON S.id = TL.subsidiary LEFT JOIN customrecord_prq_contract C ON C.id = T.custbody_prq_contract WHERE BUILTIN.DF(T.type) IN ('Estimate','Sales Order') AND TL.mainLine = 'F' AND TL.taxLine = 'F'"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("unique_key", th.IntegerType),
        th.Property("id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("approvalstatus", th.StringType),
        th.Property("tranid", th.StringType),
        th.Property("trandate", th.DateType),
        th.Property("enduserid", th.IntegerType),
        th.Property("enduser", th.StringType),
        th.Property("billtocustomerid", th.IntegerType),
        th.Property("billtocustomer", th.StringType),
        th.Property("contractid", th.IntegerType),
        th.Property("contract", th.StringType),
        th.Property("partner", th.StringType),
        th.Property("contracttype", th.StringType),
        th.Property("contractarea", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("exchangerate", th.NumberType),
        th.Property("subsidiary", th.StringType),
        th.Property("fixedexchangerateusd", th.NumberType),
        th.Property("transtartdate", th.DateType),
        th.Property("tranenddate", th.DateType),
        th.Property("tranmonths", th.NumberType),
        th.Property("revenue", th.StringType),
        th.Property("location", th.StringType),
        th.Property("carr", th.NumberType),
        th.Property("incrementalcarr", th.NumberType),
        th.Property("carrstartdate", th.DateType),
        th.Property("carrenddate", th.DateType),
        th.Property("mainproduct", th.StringType),
        th.Property("csm", th.StringType),
        th.Property("salesrep", th.StringType),
        th.Property("renewal", th.StringType),
        th.Property("renewalstartdate", th.DateType),
        th.Property("renewalenddate", th.DateType),
        th.Property("renewalmonths", th.NumberType),
        th.Property("renewed", th.StringType),
        th.Property("renewalestimate", th.StringType),
        th.Property("lineid", th.IntegerType),
        th.Property("line", th.StringType),
        th.Property("linerevenue", th.StringType),
        th.Property("hubspotline", th.StringType),
        th.Property("itemid", th.IntegerType),
        th.Property("item", th.StringType),
        th.Property("productfamily", th.StringType),
        th.Property("users", th.IntegerType),
        th.Property("amountexcludingtax", th.NumberType),
        th.Property("quantity", th.NumberType),
        th.Property("netamount", th.NumberType),
        th.Property("description", th.StringType),
        th.Property("isclosed", th.StringType),
        th.Property("linestartdate", th.DateType),
        th.Property("lineenddate", th.DateType),
        th.Property("linemonths", th.NumberType),
        th.Property("linerenewal", th.StringType),
        th.Property("arrcalc", th.StringType),
        th.Property("typeheyaxel", th.StringType),
        th.Property("createddate", th.DateType),
        th.Property("billingperiodfrom", th.DateType),
        th.Property("billingperiodto", th.DateType),
    ).to_dict()
