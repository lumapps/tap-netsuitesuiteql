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

from datetime import date, datetime

class ArrHistoryStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "arr_history"
    path = ""
    primary_keys = ["unique_key"]
    replication_key="last_modified_date"
    is_sorted=True
    start_date=datetime.fromisoformat("2010-01-01 00:00:00")

    query = """SELECT TL.uniqueKey as unique_key, 
        T.id as id, 
        to_char(T.trandate, 'dd/MM/YYYY') as arr_date, 
        TL.netamount as netamount, 
        T.currency as arr_currency, 
        AT.name as arr_type, 
        CE.id as enduser_id, 
        CE.custentity_lum_cus_sfid as enduser_sfid, 
        LT.tranid as so_tranid, 
        LTC.symbol as so_currency, 
        LT.exchangerate as so_exchange_rate, 
        CR.custentity_lum_cus_sfid as reseller_sfid, 
        CR.companyname as reseller_name, 
        CP.custentity_lum_cus_sfid as partner_sfid, 
        CP.companyname as partner_name, 
        S.name as subsidiary_name, 
        SC.symbol as subsidiary_currency, 
        CT.name as contract_name, 
        CT.id as contract_id, 
        CEP.id as enduser_parent_id, 
        CEP.custentity_lum_cus_sfid as enduser_parent_sfid, 
        CEGP.custentity_lum_cus_sfid as enduser_grandparent_sfid, 
        S.custrecord_lum_fixedfxusd_sub as subsidiary_exchange_rate, 
        RR.name as report_region,

        to_char(GREATEST(
            coalesce(T.LastModifiedDate, T.createdDateTime), 
            coalesce(TL.lineLastModifiedDate, TL.lineCreatedDate)
        ), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

        FROM transactionline TL  
        LEFT JOIN transaction T ON TL.transaction = T.id  
        LEFT JOIN customlist_lum_arrh_arrtype AT ON AT.id = T.custbody_lum_arrh_arrtype  
        LEFT JOIN customer CE ON CE.id = T.custbody_lum_arrh_enduser 
        LEFT JOIN customrecord_prq_contract CT ON CT.id = T.custbody_lum_arrh_contract 
        LEFT JOIN customer CR ON CR.id = T.custbody_lum_arrh_reseller 
        LEFT JOIN customer CP ON CP.id = T.custbody_lum_arrh_partner 
        LEFT JOIN subsidiary S ON S.id = TL.subsidiary 
        LEFT JOIN transaction LT ON LT.id = T.custbody_lum_arrh_transaction 
        LEFT JOIN customer CEP ON CEP.id = CE.parent 
        LEFT JOIN customer CEGP ON CEGP.id = CEP.parent 
        LEFT JOIN CUSTOMLIST_LUM_ARRH_REGION RR ON RR.id=T. custbody_lum_arrh_region 
        LEFT JOIN currency LTC ON LTC.id=LT.currency 
        LEFT JOIN currency SC ON SC.id=S.currency 
        WHERE AT.name IN ('ARR', 'cARR') 
        AND T.recordtype = 'customtransaction_lum_arrh'  
        AND TL.mainline = 'F' 
        AND (
            T.LastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
            OR TL.lineLastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ) 

        ORDER BY last_modified_date, unique_key
        """

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
        th.Property("enduser_parent_id", th.IntegerType),
        th.Property("enduser_parent_sfid", th.StringType),
        th.Property("enduser_grandparent_sfid", th.StringType),
        th.Property("subsidiary_exchange_rate", th.NumberType),
        th.Property("report_region", th.StringType),
        th.Property("last_modified_date", th.DateTimeType),

    ).to_dict()


class EndusersStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "endusers"
    path = ""
    primary_keys = ["id"]

    replication_key="last_modified_date"
    is_sorted=True
    start_date=datetime.fromisoformat("2010-01-01 00:00:00")

    query = """SELECT C.id as id, 
        C.custentity_lum_cus_sfid as enduser_sfid, 
        C.companyName as companyname, 
        CY.id as country_code, 
        CY.name as country, 
        state.shortname as state_shortname, 
        state.fullname as state_fullname, 
        to_char(C.firstOrderDate, 'dd/MM/YYYY') as first_order_date, 
        S.id as subsidiary_id, 
        S.name as subsidiary_name, 
        CEP.id as enduser_parent_id, 
        CEP.companyname as enduser_parent_name, 
        to_char(CEP.firstOrderDate, 'dd/MM/YYYY') as parent_first_order_date, 
        C.custentity_prq_end_user as is_enduser, 
        C.custentity_prq_partner as is_partner, 
        C.custentity_prq_reseller as is_reseller, 
        AO.email as salesrep_email, 
        CSM.email as csm_email,
        to_char(coalesce(C.lastModifiedDate, C.dateCreated), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

        FROM customer C  
        LEFT JOIN customer CEP ON CEP.id = C.parent  
        LEFT JOIN customerSubsidiaryRelationship SR ON SR.entity=C.id  
        LEFT JOIN Subsidiary S ON S.id=SR.subsidiary   
        LEFT JOIN EntityAddress ADD ON C. defaultBillingAddress=ADD. nKey  
        LEFT JOIN Country CY ON ADD.country=CY.id  
        LEFT JOIN state ON ADD.country = state.country AND ADD.dropdownstate = state.shortname  
        LEFT JOIN employee AO ON AO.id = C.salesrep 
        LEFT JOIN employee CSM ON CSM.id = C.custentity_prq_customer_success_manager 
        WHERE (C.custentity_prq_end_user='T' OR C.custentity_prq_partner='T' OR C.custentity_prq_reseller='T') 
        AND (
            C.lastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
            OR C.dateCreated>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ) 
        ORDER BY last_modified_date, id
        """

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("enduser_sfid", th.StringType),
        th.Property("companyname", th.StringType),
        th.Property("country_code", th.StringType),
        th.Property("country", th.StringType),
        th.Property("state_shortname", th.StringType),
        th.Property("state_fullname", th.StringType),
        th.Property("enduser_parent_id", th.IntegerType),
        th.Property("enduser_parent_name", th.StringType),
        th.Property("subsidiary_id", th.IntegerType),
        th.Property("subsidiary_name", th.StringType),
        th.Property("first_order_date", th.DateType),
        th.Property("parent_first_order_date", th.DateType),
        th.Property("is_enduser", th.StringType),
        th.Property("is_partner", th.StringType),
        th.Property("is_reseller", th.StringType),
        th.Property("salesrep_email", th.StringType),
        th.Property("csm_email", th.StringType),
        th.Property("last_modified_date", th.DateTimeType),

    ).to_dict()

class GeographicalHierarchyStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "geography"
    path = ""
    primary_keys = ["id"]
    replication_key = "last_modified_date"
    start_date=datetime.fromisoformat("2010-01-01 00:00:00")

    query = """SELECT 
        R.recordid as id, 
        CY.id as country_code,
        R.name as name, 
        RT.name as type, 
        R.parent as parent,
        to_char(GREATEST(
            COALESCE(R.lastmodified, R.created),
            COALESCE(RT.lastmodified, RT.created)
        ), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

    FROM CUSTOMRECORD_LUM_REGION_CORPORATE R 
    LEFT JOIN CUSTOMLIST_LUM_GEOGRAPHY_TYPE RT ON RT.id = R.custrecord_lum_region_corporate_type 
    LEFT JOIN Country CY ON CY.name=R.name
    WHERE R.isinactive='F'
    AND R. custrecord_lum_region_corporate_type IS NOT NULL
    AND (
        R.lastmodified>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        OR R.created>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        OR RT.lastmodified>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        OR RT.created>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
    ) 
    ORDER BY last_modified_date, id
    """


    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("country_code", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("parent", th.IntegerType),
        th.Property("last_modified_date", th.DateTimeType),

    ).to_dict()

class LicensesCountStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "licenses_count"
    path = ""
    primary_keys = ["id"]
    replication_key="last_modified_date"
    is_sorted=True
    start_date=datetime.fromisoformat("2010-01-01 00:00:00")

    query = """
        SELECT C.id as id, 
        to_char(custrecord_lum_licenses_count_date, 'dd/MM/YYYY') as count_date, 
        LT.name as license_count_type, 
        C.custrecord_lum_licenses_count_enduser as enduser_id, 
        C.custrecord_lum_licenses_count_licenses as licenses,
        to_char(coalesce(C.lastmodified, C.created), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

        FROM customrecord_lum_licenses_count C
        LEFT JOIN CUSTOMLIST_LUM_LICENSES_COUNT_TYPE LT ON C.custrecord_lum_licenses_count_type = LT.id 
        WHERE (
            C.lastmodified>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
            OR C.created>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ) 
        ORDER BY last_modified_date, id
        """

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("count_date", th.DateType),
        th.Property("license_count_type", th.StringType),
        th.Property("enduser_id", th.IntegerType),
        th.Property("licenses", th.IntegerType),
        th.Property("last_modified_date", th.DateTimeType),

    ).to_dict()


class SalesOrdersStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "sales_orders"
    path = ""
    primary_keys = ["unique_key"]

    replication_key="last_modified_date"
    is_sorted=True
    start_date=datetime.fromisoformat("2020-01-01 00:00:00")

    query = """SELECT 
        TL.uniqueKey as unique_key, 
        T.entityStatus as entity_status, 
        T.id as id, 
        T.type as tran_type, 
        PREV.previousDoc as previous_estimate, 
        to_char(NEXTTRAN.trandate, 'dd/MM/YYYY') as next_tran_date, 
        TS.fullName as tran_status, 
        TCS.name as approval_status, 
        T.tranId as tran_id, 
        to_char(T.tranDate, 'dd/MM/YYYY') as tran_date,
        T.custbody_prq_end_user as enduser_id,
        T.entity as bill_to_customer_id,
        T.custbody_prq_contract as contract_id,
        C.custrecord_prq_ct_partner as partner_id,
        CT.name as contract_type,
        CAREA.name as contract_area,
        TC.symbol as currency,
        T.exchangeRate as exchange_rate,
        TL.subsidiary as subsidiary,
        S.name as subsidiary_name,
        SC.symbol as subsidiary_currency,
        S.custrecord_lum_fixedfxusd_sub as fixed_exchange_rate_usd,
        S.custrecord_lum_fixedfxusd19_sub as fixed_exchange_rate_usd19,
        to_char(T.startDate, 'dd/MM/YYYY') as tran_start_date,
        to_char(T.endDate, 'dd/MM/YYYY') as tran_end_date, 
        T.custbody_prq_months as tran_months, 
        TREV.name as revenue, 
        TLLOC.name as location,
        T.custbody_prq_carr as carr,
        T.custbody_lum_incrementalcarr as incremental_carr,
        to_char(T.custbody_prq_carr_start_date, 'dd/MM/YYYY') as carr_start_date,
        to_char(T.custbody_prq_carr_end_date, 'dd/MM/YYYY') as carr_end_date,
        T.custbody_prq_product as main_item_id,
        TPRODUCT.fullName as main_item_name,
        TPRODUCT.displayName as main_item_display_name,
        T.custbody_prq_customer_success_manager as csm, 
        T.employee as sales_rep,
        SR.email as sales_rep_email,
        SR.firstname as sales_rep_firstname,
        SR.lastname as sales_rep_lastname,
        T.custbody_prq_renewal as renewal,
        to_char(T.custbody_prq_renewal_start_date, 'dd/MM/YYYY') as renewal_start_date,
        to_char(T.custbody_prq_renewal_end_date, 'dd/MM/YYYY') as renewal_end_date,
        T.custbody_prq_renewal_months as renewal_months,
        T.custbody_prq_renewed as renewed,
        T.custbody_prq_renewal_linked_trans as renewal_estimate,
        TL.id as line_id,
        TL.custcol_prq_billing_so_line as line,
        TL.cseg_prq_revenue as line_revenue,
        TL.custcol_lum_hubspot_line as hubspot_line,
        TL.item as item_id,
        I.fullName as item_name,
        I.displayName as item_display_name,
        IF.name as product_family,
        TL.custcol_prq_users as users, 
        TL.quantity as amount_excluding_tax, 
        TL.rate as quantity, 
        TL.netAmount as net_amount, 
        TL.memo as description, 
        TL.isclosed as is_closed, 
        to_char(TL.custcol_prq_start_date, 'dd/MM/YYYY') as line_start_date, 
        to_char(TL.custcol_prq_end_date, 'dd/MM/YYYY') as line_end_date,
        TL.custcol_prq_months as line_months,
        TL.custcol_prq_renewal as line_renewal,
        TL.custcol_prq_arr_calculation as arr_calc,
        to_char(T.createdDate, 'dd/MM/YYYY') as created_date,
        to_char(TL.custcol_prq_billing_period_from, 'dd/MM/YYYY') as billing_period_from,
        to_char(TL.custcol_prq_billing_period_to, 'dd/MM/YYYY') as billing_period_to,
        RS.name as renewal_status, 
        to_char(RT.tranDate, 'dd/MM/YYYY') as renewal_tran_date, 
        T.custbody_prq_arr as arr_legacy, 
        RT.tranId as renewal_estimate_tran_id, 
        NEXTTRAN.tranId as next_tran_id,
        SOS.name as source_subsidiary,
        T.custbody_sv_fus_migrated as migrated,
        T.custbody_sv_fus_newtransacid as migrated_transaction_id,

        to_char(GREATEST(
            coalesce(T.LastModifiedDate, T.createdDateTime), 
            coalesce(TL.lineLastModifiedDate, TL.lineCreatedDate)
        ), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

    FROM transaction T   
    LEFT JOIN transactionline TL ON TL.transaction = T.id   
    LEFT JOIN customer CE ON CE.id = T.custbody_prq_end_user   
    LEFT JOIN item I ON I.id = TL.item   
    LEFT JOIN subsidiary S ON S.id = TL.subsidiary   
    LEFT JOIN subsidiary SOS ON SOS.id = T.custbody_sv_fus_sourcesub   
    LEFT JOIN currency SC ON SC.id=S.currency   
    LEFT JOIN customrecord_prq_contract C ON C.id = T.custbody_prq_contract   
    LEFT JOIN transactionStatus TS ON T.status = TS.id AND T.type = TS.trantype AND T.customtype = TS.trancustomtype   
    LEFT JOIN CUSTOMLIST_PRQ_APPROVAL_CUSTOM_STATUS TCS ON T.custbody_prq_approval_custom_status = TCS.id   
    LEFT JOIN CUSTOMLIST_PRQ_CONTRACT_TYPE CT ON T.custbody_prq_contract_type = CT.id   
    LEFT JOIN CUSTOMLIST_PRQ_CONTRACT_GEO_AREA CAREA ON C.custrecord_prq_ct_geographical_area = CAREA.id   
    LEFT JOIN currency TC ON T.currency=TC.id   
    LEFT JOIN CUSTOMRECORD_CSEG_PRQ_REVENUE TREV ON T.cseg_prq_revenue = TREV.id   
    LEFT JOIN Location TLLOC ON TL.location = TLLOC.id   
    LEFT JOIN item TPRODUCT ON T.custbody_prq_product = TPRODUCT.id   
    LEFT JOIN CUSTOMRECORD_LUM_PRODUCTFAMILY_MA IF ON I.custitem_lum_productfamily_ma = IF.id   
    LEFT JOIN PreviousTransactionLink PREV ON PREV.nextdoc=T.id AND PREV.linkType = 'EstInvc'
    LEFT JOIN NextTransactionLink NEXT ON NEXT.previousdoc=T.id AND NEXT.linkType = 'EstInvc'
    LEFT JOIN Transaction NEXTTRAN ON NEXTTRAN.id=NEXT.nextdoc  
    LEFT JOIN CUSTOMLIST_LUM_RENEWAL_STATUS RS ON T.custbody_lum_renewal_status = RS.id   
    LEFT JOIN transaction RT ON T.custbody_prq_renewal_linked_trans = RT.id   
    LEFT JOIN employee SR ON T.employee = SR.id 
    WHERE (T.type='SalesOrd' OR T.type='Estimate') 
        AND TL.mainLine = 'F' 
        AND TL.taxLine = 'F' 
        AND CT.name = 'Subscription' 
        AND (T.custbody_sv_fus_migrated <> 'T' OR T.custbody_sv_fus_migrated IS NULL)
        AND (
            T.LastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
            OR TL.lineLastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ) 

    ORDER BY last_modified_date, unique_key
    """


    schema = th.PropertiesList(
        th.Property("unique_key", th.IntegerType),
        th.Property("id", th.IntegerType),
        th.Property("entity_status", th.IntegerType),
        th.Property("previous_estimate", th.IntegerType),
        th.Property("tran_type", th.StringType),
        th.Property("tran_status", th.StringType),
        th.Property("approval_status", th.StringType),
        th.Property("tran_id", th.StringType),
        th.Property("tran_date", th.DateType),
        th.Property("enduser_id", th.IntegerType),
        th.Property("bill_to_customer_id", th.IntegerType),
        th.Property("contract_id", th.IntegerType),
        th.Property("partner_id", th.IntegerType),
        th.Property("contract_type", th.StringType),
        th.Property("contract_area", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("subsidiary", th.IntegerType),
        th.Property("subsidiary_name", th.StringType),
        th.Property("subsidiary_currency", th.StringType),
        th.Property("fixed_exchange_rate_usd", th.NumberType),
        th.Property("fixed_exchange_rate_usd19", th.NumberType),
        th.Property("tran_start_date", th.DateType),
        th.Property("tran_end_date", th.DateType),
        th.Property("tran_months", th.NumberType),
        th.Property("revenue", th.StringType),
        th.Property("location", th.StringType),
        th.Property("carr", th.NumberType),
        th.Property("incremental_carr", th.NumberType),
        th.Property("carr_start_date", th.DateType),
        th.Property("carr_end_date", th.DateType),
        th.Property("main_item_id", th.IntegerType),
        th.Property("main_item_name", th.StringType),
        th.Property("main_item_display_name", th.StringType),
        th.Property("csm", th.IntegerType),
        th.Property("sales_rep", th.IntegerType),
        th.Property("sales_rep_email", th.StringType),
        th.Property("sales_rep_firstname", th.StringType),
        th.Property("sales_rep_lastname", th.StringType),
        th.Property("renewal", th.StringType),
        th.Property("renewal_start_date", th.DateType),
        th.Property("renewal_end_date", th.DateType),
        th.Property("renewal_months", th.NumberType),
        th.Property("renewed", th.StringType),
        th.Property("renewal_estimate", th.IntegerType),
        th.Property("line_id", th.IntegerType),
        th.Property("line", th.StringType),
        th.Property("line_revenue", th.IntegerType),
        th.Property("hubspot_line", th.StringType),
        th.Property("item_id", th.IntegerType),
        th.Property("item_name", th.StringType),
        th.Property("item_display_name", th.StringType),
        th.Property("product_family", th.StringType),
        th.Property("users", th.IntegerType),
        th.Property("amount_excluding_tax", th.NumberType),
        th.Property("quantity", th.NumberType),
        th.Property("net_amount", th.NumberType),
        th.Property("description", th.StringType),
        th.Property("is_closed", th.StringType),
        th.Property("line_start_date", th.DateType),
        th.Property("line_end_date", th.DateType),
        th.Property("line_months", th.NumberType),
        th.Property("line_renewal", th.StringType),
        th.Property("arr_calc", th.StringType),
        th.Property("created_date", th.DateType),
        th.Property("billing_period_from", th.DateType),
        th.Property("billing_period_to", th.DateType),
        th.Property("renewal_status", th.StringType),
        th.Property("renewal_tran_date", th.DateType),
        th.Property("next_tran_date", th.DateType),
        th.Property("arr_legacy", th.NumberType),
        th.Property("renewal_estimate_tran_id", th.StringType),
        th.Property("next_tran_id", th.StringType),
        th.Property("source_subsidiary", th.StringType),
        th.Property("migrated", th.StringType),
        th.Property("migrated_transaction_id", th.IntegerType),
        th.Property("last_modified_date", th.DateTimeType)

    ).to_dict()


class ArrRestatementsStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "arr_restatements"
    path = ""
    primary_keys = ["id"]

    replication_key="last_modified_date"
    is_sorted=True
    start_date=datetime.fromisoformat("2010-01-01 00:00:00")

    query = """SELECT R.id as id, 
        R.custrecord_prq_arr_amount as arr, 
        to_char(R.custrecord_prq_arr_start_date, 'dd/MM/YYYY') as start_date, 
        to_char(R.custrecord_prq_end_date, 'dd/MM/YYYY') as end_date, 
        R.custrecord_prq_arr_so_est as so_est_id, 
        to_char(R.created, 'dd/MM/YYYY') as created, 
        R.custrecord_lum_arr_amendment as amendment, 
        R.custrecord_lum_arr_users_dnc as ignore_users,
        SOS.name as source_subsidiary,
        R.custrecord_sv_fus_migrated as migrated,
        R.custrecord_sv_fus_newtransacid as migrated_transaction_id,
        to_char(coalesce(R.lastmodified, R.created), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

        FROM customrecord_prq_arr_restatement R 
        LEFT JOIN subsidiary SOS ON SOS.id = R.custrecord_sv_fus_sourcesub 
        WHERE R.isInactive='F' 
        AND (R.custrecord_sv_fus_migrated <> 'T' OR R.custrecord_sv_fus_migrated IS NULL)
        AND (
            R.lastmodified>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
            OR R.created>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ) 
        ORDER BY last_modified_date, id
        """

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("arr", th.NumberType),
        th.Property("start_date", th.DateType),
        th.Property("end_date", th.DateType),
        th.Property("created", th.DateType),
        th.Property("amendment", th.StringType),
        th.Property("ignore_users", th.StringType),
        th.Property("so_est_id", th.NumberType),
        th.Property("source_subsidiary", th.StringType),
        th.Property("migrated", th.StringType),
        th.Property("migrated_transaction_id", th.IntegerType),
        th.Property("last_modified_date", th.DateTimeType),

    ).to_dict()


class RenewalItemsStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "renewal_items"
    path = ""
    primary_keys = ["id"]
    replication_key = "last_modified_date"
    start_date=datetime.fromisoformat("2010-01-01 00:00:00")

    query = """SELECT id as id, 
        itemid as itemid, 
        displayname as displayname,
        to_char(coalesce(lastModifiedDate, createdDate), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

        FROM item 
        WHERE (custitem_prq_renewal='T' OR custitem_prq_arr_calculation='T') 
        AND lastModifiedDate >to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ORDER BY last_modified_date, id
        """

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("itemid", th.StringType),
        th.Property("displayname", th.StringType),
        th.Property("last_modified_date", th.DateTimeType),

    ).to_dict()


class PnlTransactionAccountingLinesStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "pnl_transaction_accounting_lines"
    path = ""
    primary_keys = ["unique_key"]
    replication_key="last_modified_date"
    start_date=datetime.fromisoformat("2025-01-01 00:00:00")
    is_sorted = True

    query = """SELECT 
    TL.uniqueKey as unique_key,
    T.id as id, 
    to_char(T.tranDate, 'dd/MM/YYYY') as tran_date, 
    T.tranId as tran_id, 
    AP.periodName as posting_period_name, 
    to_char(AP.startDate, 'dd/MM/YYYY') as posting_period_date,
    T.type as type_id, 
    TT.custrecord_lum_trantype_typedb as type_db,
    TT.custrecord_lum_trantype_typebi as type_bi,
    T.recordType as subtype,
    TST.custrecord_lum_tranrecordtype_typebi as subtype_db,
    TST.custrecord_lum_tranrecordtype_typebi as subtype_bi,
    TL.id as line_id, 
    S.fullName as subsidiary, 
    D.name as department, 
    D.fullname as department_hierarchy,
    TLL. fullName as location,
    T.memo as memo, 
    TL.memo as line_memo,
    A.acctNumber as account_number, 
    A.fullname as account_name,  
    E.fullName as main_entity,
    LE.entityTitle as line_entity,
    TLI.fullName as line_item,
    LJI.fullName as journal_item,
    TL.netAmount as net_amount_currency, 
    TC.symbol as currency,
    TAL.netAmount as net_amount_sub_currency, 
    SC.symbol as sub_currency,
    TAL.account as account_id, 
    TL.department department_id,
    TJ.name as journal_type,
    S.custrecord_lum_fixedfxusd_sub as budget_rate_usd,
    T.createdDateTime as tran_created,
    T.LastModifiedDate as tran_modified,
    TL.lineCreatedDate as line_created,
    TL.lineLastModifiedDate as line_modified,
    TAL.lastModifiedDate as accounting_modified,
    to_char(GREATEST(
        coalesce(T.LastModifiedDate, T.createdDateTime), 
        coalesce(TL.lineLastModifiedDate, TL.lineCreatedDate), 
        coalesce(TAL.lastModifiedDate, TL.lineLastModifiedDate, TL.lineCreatedDate)
        ), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

    FROM transactionAccountingLine TAL 
    INNER JOIN transactionLine TL ON TL.id = TAL.transactionLine AND TL.transaction = TAL.transaction 
    INNER JOIN transaction T ON T.id = TAL.transaction 
    LEFT JOIN account A ON A.id = TAL.account 
    LEFT JOIN accountType AT ON AT.id = A.acctType 
    LEFT JOIN subsidiary S ON S.id = TL.subsidiary 
    LEFT JOIN department D ON D.id = TL.department 
    LEFT JOIN accountingPeriod AP ON AP.id = T.postingPeriod 
    LEFT JOIN Location TLL ON TLL.id=TL.location 
    LEFT JOIN entity E ON E.id=T.entity 
    LEFT JOIN entity LE ON LE.id=TL.entity 
    LEFT JOIN Item TLI ON TLI.id=TL.item 
    LEFT JOIN CUSTOMRECORD_PRQ_JOURNAL TJ ON TJ.id=T.custbody_prq_journal 
    LEFT JOIN currency SC ON SC.id=S.currency 
    LEFT JOIN currency TC ON TC.id=T.currency 
    LEFT JOIN item LJI ON LJI.id=TL.custcol_prq_item_je 
    LEFT JOIN customrecord_lum_trantype TT ON TT.name=T.type 
    LEFT JOIN CUSTOMRECORD_LUM_TRANRECORDTYPE TST ON TST.name=T.recordType
    WHERE TAL.posting = 'T' AND AT.balanceSheet = 'F' 
    AND (
        T.lastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        OR TL.lineLastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        OR TAL.lastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
    ) 
    ORDER BY last_modified_date, unique_key
    """

    schema = th.PropertiesList(
        th.Property("unique_key", th.IntegerType),
        th.Property("id", th.IntegerType),
        th.Property("tran_date", th.DateType),
        th.Property("tran_id", th.StringType),
        th.Property("posting_period_name", th.StringType),
        th.Property("posting_period_date", th.DateType),
        th.Property("type_id", th.StringType),
        th.Property("type_db", th.IntegerType),
        th.Property("type_bi", th.IntegerType),
        th.Property("subtype", th.StringType),
        th.Property("subtype_db", th.IntegerType),
        th.Property("subtype_bi", th.IntegerType),
        th.Property("line_id", th.IntegerType),
        th.Property("subsidiary", th.StringType),
        th.Property("department", th.StringType),
        th.Property("department_hierarchy", th.StringType),
        th.Property("location", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("line_memo", th.StringType),
        th.Property("account_number", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("main_entity", th.StringType),
        th.Property("line_entity", th.StringType),
        th.Property("line_item", th.StringType),
        th.Property("journal_item", th.StringType),
        th.Property("net_amount_currency", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("net_amount_sub_currency", th.NumberType),
        th.Property("sub_currency", th.StringType),
        th.Property("account_id", th.IntegerType),
        th.Property("department_id", th.IntegerType),
        th.Property("journal_type", th.StringType),
        th.Property("budget_rate_usd", th.NumberType),
        th.Property("tran_created", th.DateTimeType),
        th.Property("tran_modified", th.DateTimeType),
        th.Property("line_created", th.DateTimeType),
        th.Property("line_modified", th.DateTimeType),
        th.Property("accounting_modified", th.DateTimeType),
        th.Property("last_modified_date", th.DateTimeType),
    ).to_dict()

class PnlAccountsStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "pnl_accounts"
    path = ""

    primary_keys = ["id"]
    replication_key="last_modified_date"
    start_date=datetime.fromisoformat("2015-01-01 00:00:00")
    is_sorted = True

    query = """SELECT 
        A.id as id,
        A.externalid as external_id,
        A.isInactive as is_inactive,
        A.acctNumber as account_number,
        A.fullname as full_name,
        T.longName as account_type,
        TLUM.name as account_pnl_type_lumapps,
        TFR.name as account_pnl_type_fr_gaap,
        CLUM.name as account_pnl_category_lumapps,
        CFR.name as account_pnl_category_fr_gaap,
        BU.name as account_bu,
        I.fullName as account_default_item,
        to_char(A.lastModifiedDate, 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

        FROM account A
        LEFT JOIN accountType T ON T.id = A.acctType
        LEFT JOIN CUSTOMLIST_LUM_PNL_TYPE TLUM ON TLUM.id=A.custrecord_lum_account_pnl_type
        LEFT JOIN CUSTOMLIST_LUM_PNL_TYPE TFR ON TFR.id=A.custrecord_lum_account_pnl_type_fr
        LEFT JOIN CUSTOMLIST_LUM_PNL_CATEGORY CLUM ON CLUM.id=A.custrecord_lum_account_pnl_category
        LEFT JOIN CUSTOMLIST_LUM_PNL_CATEGORY CFR ON CFR.id=A.custrecord_lum_account_pnl_category_fr
        LEFT JOIN CUSTOMLIST_LUM_PNL_BU BU ON BU.id=A.custrecord_lum_account_pnl_bu
        LEFT JOIN item I ON I.id=A.custrecord_lum_account_item_default
        WHERE A.isinactive = 'F'
        AND T.balanceSheet = 'F'
        AND T. longName NOT IN ('Non Posting', 'Statistical')
        AND acctNumber <> '7ARR'
        AND A.lastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ORDER BY last_modified_date, id
    """

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("external_id", th.StringType),
        th.Property("is_inactive", th.StringType),
        th.Property("account_number", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("account_type", th.StringType),
        th.Property("account_pnl_type_lumapps", th.StringType),
        th.Property("account_pnl_type_fr_gaap", th.StringType),
        th.Property("account_pnl_category_lumapps", th.StringType),
        th.Property("account_pnl_category_fr_gaap", th.StringType),
        th.Property("account_bu", th.StringType),
        th.Property("account_default_item", th.StringType),
        th.Property("last_modified_date", th.DateTimeType),
    ).to_dict()

class PnlDepartmentsStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "pnl_departments"
    path = ""
    primary_keys = ["id"]
    replication_key="last_modified_date"
    start_date=datetime.fromisoformat("2015-01-01 00:00:00")
    is_sorted = True

    query = """SELECT 
        D.id as id,
        D.externalId as external_id,
        D.isInactive as is_inactive,
        D.name as department,
        D.fullname as department_hierarchy,
        BU.name as department_bu,
        CC.name as department_cost_center,
        D.custrecord_lum_department_pnl_lic_staff as department_licences_staff,
        D.custrecord_prq_department_level as level,
        to_char(D.lastModifiedDate, 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

        FROM Department D LEFT JOIN CUSTOMLIST_LUM_PNL_BU BU ON BU.id=D.custrecord_lum_department_pnl_bu
        LEFT JOIN CUSTOMRECORD_LUM_PNL_COSTCENTER CC ON CC.id=D.custrecord_lum_department_pnl_costcenter
        WHERE D.lastModifiedDate>to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ORDER BY last_modified_date, D.id
    """

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("external_id", th.StringType),
        th.Property("is_inactive", th.StringType),
        th.Property("department", th.StringType),
        th.Property("department_hierarchy", th.StringType),
        th.Property("department_bu", th.StringType),
        th.Property("department_cost_center", th.StringType),
        th.Property("department_licences_staff", th.IntegerType),
        th.Property("level", th.IntegerType),
        th.Property("last_modified_date", th.DateTimeType),
    ).to_dict()

class PnlConsolidatedExchangeRatesStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "pnl_consolidated_exchange_rates"
    path = ""
    primary_keys = ["id"]
    replication_method="FULL-TABLE"
    
    query = """SELECT
        CER.id as id,
        AP.periodName as posting_period,
        to_char(AP.startDate, 'dd/MM/YYYY') as start_date,
        to_char(AP.endDate, 'dd/MM/YYYY') as end_date,
        SFROM.name as from_subsidiary,
        STO.name as to_subsidiary,
        CFROM.symbol as from_currency,
        CTO.symbol as to_currency,
        CER.averageRate as average_rate

        FROM consolidatedExchangeRate CER
        LEFT JOIN accountingPeriod AP ON AP.id = CER.postingPeriod
        LEFT JOIN Subsidiary SFROM ON SFROM.id=CER.fromSubsidiary
        LEFT JOIN Subsidiary STO ON STO.id=CER.toSubsidiary
        LEFT JOIN currency CFROM ON CFROM.id=CER.fromCurrency
        LEFT JOIN currency CTO ON CTO.id=CER.toCurrency
        ORDER BY CER.id
    """

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("posting_period", th.StringType),
        th.Property("start_date", th.DateType),
        th.Property("end_date", th.DateType),
        th.Property("from_subsidiary", th.StringType),
        th.Property("to_subsidiary", th.StringType),
        th.Property("from_currency", th.StringType),
        th.Property("to_currency", th.StringType),
        th.Property("average_rate", th.NumberType)
    ).to_dict()
