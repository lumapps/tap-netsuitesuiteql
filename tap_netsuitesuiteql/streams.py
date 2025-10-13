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
    # replication_key = None
    replication_method="INCREMENTAL"
    replication_key="last_modified_date"
    is_sorted=True
    start_date=datetime.fromisoformat("2025-01-01 00:00:00")

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
            T.LastModifiedDate>=to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
            OR TL.lineLastModifiedDate>=to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ) 

        ORDER BY last_modified_date
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

    replication_method="INCREMENTAL"
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
            C.lastModifiedDate>=to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
            OR C.dateCreated>=to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
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
    query = """SELECT 
        R.recordid as id, 
        CY.id as country_code,
        R.name as name, 
        RT.name as type, 
        R.parent as parent 

    FROM CUSTOMRECORD_LUM_REGION_CORPORATE R 
    LEFT JOIN CUSTOMLIST_LUM_GEOGRAPHY_TYPE RT ON RT.id = R.custrecord_lum_region_corporate_type 
    LEFT JOIN Country CY ON CY.name=R.name
    WHERE R.isinactive='F' ORDER BY id
    """

    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("country_code", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("parent", th.IntegerType),

    ).to_dict()

class LicensesCountStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "licenses_count"
    path = ""
    primary_keys = ["id"]
    query = "SELECT customrecord_lum_licenses_count.id id, to_char(custrecord_lum_licenses_count_date, 'dd/MM/YYYY') count_date, LT.name as license_count_type, custrecord_lum_licenses_count_enduser enduser_id, custrecord_lum_licenses_count_licenses licenses FROM customrecord_lum_licenses_count LEFT JOIN CUSTOMLIST_LUM_LICENSES_COUNT_TYPE LT ON customrecord_lum_licenses_count.custrecord_lum_licenses_count_type = LT.id ORDER BY custrecord_lum_licenses_count_date DESC,  custrecord_lum_licenses_count_type, custrecord_lum_licenses_count_enduser"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("count_date", th.DateType),
        th.Property("license_count_type", th.StringType),
        th.Property("enduser_id", th.IntegerType),
        th.Property("licenses", th.IntegerType),

    ).to_dict()


class SalesOrdersStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "sales_orders"
    path = ""
    primary_keys = ["unique_key"]

    # replication_key=None
    replication_method="INCREMENTAL"
    replication_key="last_modified_date"
    is_sorted=True
    start_date=datetime.fromisoformat("2025-01-01 00:00:00")

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

        to_char(GREATEST(
            coalesce(T.LastModifiedDate, T.createdDateTime), 
            coalesce(TL.lineLastModifiedDate, TL.lineCreatedDate)
        ), 'YYYY-MM-DD HH24:MI:SS') as last_modified_date

    FROM transaction T   
    LEFT JOIN transactionline TL ON TL.transaction = T.id   
    LEFT JOIN customer CE ON CE.id = T.custbody_prq_end_user   
    LEFT JOIN item I ON I.id = TL.item   
    LEFT JOIN subsidiary S ON S.id = TL.subsidiary   
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
    LEFT JOIN PreviousTransactionLink PREV ON PREV.nextdoc=T.id   
    LEFT JOIN NextTransactionLink NEXT ON NEXT.previousdoc=T.id   
    LEFT JOIN Transaction NEXTTRAN ON NEXTTRAN.id=NEXT.nextdoc  
    LEFT JOIN CUSTOMLIST_LUM_RENEWAL_STATUS RS ON T.custbody_lum_renewal_status = RS.id   
    LEFT JOIN transaction RT ON T.custbody_prq_renewal_linked_trans = RT.id   
    LEFT JOIN employee SR ON T.employee = SR.id 
    WHERE (T.type='SalesOrd' OR T.type='Estimate') 
        AND TL.mainLine = 'F' 
        AND TL.taxLine = 'F' 
        AND CT.name = 'Subscription' 
        AND (
            T.LastModifiedDate>=to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
            OR TL.lineLastModifiedDate>=to_date('__STARTING_TIMESTAMP__', 'YYYY-MM-DD HH24:MI:SS')
        ) 

    ORDER BY last_modified_date
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
        th.Property("last_modified_date", th.DateTimeType)

    ).to_dict()


class ArrRestatementsStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "arr_restatements"
    path = ""
    primary_keys = ["id"]
    query = "SELECT R.id id, R.custrecord_prq_arr_amount arr, to_char(R.custrecord_prq_arr_start_date, 'dd/MM/YYYY') start_date, to_char(R.custrecord_prq_end_date, 'dd/MM/YYYY') end_date, R.custrecord_prq_arr_so_est so_est_id, to_char(R.created, 'dd/MM/YYYY') created, R.custrecord_lum_arr_amendment amendment, R.custrecord_lum_arr_users_dnc ignore_users FROM customrecord_prq_arr_restatement R WHERE isInactive='F' ORDER BY id"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("arr", th.NumberType),
        th.Property("start_date", th.DateType),
        th.Property("end_date", th.DateType),
        th.Property("created", th.DateType),
        th.Property("amendment", th.StringType),
        th.Property("ignore_users", th.StringType),
        th.Property("so_est_id", th.NumberType),

    ).to_dict()


class RenewalItemsStream(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "renewal_items"
    path = ""
    primary_keys = ["id"]
    query = "select id, itemid, displayname from item where (custitem_prq_renewal='T' OR custitem_prq_arr_calculation='T') ORDER BY id"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("itemid", th.StringType),
        th.Property("displayname", th.StringType),

    ).to_dict()

class PostingTransactionLines(NetsuiteSuiteQLStream):
    """Define custom stream."""

    name = "posting_transaction_lines"
    path = ""
    primary_keys = ["unique_key"]
    query = "SELECT  T.id AS id, T.tranid AS tranid,  to_char(T.trandate, 'dd/MM/YYYY') AS trandate,  T.type AS type, T.entity AS entity_id,  E.companyName AS entity, TL.entity AS line_entity_id, LE.companyName AS line_entity, APS.name AS approval_status,  CAPS.name AS approval_custom_status,  T.status AS status, T.memo AS memo, TL.id AS line_id, TL.mainLine AS main_line, TL.taxLine AS tax_line, TL.uniqueKey AS unique_key, SU.name AS subsidiary, DP.name AS department, IT.itemid AS item, LOC.name AS location, INT.name AS interco, REV.name AS revenue, TL.memo AS description, TL.quantity AS quantity, TL.rate AS rate, TL.foreignAmount AS foreign_amount, AM.name AS amortization_sched, to_char(TL.amortizStartDate, 'dd/MM/YYYY') AS amortiz_start_date, to_char(TL.amortizationEndDate, 'dd/MM/YYYY') AS amortization_end_date, to_char(TL.custcol_prq_start_date, 'dd/MM/YYYY') AS start_date_line, to_char(TL.custcol_prq_end_date, 'dd/MM/YYYY') as end_date_line,    TL.isRevRecTransaction AS is_rev_rec_transaction, CU.name AS currency, T.exchangeRate AS exchange_rate, TAL.posting AS posting, AP.periodName AS posting_period, to_char(AP.startDate, 'dd/MM/YYYY') AS posting_period_start_date, TALA.fullname AS account, TL.eliminate AS eliminate, INTT.id AS interco_transaction_id, INTT.tranid AS interco_transaction_tranid, T.intercoStatus AS interco_status, to_char(T.reversalDate, 'dd/MM/YYYY') AS reversal_date, T.reversal AS reversal_id, REVS.tranid AS reversal_tranid, TERM.name AS terms, to_char(T.dueDate, 'dd/MM/YYYY') AS due_date, to_char(T.custbody_document_date, 'dd/MM/YYYY') AS document_date, T.paymentHold AS payment_hold, T.customform AS custom_form FROM transactionLine TL LEFT JOIN transaction T ON T.id = TL.transaction LEFT JOIN transactionAccountingLine TAL ON TAL.transaction = TL.transaction AND TAL.transactionLine=TL.id LEFT JOIN account TALA ON TAL.account=TALA.id LEFT JOIN accountingPeriod AP ON AP.id = T.postingPeriod LEFT JOIN customer E ON E.id=T.entity LEFT JOIN customer LE ON LE.id=TL.entity LEFT JOIN approvalstatus APS ON T.approvalstatus=APS.id LEFT JOIN customlist_prq_approval_custom_status CAPS ON CAPS.id=T.custbody_prq_approval_custom_status LEFT JOIN Subsidiary SU ON TL.subsidiary = SU.id LEFT JOIN department DP ON TL.department = DP.id LEFT JOIN item IT ON IT.id=TL.item LEFT JOIN location LOC ON LOC.id=TL.location LEFT JOIN classification INT ON INT.id=TL.class LEFT JOIN CUSTOMRECORD_CSEG_PRQ_REVENUE REV ON REV.id=TL. cseg_prq_revenue LEFT JOIN AmortizationSchedule AM ON AM.id=TL.amortizationsched LEFT JOIN currency CU ON CU.id=T.currency LEFT JOIN transaction INTT ON INTT.id=T.intercoTransaction LEFT JOIN transaction REVS ON REVS.id=T.reversal LEFT JOIN term TERM ON TERM.id=T.terms WHERE  TAL.posting = 'T' ORDER BY TL.uniqueKey"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("amortiz_start_date", th.DateType),
        th.Property("amortization_end_date", th.DateType),
        th.Property("amortization_sched", th.StringType),
        th.Property("approval_custom_status", th.StringType),
        th.Property("approval_status", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("custom_form", th.IntegerType),
        th.Property("department", th.StringType),
        th.Property("description", th.StringType),
        th.Property("document_date", th.DateType),
        th.Property("due_date", th.DateType),
        th.Property("eliminate", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("entity_id", th.IntegerType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("foreign_amount", th.NumberType),
        th.Property("id", th.IntegerType),
        th.Property("interco", th.StringType),
        th.Property("interco_status", th.IntegerType),
        th.Property("interco_transaction_id", th.IntegerType),
        th.Property("interco_transaction_tranid", th.StringType),
        th.Property("is_rev_rec_transaction", th.StringType),
        th.Property("item", th.StringType),
        th.Property("line_entity", th.StringType),
        th.Property("line_entity_id", th.IntegerType),
        th.Property("line_id", th.IntegerType),
        th.Property("location", th.StringType),
        th.Property("main_line", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("payment_hold", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("posting_period", th.StringType),
        th.Property("posting_period_start_date", th.DateType),
        th.Property("quantity", th.NumberType),
        th.Property("rate", th.NumberType),
        th.Property("revenue", th.StringType),
        th.Property("reversal_date", th.DateType),
        th.Property("reversal_id", th.IntegerType),
        th.Property("reversal_tranid", th.StringType),
        th.Property("start_date_line", th.DateType),
        th.Property("end_date_line", th.DateType),
        th.Property("status", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("tax_line", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("trandate", th.DateType),
        th.Property("tranid", th.StringType),
        th.Property("type", th.StringType),
        th.Property("unique_key", th.IntegerType),

    ).to_dict()


