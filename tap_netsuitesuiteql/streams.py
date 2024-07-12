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

    name = "arr_history "
    path = ""
    primary_keys = ["id"]
    query = "SELECT T.id id, T.trandate arr_date, TL.netamount, T.currency arr_currency, AT.name arr_type, CE.id enduser_id, CE.custentity_lum_cus_sfid enduser_sfid, LT.tranid so_tranid, LT.currency so_currency, LT.exchangerate so_exchange_rate, CR.custentity_lum_cus_sfid reseller_sfid, CP.custentity_lum_cus_sfid partner_sfid, S.name subsidiary_name, S.currency subsidiary_currency, CT.name contract_name, CT.id contract_id, CEP.custentity_lum_cus_sfid enduser_parent_sfid, CEGP.custentity_lum_cus_sfid enduser_grandparent_sfid, S.custrecord_lum_fixedfxusd_sub subsidiary_exchange_rate, FROM transaction T  LEFT JOIN transactionline TL ON TL.transaction = T.id  LEFT JOIN customlist_lum_arrh_arrtype AT ON AT.id = T.custbody_lum_arrh_arrtype  LEFT JOIN customer CE ON CE.id = T.custbody_lum_arrh_enduser LEFT JOIN customrecord_prq_contract CT ON CT.id = T.custbody_lum_arrh_contract LEFT JOIN customer CR ON CR.id = T.custbody_lum_arrh_reseller LEFT JOIN customer CP ON CP.id = T.custbody_lum_arrh_partner LEFT JOIN subsidiary S ON S.id = TL.subsidiary LEFT JOIN transaction LT ON LT.id = T.custbody_lum_arrh_transaction LEFT JOIN customer CEP ON CEP.id = CE.parent LEFT JOIN customer CEGP ON CEGP.id = CEP.parent WHERE AT.name IN ('ARR', 'cARR') AND T.recordtype = 'customtransaction_lum_arrh'  AND TL.mainline = 'F'"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "id",
            th.IntegerType,
        ),
        th.Property(
            "arr_date",
            th.DateType,
        ),
        th.Property(
            "netamount",
            th.NumberType,
        ),
        th.Property("arr_currency", th.IntegerType),
        th.Property("arr_type", th.StringType),
        th.Property(
            "enduser_id",
            th.IntegerType,
        ),
        th.Property("so_tranid", th.StringType),
        th.Property("so_currency", th.StringType),
        th.Property("so_exchange_rate", th.StringType),
        th.Property("reseller_sfid", th.StringType),
        th.Property("partner_sfid", th.StringType),
        th.Property("subsidiary_name", th.StringType),
        th.Property("subsidiary_currency", th.IntegerType),
        th.Property("contract_name", th.StringType),
        th.Property("contract_id", th.IntegerType),
        th.Property("enduser_parent_sfid", th.StringType),
        th.Property("enduser_grandparent_sfid", th.StringType),
        th.Property("subsidiary_exchange_rate", th.StringType),

    ).to_dict()
