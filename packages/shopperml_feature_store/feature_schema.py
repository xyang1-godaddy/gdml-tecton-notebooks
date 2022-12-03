# NOTE: These are the only shopperml-feature-store feature groups with
# specified schemas; when adding more, make sure to update feature_schema.py
DEFINED_FEATURE_GROUPS = (
    "domaininfo",
    "marketing_cdl",
    "uds_traffic_last10",
    "uds_order_lifetime",
    "uds_product_billing",
    "uds_order_last90",
)

FLOAT_FIELDS = [
    "marketing_cdl.b_xxx_china_cust",
    "marketing_cdl.b_xxx_first_order_viral",
    "marketing_cdl.b_xxx_first_pop_c3",
    "marketing_cdl.b_xxx_iscompany",
    "marketing_cdl.b_xxx_isattrition_computed",
    "marketing_cdl.b_xxx_isdonotcall",
    "marketing_cdl.b_xxx_isreseller",
    "marketing_cdl.d_bin_last_mm_order_days",
    "marketing_cdl.d_bin_last_bb_order_days",
    "marketing_cdl.d_bin_last_email_order_days",
    "marketing_cdl.d_bin_last_bb_contact_days",
    "marketing_cdl.d_bin_last_mm_contact_days",
    "marketing_cdl.d_bin_last_email_contact_days",
    "marketing_cdl.d_bin_last_bb_open_days",
    "marketing_cdl.d_bin_last_mm_open_days",
    "marketing_cdl.d_bin_last_email_open_days",
    "marketing_cdl.i_xxx_bb_contacts_30d",
    "marketing_cdl.i_xxx_bb_contacts_90d",
    "marketing_cdl.i_xxx_bb_contacts_1y",
    "marketing_cdl.i_xxx_bb_opens_90d",
    "marketing_cdl.i_xxx_bb_opens_30d",
    "marketing_cdl.i_xxx_email_contacts_30d",
    "marketing_cdl.i_xxx_email_contacts_90d",
    "marketing_cdl.i_xxx_email_contacts_1y",
    "marketing_cdl.i_xxx_email_opens_90d",
    "marketing_cdl.i_xxx_email_opens_30d",
]

STRING_FIELDS = [
    "marketing_cdl.c_xxx_countrycode",
    "marketing_cdl.c_xxx_tenure_months",
]

STRING_ARRAY_FIELDS = [
    "domaininfo.status",
    "domaininfo.previousregistrarid",
    "domaininfo.gaining_registrar_id",
    "uds_order_lifetime.product_pnl_line_name",
    "uds_order_last90.product_type_id",
    "uds_order_last90.free_order_flag",
    "uds_order_last90.bundle_id",
    "uds_order_last90.product_free_trial_flag",
    "uds_order_last90.domain_bulk_flag",
    "uds_order_last90.primary_payment_type_name",
    "uds_order_last90.primary_payment_subtype_name",
    "uds_order_last90.product_name",
    "uds_order_last90.product_period_name",
    "uds_order_last90.product_pnl_new_renewal_name",
    "uds_order_last90.item_tracking_code",
    "uds_order_last90.order_id",
    "uds_order_last90.order_isc_source_code",
    "uds_order_last90.order_isc_channel_id",
    "uds_order_last90.order_country_code",
    "uds_order_last90.order_site_language_code",
    "uds_order_last90.point_of_purchase_name",
    "uds_order_last90.order_state_code",
    "uds_order_last90.purchase_path_name",
    "uds_order_last90.secondary_payment_type_name",
    "uds_order_last90.secondary_payment_subtype_name",
    "uds_order_last90.product_purchase_type_name",
    "uds_order_last90.product_pnl_line_name",
    "uds_order_last90.original_product_pnl_new_renewal_name",
    "uds_order_last90.order_isc_channel_name",
    "uds_order_last90.order_isc_sub_channel_name",
    "uds_order_last90.order_country_name",
    "uds_order_last90.order_domestic_international_name",
    "uds_order_last90.order_region_1_name",
    "uds_order_last90.order_region_2_name",
    "uds_order_last90.crm_portfolio_type_name",
    "uds_order_last90.crm_portfolio_rep_name",
    "uds_order_last90.c3_rep_name",
    "uds_order_last90.c3_call_center_location_name",
    "uds_order_last90.reseller_name",
    "uds_product_billing.product_name",
    "uds_product_billing.product_pnl_group_name",
    "uds_product_billing.product_pnl_category_name",
    "uds_product_billing.product_type_name",
    "uds_product_billing.billing_attempt_sequence_name",
    "uds_product_billing.product_period_name",
    "uds_product_billing.product_period_qty",
    "uds_traffic_last10.session_type_name",
    "uds_traffic_last10.session_viewed_sales_page_flag",
    "uds_traffic_last10.session_viewed_help_page_flag",
    "uds_traffic_last10.session_mobile_app_flag",
    "uds_traffic_last10.session_bounce_flag",
    "uds_traffic_last10.session_free_trial_signup_flag",
    "uds_traffic_last10.session_purchase_flag",
    "uds_traffic_last10.new_product_purchased_flag",
    "uds_traffic_last10.renewal_product_purchased_flag",
    "uds_traffic_last10.repeat_shopper_flag",
    "uds_traffic_last10.repeat_visitor_flag",
    "uds_traffic_last10.site_language_code",
    "uds_traffic_last10.site_country_code",
    "uds_traffic_last10.ip_country_code",
    "uds_traffic_last10.referring_domain_name",
    "uds_traffic_last10.browser_name",
    "uds_traffic_last10.browser_operating_system_name",
    "uds_traffic_last10.isc_channel_name",
    "uds_traffic_last10.isc_sub_channel_name",
    "uds_traffic_last10.referred_by",
    "uds_traffic_last10.device_type_name",
    "uds_traffic_last10.landing_page_name",
    "uds_traffic_last10.exit_page_name",
    "uds_traffic_last10.shopper_ids",
    "uds_traffic_last10.visit_guid",
    "uds_traffic_last10.session_begin_date",
    "uds_traffic_last10.session_begin_ts",
    "uds_traffic_last10.session_end_ts",
    "uds_traffic_last10.host_names",
    "uds_traffic_last10.fully_qualified_page_names",
    "uds_order_last90.visit_guid",
    "uds_order_last90.order_date",
    "uds_order_last90.order_ts",
]

FLOAT_ARRAY_FIELDS = [
    "domaininfo.isactiveflag",
    "domaininfo.billingstatus",
    "domaininfo.autorenewflag",
    "domaininfo.isproxied",
    "domaininfo.islocked",
    "domaininfo.isregistrarhold",
    "domaininfo.islimited",
    "domaininfo.issuperlocked",
    "domaininfo.isinternaltransfer",
    "domaininfo.isexpirationprotected",
    "domaininfo.istransferprotected",
    "domaininfo.issmartdomain",
    "domaininfo.invalidwhois",
    "domaininfo.fraud",
    "domaininfo.valuation_wholesale",
    "domaininfo.valuation_sale",
    "uds_order_lifetime.order_count",
    "uds_order_lifetime.total_quantity",
    "uds_order_lifetime.total_spent",
    "uds_order_last90.receipt_qty",
    "uds_order_last90.product_unit_qty",
    "uds_order_last90.receipt_price_amt",
    "uds_order_last90.product_period_qty",
    "uds_order_last90.gcr_amt",
    "uds_order_last90.fair_market_value_amt",
    "uds_product_billing.primary_product_flag",
    "uds_product_billing.auto_renewal_flag",
    "uds_product_billing.original_list_price_amt",
    "uds_product_billing.original_order_product_free_trial_flag",
    "uds_traffic_last10.page_views_qty",
    "uds_traffic_last10.visits_qty",
    "uds_traffic_last10.orders_qty",
    "uds_traffic_last10.new_orders_qty",
    "uds_traffic_last10.renewal_orders_qty",
    "uds_traffic_last10.receipt_price_amt",
    "uds_traffic_last10.session_duration_seconds",
    "uds_traffic_last10.row_num",
]

# Borrowed from https://github.secureserver.net/shopperml/shopperml/blob/master/shopperml/features/lib/numeric_types.py
TRUE_VALUES = (True, 1, "true", "True", "1", "Y")
FALSE_VALUES = (False, 0, None, "false", "False", "0", "N")


def all_fields():
    return FLOAT_FIELDS + STRING_FIELDS + STRING_ARRAY_FIELDS + FLOAT_ARRAY_FIELDS


def initialize_columns(dataframe, feature_groups):
    new_columns = [field for field in all_fields() if field.split(".")[0] in feature_groups]
    for field in new_columns:
        dataframe[field] = None

    return dataframe


def coerce_float(x):
    try:
        return float(x)
    except TypeError:
        return 0.0
    except ValueError:
        if x in TRUE_VALUES:
            return 1.0
        elif x in FALSE_VALUES:
            return 0.0
        return 0.0


def apply(dataframe):
    for field in FLOAT_FIELDS:
        if field in dataframe.columns:
            dataframe[field] = dataframe[field].astype(float)

    for field in STRING_FIELDS:
        if field in dataframe.columns:
            dataframe[field] = dataframe[field].astype(str)

    for field in STRING_ARRAY_FIELDS:
        if field in dataframe.columns:
            dataframe[field] = dataframe[field].map(lambda x: [str(i) for i in x] if x is not None else [])

    for field in FLOAT_ARRAY_FIELDS:
        if field in dataframe.columns:
            dataframe[field] = dataframe[field].map(lambda x: [coerce_float(i) for i in x] if x is not None else [])
    return dataframe
