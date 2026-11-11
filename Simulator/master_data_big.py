"""
master_data.py
Synthetic UPI/eFRM master data.

IMPORTANT:
- Values are synthetic/test values unless explicitly documented otherwise.
- Do not use this file as an NPCI production codebook.
- Validate specification-sensitive codes against the exact UPI/NPCI version used by your test environment.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, TimestampType
)

# ---------------------------------------------------------------------------
# Master/reference values
# ---------------------------------------------------------------------------

APPS = [
    "com.phonepe.app", "net.one97.paytm", "in.org.npci.upiapp",
    "com.google.android.apps.nbu.paisa.user",
    "in.amazon.mShop.android.shopping",
    "com.mobikwik_new", "com.freecharge.android",
    "com.csam.icici.bank.imobile", "com.axis.mobile",
    "com.sbi.upi", "com.dbs.in.digitalbank"
]

APP_NAMES = [
    "PhonePe", "Paytm", "BHIM", "Google Pay", "Amazon Pay",
    "MobiKwik", "Freecharge", "iMobile", "Axis Mobile",
    "YONO", "DBS digibank"
]

OS_LIST = ["Android", "iOS"]
ANDROID_VERSIONS = ["11", "12", "13", "14", "15", "16"]
IOS_VERSIONS = ["16", "17", "18", "19"]

DEVICE_TYPES = ["ANDROID_PHONE", "IOS_PHONE", "ANDROID_TABLET", "IOS_TABLET"]
DEVICE_BRANDS = [
    "SAMSUNG", "APPLE", "ONEPLUS", "XIAOMI", "REALME", "VIVO",
    "OPPO", "MOTOROLA", "GOOGLE", "NOTHING", "NOKIA", "POCO"
]
DEVICE_MODELS = [
    "Galaxy A15", "Galaxy A35", "Galaxy A55", "Galaxy S23", "Galaxy S24",
    "iPhone 14", "iPhone 15", "iPhone 16", "OnePlus 11", "OnePlus 12",
    "Redmi Note 13", "Redmi Note 14", "Realme 12", "Vivo V30",
    "Oppo Reno 11", "Moto G84", "Pixel 8", "Pixel 9", "Nothing Phone 2"
]

BANKS = [
    ("HDFC", "HDFC Bank"), ("ICIC", "ICICI Bank"), ("SBIN", "State Bank of India"),
    ("UTIB", "Axis Bank"), ("KKBK", "Kotak Mahindra Bank"), ("YESB", "YES BANK"),
    ("PUNB", "Punjab National Bank"), ("CNRB", "Canara Bank"),
    ("BARB", "Bank of Baroda"), ("BKID", "Bank of India"),
    ("UBIN", "Union Bank of India"), ("IDIB", "Indian Bank"),
    ("IOBA", "Indian Overseas Bank"), ("FDRL", "Federal Bank"),
    ("INDB", "IndusInd Bank"), ("IDFB", "IDFC FIRST Bank"),
    ("AUBL", "AU Small Finance Bank"), ("RBLB", "RBL Bank"),
    ("JIOP", "Jio Payments Bank")
]

BANK_TYPES = ["PUBLIC", "PRIVATE", "SMALL_FINANCE", "PAYMENTS", "FOREIGN", "COOPERATIVE"]

HANDLES = [
    "@ybl", "@axl", "@okhdfcbank", "@oksbi", "@okicici", "@paytm",
    "@ibl", "@upi", "@okaxis", "@okyes", "@okbob", "@okcanara", "@okunion"
]

PSPS = [
    "PHONEPE", "GOOGLEPAY", "PAYTM", "AMAZONPAY", "BHARATPE",
    "CRED", "MOBIKWIK", "NAVI", "BANK_PSP", "OTHER_TPAP"
]

PAYMENT_CHANNELS = ["UPI_APP", "QR", "INTENT", "COLLECT", "IN_APP", "WEB", "BANK_APP"]
TRANSACTION_TYPES = ["P2P", "P2M", "P2A", "COLLECT", "REFUND", "REVERSAL", "MANDATE"]
PAYMENT_FLOWS = ["PAY", "COLLECT", "REFUND", "REVERSAL", "MANDATE"]
INTEGRATION_MODES = ["QR", "INTENT", "COLLECT", "APP"]

PURPOSE_CODES = ["00", "01", "02"]
INITIATION_MODES = ["00", "01", "02", "11"]

CURRENCIES = ["INR"]
COUNTRIES = ["IN"]
TELECOM_OPERATORS = ["JIO", "AIRTEL", "VI", "BSNL"]
NETWORK_TYPES = ["4G", "5G", "WIFI", "3G"]

ZONES = ["EAST", "WEST", "NORTH", "SOUTH", "CENTRAL", "NORTHEAST"]
STATES = [
    "WEST_BENGAL", "MAHARASHTRA", "KARNATAKA", "TAMIL_NADU",
    "TELANGANA", "DELHI", "GUJARAT", "RAJASTHAN", "UTTAR_PRADESH",
    "BIHAR", "ODISHA", "ASSAM", "KERALA", "PUNJAB", "HARYANA",
    "MADHYA_PRADESH", "ANDHRA_PRADESH", "JHARKHAND", "CHHATTISGARH",
    "UTTARAKHAND"
]
CITIES = [
    "KOLKATA", "MUMBAI", "BENGALURU", "CHENNAI", "HYDERABAD",
    "NEW_DELHI", "PUNE", "AHMEDABAD", "JAIPUR", "BHUBANESWAR",
    "LUCKNOW", "PATNA", "GUWAHATI", "KOCHI", "INDORE", "SURAT"
]

CUSTOMER_TYPES = ["INDIVIDUAL", "MERCHANT", "CORPORATE", "SMALL_BUSINESS"]
ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "SALARY", "NRE", "NRO"]
MERCHANT_TYPES = [
    "GROCERY", "RESTAURANT", "FUEL", "E_COMMERCE", "TRAVEL",
    "EDUCATION", "HEALTHCARE", "UTILITY", "RETAIL", "ENTERTAINMENT",
    "PHARMACY", "FOOD_DELIVERY", "TRANSPORT", "HOTEL"
]
MERCHANT_SIZES = ["MICRO", "SMALL", "MEDIUM", "LARGE", "ENTERPRISE"]

MCC_CODES = [
    "5411", "5812", "5541", "5311", "5912", "4111", "4722",
    "5732", "6012", "6300", "7011", "7999", "4900", "8299"
]

AUTH_METHODS = ["UPI_PIN", "BIOMETRIC", "DEVICE_AUTH", "OTP"]
AUTH_STATUSES = ["SUCCESS", "FAILED", "NOT_REQUIRED"]
PROCESSING_STATUSES = ["SUCCESS", "FAILURE", "PENDING", "TIMEOUT", "REVERSED", "DEEMED"]
DECISIONS = ["ALLOW", "DECLINE", "CHALLENGE", "REVIEW"]
RISK_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
FRAUD_TYPES = [
    "NONE", "ACCOUNT_TAKEOVER", "SOCIAL_ENGINEERING", "MULE_ACCOUNT",
    "DEVICE_COMPROMISE", "VELOCITY_ABUSE", "SUSPICIOUS_MERCHANT",
    "SUSPICIOUS_BENEFICIARY", "LOCATION_ANOMALY"
]

RISK_FLAGS = [
    "NEW_DEVICE", "NEW_BENEFICIARY", "HIGH_AMOUNT", "HIGH_VELOCITY",
    "LOCATION_MISMATCH", "DEVICE_MISMATCH", "UNUSUAL_TIME",
    "UNUSUAL_AMOUNT", "MULTIPLE_ACCOUNTS_DEVICE", "MULTIPLE_DEVICES_ACCOUNT",
    "MULTIPLE_IP_DEVICE", "SUSPICIOUS_IP", "VPN_DETECTED", "PROXY_DETECTED",
    "ACCOUNT_AGE_LOW", "BENEFICIARY_AGE_LOW", "RAPID_SUCCESSION",
    "FAILED_TXN_SPIKE"
]

RESULT_ERR_CODES = [
    ("SUCCESS", "00"),
    ("FAILURE", "U30"),
    ("FAILURE", "FR"),
    ("FAILURE", "U01"),
    ("FAILURE", "U16"),
    ("FAILURE", "U17"),
    ("FAILURE", "U28"),
    ("PENDING", "91"),
]

SERVER_REGIONS = ["EAST", "WEST", "NORTH", "SOUTH"]
SERVICE_NAMES = [
    "upi-gateway", "upi-switch", "risk-engine", "datalake-consumer",
    "notification-service", "case-management-service"
]
PROCESSOR_TYPES = ["ONLINE", "OFFLINE"]
DB_SYSTEMS = ["YUGABYTEDB", "CASSANDRA", "HIVE", "ELASTICSEARCH"]

LANGUAGES = ["en_IN", "bn_IN", "hi_IN", "ta_IN", "te_IN", "mr_IN", "gu_IN"]
TIME_PERIODS = ["EARLY_MORNING", "MORNING", "AFTERNOON", "EVENING", "NIGHT", "LATE_NIGHT"]

# ---------------------------------------------------------------------------
# 165+ transaction attributes / schema
# ---------------------------------------------------------------------------

UPI_EFRM_SCHEMA = StructType([
    StructField("txn_id", StringType(), False),
    StructField("txn_ref_id", StringType(), True),
    StructField("org_txn_id", StringType(), True),
    StructField("parent_txn_id", StringType(), True),
    StructField("rrn", StringType(), True),
    StructField("message_id", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_ref_id", StringType(), True),
    StructField("merchant_ref_id", StringType(), True),

    StructField("txn_timestamp", TimestampType(), False),
    StructField("txn_date", StringType(), False),
    StructField("txn_year", IntegerType(), False),
    StructField("txn_month", IntegerType(), False),
    StructField("txn_day", IntegerType(), False),
    StructField("txn_hour", IntegerType(), False),
    StructField("txn_minute", IntegerType(), False),
    StructField("txn_second", IntegerType(), False),
    StructField("day_of_week", IntegerType(), False),
    StructField("week_of_year", IntegerType(), False),
    StructField("quarter", IntegerType(), False),
    StructField("is_weekend", BooleanType(), False),
    StructField("is_holiday", BooleanType(), False),
    StructField("is_business_hour", BooleanType(), False),
    StructField("time_period", StringType(), False),
    StructField("festival_flag", BooleanType(), False),
    StructField("salary_day_flag", BooleanType(), False),
    StructField("month_start_flag", BooleanType(), False),
    StructField("month_end_flag", BooleanType(), False),

    StructField("payer_id", StringType(), False),
    StructField("payer_vpa", StringType(), False),
    StructField("payer_bank_code", StringType(), False),
    StructField("payer_bank_name", StringType(), False),
    StructField("payer_ifsc", StringType(), False),
    StructField("payer_account_type", StringType(), False),
    StructField("payer_customer_type", StringType(), False),
    StructField("payer_psp", StringType(), False),
    StructField("payer_mobile_hash", StringType(), False),

    StructField("payee_id", StringType(), False),
    StructField("payee_vpa", StringType(), False),
    StructField("payee_bank_code", StringType(), False),
    StructField("payee_bank_name", StringType(), False),
    StructField("payee_ifsc", StringType(), False),
    StructField("payee_account_type", StringType(), False),
    StructField("payee_customer_type", StringType(), False),
    StructField("payee_psp", StringType(), False),

    StructField("merchant_id", StringType(), True),
    StructField("merchant_vpa", StringType(), True),
    StructField("merchant_type", StringType(), True),
    StructField("merchant_size", StringType(), True),
    StructField("merchant_mcc", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_pincode", StringType(), True),
    StructField("merchant_account_age_days", IntegerType(), True),

    StructField("app", StringType(), False),
    StructField("app_name", StringType(), False),
    StructField("app_version", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("device_fingerprint", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("device_brand", StringType(), False),
    StructField("device_model", StringType(), False),
    StructField("device_os", StringType(), False),
    StructField("device_os_version", StringType(), False),
    StructField("device_language", StringType(), False),
    StructField("device_timezone", StringType(), False),
    StructField("device_rooted", BooleanType(), False),
    StructField("device_emulator", BooleanType(), False),
    StructField("device_first_seen_days_ago", IntegerType(), False),
    StructField("device_last_seen_minutes_ago", IntegerType(), False),

    StructField("mobile_operator", StringType(), False),
    StructField("network_type", StringType(), False),
    StructField("ip_address", StringType(), False),
    StructField("ip_country", StringType(), False),
    StructField("ip_region", StringType(), False),
    StructField("ip_city", StringType(), False),
    StructField("ip_risk_score", DoubleType(), False),
    StructField("vpn_flag", BooleanType(), False),
    StructField("proxy_flag", BooleanType(), False),

    StructField("payer_country", StringType(), False),
    StructField("payer_state", StringType(), False),
    StructField("payer_city", StringType(), False),
    StructField("payer_pincode", StringType(), False),
    StructField("payee_country", StringType(), False),
    StructField("payee_state", StringType(), False),
    StructField("payee_city", StringType(), False),
    StructField("payee_pincode", StringType(), False),
    StructField("payer_latitude", DoubleType(), False),
    StructField("payer_longitude", DoubleType(), False),
    StructField("payee_latitude", DoubleType(), False),
    StructField("payee_longitude", DoubleType(), False),
    StructField("geo_distance_km", DoubleType(), False),
    StructField("location_mismatch", BooleanType(), False),

    StructField("amount", DoubleType(), False),
    StructField("requested_amount", DoubleType(), False),
    StructField("settled_amount", DoubleType(), False),
    StructField("refund_amount", DoubleType(), False),
    StructField("fee_amount", DoubleType(), False),
    StructField("tax_amount", DoubleType(), False),
    StructField("discount_amount", DoubleType(), False),
    StructField("cashback_amount", DoubleType(), False),
    StructField("currency", StringType(), False),

    StructField("transaction_type", StringType(), False),
    StructField("payment_flow", StringType(), False),
    StructField("payment_channel", StringType(), False),
    StructField("integration_mode", StringType(), False),
    StructField("purpose_code", StringType(), False),
    StructField("initiation_mode", StringType(), False),

    StructField("authentication_method", StringType(), False),
    StructField("authentication_status", StringType(), False),
    StructField("authentication_attempt_count", IntegerType(), False),
    StructField("authentication_failure_count", IntegerType(), False),
    StructField("pin_retry_count", IntegerType(), False),
    StructField("device_binding_status", StringType(), False),
    StructField("mobile_binding_status", StringType(), False),

    StructField("txn_count_1m", IntegerType(), False),
    StructField("txn_count_5m", IntegerType(), False),
    StructField("txn_count_15m", IntegerType(), False),
    StructField("txn_count_30m", IntegerType(), False),
    StructField("txn_count_1h", IntegerType(), False),
    StructField("txn_count_6h", IntegerType(), False),
    StructField("txn_count_24h", IntegerType(), False),
    StructField("amount_1h", DoubleType(), False),
    StructField("amount_6h", DoubleType(), False),
    StructField("amount_24h", DoubleType(), False),
    StructField("unique_payees_1h", IntegerType(), False),
    StructField("unique_payees_24h", IntegerType(), False),
    StructField("unique_devices_24h", IntegerType(), False),
    StructField("unique_ips_24h", IntegerType(), False),
    StructField("unique_banks_24h", IntegerType(), False),

    StructField("payer_account_age_days", IntegerType(), False),
    StructField("payer_txn_count_24h", IntegerType(), False),
    StructField("payer_txn_count_7d", IntegerType(), False),
    StructField("payer_amount_24h", DoubleType(), False),
    StructField("payer_amount_7d", DoubleType(), False),
    StructField("payee_account_age_days", IntegerType(), False),
    StructField("payee_txn_count_24h", IntegerType(), False),
    StructField("payee_amount_24h", DoubleType(), False),

    StructField("merchant_txn_count_24h", IntegerType(), True),
    StructField("merchant_amount_24h", DoubleType(), True),
    StructField("merchant_risk_score", DoubleType(), True),

    StructField("new_device_flag", BooleanType(), False),
    StructField("new_beneficiary_flag", BooleanType(), False),
    StructField("high_amount_flag", BooleanType(), False),
    StructField("high_velocity_flag", BooleanType(), False),
    StructField("unusual_time_flag", BooleanType(), False),
    StructField("unusual_amount_flag", BooleanType(), False),
    StructField("multiple_accounts_device_flag", BooleanType(), False),
    StructField("multiple_devices_account_flag", BooleanType(), False),
    StructField("suspicious_ip_flag", BooleanType(), False),

    StructField("risk_score", DoubleType(), False),
    StructField("rule_score", DoubleType(), False),
    StructField("ml_score", DoubleType(), False),
    StructField("velocity_score", DoubleType(), False),
    StructField("device_score", DoubleType(), False),
    StructField("location_score", DoubleType(), False),
    StructField("merchant_score", DoubleType(), False),
    StructField("customer_score", DoubleType(), False),
    StructField("network_score", DoubleType(), False),
    StructField("risk_level", StringType(), False),
    StructField("fraud_type", StringType(), False),
    StructField("rule_triggered", BooleanType(), False),
    StructField("rule_id", StringType(), True),
    StructField("rule_count", IntegerType(), False),

    StructField("processing_status", StringType(), False),
    StructField("response_code", StringType(), False),
    StructField("error_code", StringType(), True),
    StructField("timeout_flag", BooleanType(), False),
    StructField("retry_flag", BooleanType(), False),
    StructField("retry_count", IntegerType(), False),
    StructField("reprocessed_flag", BooleanType(), False),

    StructField("decision", StringType(), False),
    StructField("decision_reason", StringType(), False),
    StructField("is_fraud", IntegerType(), False),
    StructField("is_suspicious", IntegerType(), False),
    StructField("is_declined", IntegerType(), False),
    StructField("is_reversed", IntegerType(), False),
    StructField("is_manual_review", IntegerType(), False),

    StructField("online_offline_flag", StringType(), False),
    StructField("processor_type", StringType(), False),
    StructField("processing_node", StringType(), False),
    StructField("server_region", StringType(), False),
    StructField("availability_zone", StringType(), False),
    StructField("service_name", StringType(), False),
    StructField("service_version", StringType(), False),
    StructField("kafka_topic", StringType(), False),
    StructField("kafka_partition", IntegerType(), False),
    StructField("kafka_offset", LongType(), False),
    StructField("consumer_group", StringType(), False),
    StructField("consumer_instance", StringType(), False),
    StructField("event_lag_ms", LongType(), False),
    StructField("processing_latency_ms", IntegerType(), False),
    StructField("queue_latency_ms", IntegerType(), False),
    StructField("database_latency_ms", IntegerType(), False),
    StructField("total_latency_ms", IntegerType(), False),
    StructField("yugabyte_flag", BooleanType(), False),
    StructField("cassandra_flag", BooleanType(), False),
    StructField("hive_flag", BooleanType(), False),
    StructField("elasticsearch_flag", BooleanType(), False),
])

ATTRIBUTE_COUNT = len(UPI_EFRM_SCHEMA.fields)
