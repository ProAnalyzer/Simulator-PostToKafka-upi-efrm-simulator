"""
master_data.py — Synthetic UPI/eFRM master data for PySpark.

Usage:
    from master_data import *
    print(len(DEVICE_MODELS))  # 1000
    print(len(APP_NAMES))      # 1000

Important:
- Values explicitly marked SYNTHETIC are generated test values, not official
  NPCI, bank, telecom, MCC, or production identifiers.
- Protocol-sensitive codes are intentionally kept to a small controlled list.
  Do not expand official code lists with invented values.
- This module has no PySpark dependency except for UPI_EFRM_SCHEMA, which is
  created lazily by build_upi_efrm_schema().
"""

from itertools import product

TARGET_VALUES = 1000


def _unique(values):
    """Keep order while removing duplicates."""
    return list(dict.fromkeys(values))


def _expand(prefix, seeds, target=TARGET_VALUES, width=4):
    """Build a deterministic list of unique synthetic labels."""
    result = _unique(list(seeds))
    i = 1
    while len(result) < target:
        result.append(f"{prefix}_SYNTH_{i:0{width}d}")
        i += 1
    return result


def _combine(prefix, left, right, target=TARGET_VALUES):
    """Create deterministic synthetic combinations."""
    result = []
    for a, b in product(left, right):
        result.append(f"{prefix}_{a}_{b}")
        if len(result) >= target:
            return _unique(result)
    return _expand(prefix, result, target)


# ---------------------------------------------------------------------------
# Apps and device inventory
# ---------------------------------------------------------------------------

APPS = [
    "com.phonepe.app", "net.one97.paytm", "in.org.npci.upiapp",
    "com.google.android.apps.nbu.paisa.user",
    "in.amazon.mShop.android.shopping", "com.mobikwik_new",
    "com.freecharge", "com.myairtelapp", "com.bharatpe.app",
    "com.whatsapp", "com.sbi.lotusintouch", "com.axis.mobile",
    "com.axis.mobilebanking", "com.snapwork.hdfc",
    "com.csam.icici.bank.imobile", "com.kotak.mobilebanking",
    "com.federalbank.mobile", "com.yesbank.mobile",
    "com.indusind.mobile", "com.rblbank.mobank", "com.au.bank",
    "com.flipkart.android", "com.jio.myjio",
]
APPS = _expand("app", APPS)

APP_NAMES = _expand("PaymentApp", [
    "PhonePe", "Paytm", "BHIM", "Google Pay", "Amazon Pay",
    "MobiKwik", "Freecharge", "Airtel Payments", "BharatPe",
    "WhatsApp Pay", "SBI Banking", "Axis Mobile", "HDFC MobileBanking",
    "iMobile Pay", "Kotak Mobile Banking", "Federal Bank",
    "YES BANK", "IndusMobile", "RBL MoBank", "AU 0101",
    "Flipkart Pay", "MyJio",
])

OS_LIST = ["Android", "iOS"]
ANDROID_VERSIONS = [
    "10", "11", "12", "12L", "13", "14", "15", "16"
]
IOS_VERSIONS = [
    "15", "15.7", "16", "16.7", "17", "17.7", "18", "18.5", "26"
]
DEVICE_TYPES = [
    "SMARTPHONE", "TABLET", "WEARABLE", "EMULATOR",
    "VIRTUAL_DEVICE", "UNKNOWN"
]
DEVICE_BRANDS = _expand("DeviceBrand", [
    "Samsung", "Xiaomi", "Redmi", "OnePlus", "OPPO", "Vivo",
    "Realme", "Motorola", "Google", "Apple", "Nothing", "iQOO",
    "POCO", "Tecno", "Infinix", "Lava", "Nokia", "Honor",
    "Asus", "Lenovo", "Huawei", "Sony", "Micromax", "Itel",
])
DEVICE_MODELS = _expand("DeviceModel", [
    "Galaxy S24", "Galaxy A55", "Galaxy M35", "Redmi Note 13",
    "Redmi Note 14", "Xiaomi 14", "OnePlus 12", "OnePlus Nord 4",
    "OPPO Reno 11", "Vivo V30", "Realme 12 Pro", "Pixel 8",
    "Pixel 9", "iPhone 15", "iPhone 16", "Moto Edge 50",
    "Nothing Phone 2", "iQOO Neo 9", "POCO X6", "Lava Blaze",
])

# ---------------------------------------------------------------------------
# Banks, handles, PSPs
# ---------------------------------------------------------------------------

BANKS = [
    "State Bank of India", "HDFC Bank", "ICICI Bank", "Axis Bank",
    "Kotak Mahindra Bank", "Punjab National Bank", "Bank of Baroda",
    "Canara Bank", "Union Bank of India", "Bank of India",
    "Indian Bank", "Central Bank of India", "UCO Bank",
    "Indian Overseas Bank", "Bank of Maharashtra", "YES BANK",
    "IndusInd Bank", "IDFC FIRST Bank", "Federal Bank",
    "South Indian Bank", "Karnataka Bank", "Karur Vysya Bank",
    "City Union Bank", "Tamilnad Mercantile Bank", "RBL Bank",
    "Bandhan Bank", "AU Small Finance Bank", "Equitas Small Finance Bank",
    "Jana Small Finance Bank", "ESAF Small Finance Bank",
    "Jio Payments Bank", "Airtel Payments Bank", "India Post Payments Bank",
]
BANKS = _expand("SyntheticBank", BANKS)

HANDLES = [
    "@ybl", "@axl", "@okhdfcbank", "@oksbi", "@okicici", "@paytm",
    "@ibl", "@upi", "@apl", "@jio", "@okaxis", "@okbob",
    "@okunionbank", "@okcanara", "@okpnb", "@okyes", "@okindus",
    "@okfederal", "@okkotak", "@freecharge", "@mobikwik",
    "@idfcbank", "@indus", "@fbl", "@aubank", "@dbs", "@cnrb",
    "@barodampay", "@axisbank",
]
PSPS = _expand("PSP", [
    "PhonePe", "Paytm", "GooglePay", "BHIM", "AmazonPay",
    "AxisPSP", "HDFCPSP", "ICICIPSP", "SBIUPI", "YesPSP",
    "JioPSP", "AirtelPSP", "MobiKwikPSP", "FreechargePSP",
])

# ---------------------------------------------------------------------------
# Transaction categories and protocol-sensitive code sets
# ---------------------------------------------------------------------------

PAYMENT_CHANNELS = [
    "UPI", "UPI_QR", "UPI_INTENT", "UPI_COLLECT", "UPI_P2P",
    "UPI_P2M", "UPI_AUTOPAY", "UPI_LITE", "UPI_OFFLINE",
    "IMPS", "NEFT", "RTGS", "CARD", "NETC", "AEPS", "BBPS",
]
TRANSACTION_TYPES = [
    "P2P", "P2M", "COLLECT", "PAY", "REFUND", "REVERSAL",
    "MANDATE_CREATE", "MANDATE_EXECUTE", "MANDATE_REVOKE",
    "BALANCE_ENQUIRY", "ACCOUNT_VERIFY", "CASH_WITHDRAWAL",
]
PAYMENT_FLOWS = [
    "PUSH", "PULL", "COLLECT_REQUEST", "INTENT", "QR",
    "MANDATE", "REFUND", "REVERSAL",
]
INTEGRATION_MODES = [
    "API", "SDK", "DEEPLINK", "INTENT", "QR_SCAN", "WEBVIEW",
    "HOSTED_PAGE", "BATCH", "INTERNAL_SERVICE",
]

# These are project test categories; validate actual codes against your spec.
PURPOSE_CODES = [
    "00", "01", "02", "03", "04", "05", "06", "07",
    "08", "09", "10", "11", "12", "13", "14", "15",
]
INITIATION_MODES = ["00", "01", "02", "11"]

CURRENCIES = [
    "INR", "USD", "EUR", "GBP", "AED", "SGD", "AUD", "CAD",
    "JPY", "CHF", "CNY", "NZD", "SAR", "QAR", "MYR", "THB",
]
COUNTRIES = [
    "India", "United States", "United Kingdom", "Singapore",
    "United Arab Emirates", "Australia", "Canada", "Germany",
    "France", "Japan", "Malaysia", "Nepal", "Sri Lanka", "Bangladesh",
    "Bhutan", "Thailand", "Indonesia", "Philippines", "South Africa",
]

# ---------------------------------------------------------------------------
# Telecom, network and geography
# ---------------------------------------------------------------------------

TELECOM_OPERATORS = [
    "Jio", "Airtel", "Vodafone Idea", "BSNL", "MTNL",
    "Unknown", "WiFi-only", "SyntheticOperator",
]
NETWORK_TYPES = ["2G", "3G", "4G", "5G", "WIFI", "ETHERNET", "UNKNOWN"]
ZONES = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL", "NORTH_EAST"]
STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar",
    "Chhattisgarh", "Goa", "Gujarat", "Haryana", "Himachal Pradesh",
    "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh",
    "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland",
    "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
    "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand",
    "West Bengal", "Delhi", "Jammu and Kashmir", "Ladakh",
    "Puducherry", "Chandigarh", "Andaman and Nicobar Islands",
    "Lakshadweep", "Dadra and Nagar Haveli and Daman and Diu",
]
CITIES = [
    "Kolkata", "Mumbai", "Delhi", "Bengaluru", "Chennai", "Hyderabad",
    "Pune", "Ahmedabad", "Jaipur", "Lucknow", "Kanpur", "Nagpur",
    "Indore", "Bhopal", "Patna", "Ranchi", "Bhubaneswar", "Guwahati",
    "Kochi", "Thiruvananthapuram", "Coimbatore", "Visakhapatnam",
    "Surat", "Vadodara", "Nashik", "Noida", "Gurugram", "Faridabad",
    "Siliguri", "Durgapur", "Asansol", "Howrah", "Mysuru", "Mangaluru",
    "Vijayawada", "Warangal", "Dehradun", "Amritsar", "Jodhpur",
    "Udaipur", "Raipur", "Jamshedpur", "Imphal", "Shillong", "Agartala",
]
CITIES = _expand("SyntheticCity", CITIES)

# ---------------------------------------------------------------------------
# Customer, account and merchant reference values
# ---------------------------------------------------------------------------

CUSTOMER_TYPES = [
    "INDIVIDUAL", "SOLE_PROPRIETOR", "SMALL_BUSINESS", "CORPORATE",
    "GOVERNMENT", "NON_PROFIT", "TRUST", "PARTNERSHIP",
    "PRIVATE_LIMITED", "PUBLIC_LIMITED", "UNKNOWN",
]
ACCOUNT_TYPES = [
    "SAVINGS", "CURRENT", "SALARY", "NRE", "NRO", "OD",
    "CASH_CREDIT", "WALLET", "PAYMENTS_BANK", "SMALL_FINANCE",
    "JOINT", "UNKNOWN",
]
MERCHANT_TYPES = [
    "GROCERY", "E_COMMERCE", "FOOD_DELIVERY", "RESTAURANT",
    "FUEL", "TRAVEL", "UTILITIES", "EDUCATION", "HEALTHCARE",
    "GOVERNMENT", "ENTERTAINMENT", "GAMING", "SUBSCRIPTION",
    "RETAIL", "TRANSPORT", "DONATION", "INSURANCE", "FINANCIAL_SERVICES",
    "OTHER",
]
MERCHANT_SIZES = ["MICRO", "SMALL", "MEDIUM", "LARGE", "ENTERPRISE", "UNKNOWN"]

# MCC codes are numeric strings used for synthetic examples.
# Validate against the MCC directory before treating any value as authoritative.
MCC_CODES = [
    "5411", "5422", "5441", "5451", "5462", "5499", "5541", "5542",
    "4111", "4121", "4131", "4511", "4722", "4812", "4814", "4829",
    "4899", "4900", "5013", "5021", "5039", "5045", "5065", "5072",
    "5094", "5111", "5122", "5192", "5200", "5211", "5231", "5251",
    "5261", "5300", "5311", "5331", "5399", "5411", "5511", "5521",
    "5531", "5532", "5533", "5611", "5621", "5631", "5641", "5651",
    "5655", "5661", "5691", "5699", "5732", "5734", "5735", "5812",
    "5813", "5814", "5912", "5921", "5941", "5942", "5943", "5944",
    "5945", "5947", "5948", "5949", "5964", "5965", "5967", "5968",
    "5969", "5977", "5992", "5993", "5999", "6012", "6051", "6211",
    "6300", "7011", "7210", "7311", "7399", "7523", "7538", "7542",
    "7832", "7911", "7994", "7995", "8011", "8021", "8099", "8220",
    "8299", "8398", "8641", "8999", "9399", "9402",
]

# ---------------------------------------------------------------------------
# Authentication, decisions, risk, fraud and processing
# ---------------------------------------------------------------------------

AUTH_METHODS = [
    "UPI_PIN", "DEVICE_BINDING", "OTP", "BIOMETRIC", "MPIN",
    "PASSWORD", "TOKEN", "APP_APPROVAL", "NONE", "UNKNOWN",
]
AUTH_STATUSES = [
    "SUCCESS", "FAILED", "PENDING", "LOCKED", "EXPIRED",
    "NOT_REQUIRED", "CHALLENGE_REQUIRED", "UNKNOWN",
]
PROCESSING_STATUSES = [
    "RECEIVED", "VALIDATED", "AUTHENTICATING", "AUTHORIZED",
    "DECLINED", "PROCESSING", "SUCCESS", "FAILED", "PENDING",
    "TIMEOUT", "REVERSED", "REFUNDED", "DUPLICATE", "RETRYING",
    "QUEUED", "UNDER_REVIEW", "UNKNOWN",
]
DECISIONS = [
    "ALLOW", "BLOCK", "CHALLENGE", "REVIEW", "STEP_UP",
    "DECLINE", "RETRY", "HOLD",
]
RISK_LEVELS = ["VERY_LOW", "LOW", "MEDIUM", "HIGH", "VERY_HIGH", "CRITICAL"]
FRAUD_TYPES = [
    "NONE", "ACCOUNT_TAKEOVER", "SOCIAL_ENGINEERING", "PHISHING",
    "SIM_SWAP", "MULE_ACCOUNT", "DEVICE_SPOOFING", "VELOCITY_ABUSE",
    "UNUSUAL_LOCATION", "SUSPICIOUS_BENEFICIARY", "DUPLICATE_TRANSACTION",
    "PAYMENT_REQUEST_FRAUD", "CREDENTIAL_COMPROMISE", "BOT_ACTIVITY",
    "SYNTHETIC_IDENTITY", "REFUND_ABUSE", "ATO", "UNKNOWN",
]

SERVER_REGIONS = [
    "ap-south-1", "ap-south-2", "ap-southeast-1", "ap-southeast-2",
    "ap-northeast-1", "ap-northeast-2", "eu-west-1", "eu-central-1",
    "eu-west-2", "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "me-central-1", "af-south-1", "on-prem-kolkata", "on-prem-mumbai",
    "on-prem-delhi", "on-prem-bengaluru",
]
SERVICE_NAMES = [
    "upi-gateway", "upi-switch", "efrm-online-processor",
    "efrm-offline-processor", "risk-engine", "fraud-scoring-service",
    "transaction-validator", "device-intelligence-service",
    "customer-profile-service", "account-verification-service",
    "beneficiary-service", "notification-service", "case-management-service",
    "datalake-consumer", "kafka-subscriber", "reporting-service",
    "merchant-service", "authentication-service", "audit-service",
]
PROCESSOR_TYPES = [
    "ONLINE", "OFFLINE", "STREAMING", "BATCH", "REAL_TIME",
    "ASYNC", "RETRY", "RECONCILIATION", "SIMULATOR",
]

# ---------------------------------------------------------------------------
# Expand categories where a large synthetic entity space is useful.
# These are explicitly synthetic variants, not additional official codes.
# ---------------------------------------------------------------------------

# App package IDs are synthetic after the known seed entries.
APPS = _expand("in.synthetic.payment.app", APPS)

# Keep APP_NAMES aligned in length with APPS.
if len(APP_NAMES) < len(APPS):
    APP_NAMES = _expand("SyntheticPaymentApp", APP_NAMES, len(APPS))

# Generate at least 1000 unique device models / service instances / profiles.
DEVICE_MODELS = _expand("SYNTH_DEVICE_MODEL", DEVICE_MODELS)
SERVICE_NAMES = _expand("synthetic-service", SERVICE_NAMES)
SERVER_REGIONS = _expand("synthetic-region", SERVER_REGIONS)
TELECOM_OPERATORS = _expand("SyntheticTelecomOperator", TELECOM_OPERATORS)
PSPS = _expand("SyntheticPSP", PSPS)
CITIES = _expand("SyntheticCity", CITIES)

# BANKS are expanded for load testing only; synthetic entries are not real banks.
BANKS = _expand("SYNTHETIC_BANK", BANKS)

# ---------------------------------------------------------------------------
# Spark schema
# ---------------------------------------------------------------------------

def build_upi_efrm_schema():
    """
    Build a starter schema for transaction-level synthetic data.
    Adjust fields/types to match the columns emitted by your generator.
    Importing this module does not require PySpark.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, DoubleType,
        TimestampType, BooleanType, IntegerType
    )

    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("app_package", StringType(), True),
        StructField("app_name", StringType(), True),
        StructField("os_name", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("device_brand", StringType(), True),
        StructField("device_model", StringType(), True),
        StructField("bank_name", StringType(), True),
        StructField("upi_handle", StringType(), True),
        StructField("psp_name", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_flow", StringType(), True),
        StructField("integration_mode", StringType(), True),
        StructField("purpose_code", StringType(), True),
        StructField("initiation_mode", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True),
        StructField("telecom_operator", StringType(), True),
        StructField("network_type", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("customer_type", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("merchant_type", StringType(), True),
        StructField("merchant_size", StringType(), True),
        StructField("mcc_code", StringType(), True),
        StructField("auth_method", StringType(), True),
        StructField("auth_status", StringType(), True),
        StructField("processing_status", StringType(), True),
        StructField("decision", StringType(), True),
        StructField("risk_level", StringType(), True),
        StructField("fraud_type", StringType(), True),
        StructField("server_region", StringType(), True),
        StructField("service_name", StringType(), True),
        StructField("processor_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("transaction_velocity_5m", IntegerType(), True),
        StructField("response_code", StringType(), True),
        StructField("response_message", StringType(), True),
        StructField("processing_latency_ms", LongType(), True),
    ])


UPI_EFRM_SCHEMA = build_upi_efrm_schema()

# Validate imports and report category sizes when executed directly.
if __name__ == "__main__":
    _names = [
        "APPS", "APP_NAMES", "OS_LIST", "ANDROID_VERSIONS", "IOS_VERSIONS",
        "DEVICE_TYPES", "DEVICE_BRANDS", "DEVICE_MODELS", "BANKS", "HANDLES",
        "PSPS", "PAYMENT_CHANNELS", "TRANSACTION_TYPES", "PAYMENT_FLOWS",
        "INTEGRATION_MODES", "PURPOSE_CODES", "INITIATION_MODES", "CURRENCIES",
        "COUNTRIES", "TELECOM_OPERATORS", "NETWORK_TYPES", "ZONES", "STATES",
        "CITIES", "CUSTOMER_TYPES", "ACCOUNT_TYPES", "MERCHANT_TYPES",
        "MERCHANT_SIZES", "MCC_CODES", "AUTH_METHODS", "AUTH_STATUSES",
        "PROCESSING_STATUSES", "DECISIONS", "RISK_LEVELS", "FRAUD_TYPES",
        "SERVER_REGIONS", "SERVICE_NAMES", "PROCESSOR_TYPES", "UPI_EFRM_SCHEMA",
    ]
    for _name in _names:
        _value = globals()[_name]
        print(f"{_name:24} {len(_value) if hasattr(_value, '__len__') else 'schema'}")
