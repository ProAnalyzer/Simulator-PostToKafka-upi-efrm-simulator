import argparse
import json
import random
import time
import threading
import uuid
import socket
import hashlib
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ==========================================
# 1. GEOGRAPHIC CONSISTENCY MATRIX
# ==========================================
# Each city has: (state, zone, language, lat_range, lon_range, pincode_range)
GEO_MATRIX = {
    "MUMBAI":        ("MAHARASHTRA",      "WEST",      "mr_IN", (18.9, 19.3), (72.7, 73.1), (400001, 400100)),
    "PUNE":          ("MAHARASHTRA",      "WEST",      "mr_IN", (18.4, 18.7), (73.7, 74.0), (411001, 411060)),
    "NAGPUR":        ("MAHARASHTRA",      "CENTRAL",   "mr_IN", (21.0, 21.3), (79.0, 79.3), (440001, 440030)),
    "DELHI":         ("DELHI",            "NORTH",     "hi_IN", (28.5, 28.9), (77.0, 77.4), (110001, 110099)),
    "NEW_DELHI":     ("DELHI",            "NORTH",     "hi_IN", (28.5, 28.9), (77.0, 77.4), (110001, 110099)),
    "BENGALURU":     ("KARNATAKA",        "SOUTH",     "en_IN", (12.8, 13.2), (77.4, 77.8), (560001, 560100)),
    "CHENNAI":       ("TAMIL_NADU",       "SOUTH",     "ta_IN", (12.9, 13.3), (80.1, 80.4), (600001, 600100)),
    "HYDERABAD":     ("TELANGANA",        "SOUTH",     "te_IN", (17.2, 17.6), (78.3, 78.7), (500001, 500100)),
    "KOLKATA":       ("WEST_BENGAL",      "EAST",      "bn_IN", (22.4, 22.8), (88.2, 88.6), (700001, 700150)),
    "AHMEDABAD":     ("GUJARAT",          "WEST",      "gu_IN", (22.9, 23.3), (72.4, 72.8), (380001, 380060)),
    "SURAT":         ("GUJARAT",          "WEST",      "gu_IN", (21.0, 21.4), (72.7, 73.1), (395001, 395030)),
    "JAIPUR":        ("RAJASTHAN",        "NORTH",     "hi_IN", (26.7, 27.1), (75.6, 76.0), (302001, 302040)),
    "LUCKNOW":       ("UTTAR_PRADESH",    "NORTH",     "hi_IN", (26.7, 27.0), (80.8, 81.1), (226001, 226030)),
    "NOIDA":         ("UTTAR_PRADESH",    "NORTH",     "hi_IN", (28.4, 28.7), (77.2, 77.5), (201301, 201310)),
    "PATNA":         ("BIHAR",            "EAST",      "hi_IN", (25.5, 25.8), (85.0, 85.3), (800001, 800030)),
    "GUWAHATI":      ("ASSAM",            "NORTHEAST", "en_IN", (26.0, 26.4), (91.6, 91.9), (781001, 781040)),
    "BHUBANESWAR":   ("ODISHA",           "EAST",      "en_IN", (20.1, 20.5), (85.7, 86.0), (751001, 751030)),
    "KOCHI":         ("KERALA",           "SOUTH",     "en_IN", (9.8, 10.2),  (76.1, 76.5), (682001, 682040)),
    "INDORE":        ("MADHYA_PRADESH",   "CENTRAL",   "hi_IN", (22.6, 22.9), (75.7, 76.0), (452001, 452020)),
    "BHOPAL":        ("MADHYA_PRADESH",   "CENTRAL",   "hi_IN", (23.1, 23.4), (77.2, 77.6), (462001, 462040)),
    "RAIPUR":        ("CHHATTISGARH",     "CENTRAL",   "hi_IN", (21.1, 21.4), (81.8, 82.1), (492001, 492020)),
    "RANCHI":        ("JHARKHAND",        "EAST",      "hi_IN", (23.2, 23.5), (85.2, 85.5), (834001, 834010)),
    "DEHRADUN":      ("UTTARAKHAND",      "NORTH",     "hi_IN", (30.2, 30.5), (78.0, 78.3), (248001, 248010)),
    "AMRITSAR":      ("PUNJAB",           "NORTH",     "hi_IN", (31.5, 31.8), (74.8, 75.1), (143001, 143010)),
    "GURUGRAM":      ("HARYANA",          "NORTH",     "hi_IN", (28.3, 28.6), (76.9, 77.2), (122001, 122020)),
    "VISAKHAPATNAM": ("ANDHRA_PRADESH",   "SOUTH",     "te_IN", (17.6, 17.9), (83.1, 83.4), (530001, 530040)),
    "COIMBATORE":    ("TAMIL_NADU",       "SOUTH",     "ta_IN", (10.9, 11.2), (76.8, 77.1), (641001, 641050)),
    "WARANGAL":      ("TELANGANA",        "SOUTH",     "te_IN", (17.8, 18.1), (79.4, 79.7), (506001, 506010)),
    "SILIGURI":      ("WEST_BENGAL",      "EAST",      "bn_IN", (26.6, 26.9), (88.3, 88.6), (734001, 734010)),
    "JODHPUR":       ("RAJASTHAN",        "NORTH",     "hi_IN", (26.1, 26.4), (72.9, 73.2), (342001, 342010)),
    "GAYA":          ("BIHAR",            "EAST",      "hi_IN", (24.6, 24.9), (84.8, 85.1), (823001, 823010)),
}

# ==========================================
# 2. BANK → VPA HANDLE STRICT MAPPING
# ==========================================
BANK_HANDLE_MAP = {
    "HDFC": ["@okhdfcbank", "@ybl"],
    "ICIC": ["@okicici", "@ibl"],
    "SBIN": ["@oksbi"],
    "UTIB": ["@okaxis", "@axl"],
    "KKBK": ["@ybl"],
    "YESB": ["@okyes"],
    "PUNB": ["@ybl"],
    "CNRB": ["@okcanara"],
    "BARB": ["@okbob"],
    "BKID": ["@ybl"],
    "UBIN": ["@okunion"],
    "IDIB": ["@ibl"],
    "IOBA": ["@ybl"],
    "FDRL": ["@ybl"],
    "INDB": ["@ybl"],
    "IDFB": ["@ybl"],
    "AUBL": ["@ybl"],
    "RBLB": ["@ybl"],
    "JIOP": ["@jio", "@ybl"],
}

BANKS = [
    ("HDFC", "HDFC Bank", "PRIVATE"), ("ICIC", "ICICI Bank", "PRIVATE"),
    ("SBIN", "State Bank of India", "PUBLIC"), ("UTIB", "Axis Bank", "PRIVATE"),
    ("KKBK", "Kotak Mahindra Bank", "PRIVATE"), ("YESB", "YES BANK", "PRIVATE"),
    ("PUNB", "Punjab National Bank", "PUBLIC"), ("CNRB", "Canara Bank", "PUBLIC"),
    ("BARB", "Bank of Baroda", "PUBLIC"), ("BKID", "Bank of India", "PUBLIC"),
    ("UBIN", "Union Bank of India", "PUBLIC"), ("IDIB", "Indian Bank", "PUBLIC"),
    ("IOBA", "Indian Overseas Bank", "PUBLIC"), ("FDRL", "Federal Bank", "PRIVATE"),
    ("INDB", "IndusInd Bank", "PRIVATE"), ("IDFB", "IDFC FIRST Bank", "PRIVATE"),
    ("AUBL", "AU Small Finance Bank", "SMALL_FINANCE"), ("RBLB", "RBL Bank", "PRIVATE"),
    ("JIOP", "Jio Payments Bank", "PAYMENTS"),
]

# ==========================================
# 3. MERCHANT TYPE → MCC CODE MAPPING
# ==========================================
MERCHANT_MCC_MAP = {
    "GROCERY":       ["5411", "5412"],
    "RESTAURANT":    ["5812", "5813", "5814"],
    "FUEL":          ["5541", "5542"],
    "E_COMMERCE":    ["5999", "5311", "5312"],
    "TRAVEL":        ["4722", "4723", "7011"],
    "EDUCATION":     ["8299", "8220"],
    "HEALTHCARE":    ["8011", "8099", "5912"],
    "UTILITY":       ["4900"],
    "RETAIL":        ["5331", "5311", "5699"],
    "ENTERTAINMENT": ["7922", "7941", "7832"],
    "PHARMACY":      ["5912"],
    "FOOD_DELIVERY": ["5812", "5813"],
    "TRANSPORT":     ["4111", "4121", "4131"],
    "HOTEL":         ["7011"],
}

# ==========================================
# 4. DEVICE PROFILES (OS → Brand → Model)
# ==========================================
DEVICE_PROFILES = {
    "Android": {
        "SAMSUNG":  ["Galaxy A15", "Galaxy A35", "Galaxy A55", "Galaxy S23", "Galaxy S24"],
        "XIAOMI":   ["Redmi Note 13", "Redmi Note 14", "Xiaomi 14"],
        "ONEPLUS":  ["OnePlus 11", "OnePlus 12", "OnePlus Nord 4"],
        "OPPO":     ["Oppo Reno 11", "Oppo Find X7"],
        "VIVO":     ["Vivo V30", "Vivo X100"],
        "REALME":   ["Realme 12", "Realme GT 5"],
        "MOTOROLA": ["Moto G84", "Moto Edge 50"],
        "GOOGLE":   ["Pixel 8", "Pixel 9"],
        "NOTHING":  ["Nothing Phone 2", "Nothing Phone 2a"],
        "POCO":     ["POCO X6", "POCO F6"],
    },
    "iOS": {
        "APPLE": ["iPhone 14", "iPhone 15", "iPhone 16", "iPhone 16 Pro"],
    },
}

# ==========================================
# 5. APP PACKAGE → APP NAME (1:1)
# ==========================================
APP_MAP = {
    "com.phonepe.app": "PhonePe",
    "net.one97.paytm": "Paytm",
    "in.org.npci.upiapp": "BHIM",
    "com.google.android.apps.nbu.paisa.user": "Google Pay",
    "in.amazon.mShop.android.shopping": "Amazon Pay",
    "com.mobikwik_new": "MobiKwik",
    "com.freecharge.android": "Freecharge",
    "com.csam.icici.bank.imobile": "iMobile",
    "com.axis.mobile": "Axis Mobile",
    "com.sbi.lotusintouch": "YONO SBI",
    "com.dbs.in.digitalbank": "DBS digibank",
}

# ==========================================
# 6. USER POOL (Persistent user profiles)
# ==========================================
class UserPool:
    """Creates persistent user profiles with consistent attributes."""
    def __init__(self, size=5000):
        self.users = {}
        self.size = size
        for i in range(size):
            user_id = f"P{i:08d}"
            city = random.choice(list(GEO_MATRIX.keys()))
            state, zone, lang, lat_r, lon_r, pin_r = GEO_MATRIX[city]
            bank = random.choice(BANKS)
            handle = random.choice(BANK_HANDLE_MAP[bank[0]])
            
            # Device persistence
            os_choice = "Android" if random.random() < 0.85 else "iOS"
            brand = random.choice(list(DEVICE_PROFILES[os_choice].keys()))
            model = random.choice(DEVICE_PROFILES[os_choice][brand])
            device_id = hashlib.sha256(f"device-{user_id}".encode()).hexdigest()[:32]
            device_fp = hashlib.sha256(f"fp-{user_id}".encode()).hexdigest()
            
            # Account age (most users are established)
            account_age = int(random.lognormvariate(6.5, 1.2))  # Median ~660 days
            account_age = min(max(account_age, 30), 3500)
            
            # User behavior profile
            daily_txn_mean = random.lognormvariate(1.5, 0.8)  # Median ~4.5 txns/day
            daily_txn_mean = min(max(daily_txn_mean, 0.5), 50)
            
            self.users[user_id] = {
                "user_id": user_id,
                "home_city": city,
                "home_state": state,
                "home_zone": zone,
                "home_language": lang,
                "home_lat_range": lat_r,
                "home_lon_range": lon_r,
                "home_pincode_range": pin_r,
                "bank": bank,
                "vpa_handle": handle,
                "vpa": f"{user_id.lower()}{handle}",
                "account_age_days": account_age,
                "account_type": random.choices(
                    ["SAVINGS", "CURRENT", "SALARY", "NRE", "NRO"],
                    weights=[0.70, 0.10, 0.15, 0.03, 0.02]
                )[0],
                "customer_type": random.choices(
                    ["INDIVIDUAL", "MERCHANT", "SMALL_BUSINESS", "CORPORATE"],
                    weights=[0.92, 0.04, 0.03, 0.01]
                )[0],
                "device_id": device_id,
                "device_fingerprint": device_fp,
                "device_os": os_choice,
                "device_brand": brand,
                "device_model": model,
                "device_os_version": random.choice(
                    ["12", "13", "14", "15", "16"] if os_choice == "Android" else ["16", "17", "18"]
                ),
                "device_first_seen_days_ago": account_age + random.randint(0, 30),
                "mobile_operator": random.choices(
                    ["JIO", "AIRTEL", "VI", "BSNL"], weights=[0.42, 0.32, 0.22, 0.04]
                )[0],
                "daily_txn_mean": daily_txn_mean,
                "is_high_risk_user": random.random() < 0.005,  # 0.5% high-risk users
                "psp": random.choice(["PHONEPE", "GOOGLEPAY", "PAYTM", "AMAZONPAY", "BHARATPE", "CRED"]),
            }
    
    def get(self, user_id):
        if user_id not in self.users:
            return self.get(random.choice(list(self.users.keys())))
        return self.users[user_id]

# ==========================================
# 7. MERCHANT POOL
# ==========================================
class MerchantPool:
    def __init__(self, size=2000):
        self.merchants = {}
        for i in range(size):
            mid = f"MER{i:06d}"
            mtype = random.choices(
                ["GROCERY", "RESTAURANT", "FUEL", "E_COMMERCE", "TRAVEL", "EDUCATION",
                 "HEALTHCARE", "UTILITY", "RETAIL", "ENTERTAINMENT", "PHARMACY",
                 "FOOD_DELIVERY", "TRANSPORT", "HOTEL"],
                weights=[0.18, 0.12, 0.08, 0.10, 0.04, 0.03, 0.05, 0.06, 0.12, 0.04, 0.04, 0.08, 0.03, 0.03]
            )[0]
            city = random.choice(list(GEO_MATRIX.keys()))
            state, zone, lang, lat_r, lon_r, pin_r = GEO_MATRIX[city]
            bank = random.choice(BANKS)
            handle = random.choice(BANK_HANDLE_MAP[bank[0]])
            
            self.merchants[mid] = {
                "merchant_id": mid,
                "merchant_type": mtype,
                "mcc": random.choice(MERCHANT_MCC_MAP[mtype]),
                "city": city,
                "state": state,
                "zone": zone,
                "pincode": random.randint(pin_r[0], pin_r[1]),
                "bank": bank,
                "vpa": f"m{mid.lower()}{handle}",
                "size": random.choices(
                    ["MICRO", "SMALL", "MEDIUM", "LARGE", "ENTERPRISE"],
                    weights=[0.45, 0.30, 0.15, 0.07, 0.03]
                )[0],
                "account_age_days": int(random.lognormvariate(6, 1.5)),
                "risk_score": round(random.uniform(0, 30) + (70 if random.random() < 0.01 else 0), 2),
                "is_suspicious": random.random() < 0.01,  # 1% suspicious merchants
            }
    
    def get(self, mid=None):
        if mid and mid in self.merchants:
            return self.merchants[mid]
        return random.choice(list(self.merchants.values()))

# ==========================================
# 8. TEMPORAL PATTERN GENERATOR
# ==========================================
def generate_realistic_timestamp():
    """Generate timestamp with realistic UPI hourly distribution (IST)."""
    now = datetime.now(timezone.utc)
    ist_now = now + timedelta(hours=5, minutes=30)
    
    # Realistic hourly weights for UPI (IST)
    # Peak: 9-11 AM, 6-9 PM. Low: 1-5 AM
    hourly_weights = {
        0: 0.01, 1: 0.005, 2: 0.003, 3: 0.003, 4: 0.005, 5: 0.015,
        6: 0.03, 7: 0.05, 8: 0.07, 9: 0.09, 10: 0.09, 11: 0.07,
        12: 0.06, 13: 0.05, 14: 0.05, 15: 0.05, 16: 0.05, 17: 0.06,
        18: 0.08, 19: 0.09, 20: 0.08, 21: 0.06, 22: 0.04, 23: 0.02,
    }
    
    hours = list(hourly_weights.keys())
    weights = list(hourly_weights.values())
    ist_hour = random.choices(hours, weights=weights)[0]
    ist_minute = random.randint(0, 59)
    ist_second = random.randint(0, 59)
    
    # Build IST datetime, then convert to UTC
    ist_dt = ist_now.replace(hour=ist_hour, minute=ist_minute, second=ist_second, microsecond=0)
    utc_dt = ist_dt - timedelta(hours=5, minutes=30)
    
    # If UTC is in future (due to time-of-day wrap), shift back 1 day
    if utc_dt > now:
        utc_dt -= timedelta(days=1)
    
    return utc_dt, ist_dt, ist_hour

# ==========================================
# 9. CAUSAL RISK & FRAUD ENGINE
# ==========================================
def compute_risk_and_fraud(user, payee_info, amount, ist_hour, velocity_1h, geo_distance, is_new_beneficiary):
    """Compute risk score causally from features, then derive fraud label."""
    
    # Base risk from user profile
    risk = 5.0
    
    # Time risk (late night is riskier)
    if ist_hour in [0, 1, 2, 3, 4]:
        risk += 8.0
    elif ist_hour in [22, 23]:
        risk += 3.0
    
    # Amount risk
    if amount > 50000:
        risk += 12.0
    elif amount > 20000:
        risk += 5.0
    elif amount > 10000:
        risk += 2.0
    
    # Velocity risk
    if velocity_1h > 20:
        risk += 20.0
    elif velocity_1h > 10:
        risk += 10.0
    elif velocity_1h > 5:
        risk += 3.0
    
    # Geographic risk
    if geo_distance > 500:
        risk += 15.0
    elif geo_distance > 100:
        risk += 5.0
    
    # New beneficiary risk
    if is_new_beneficiary:
        risk += 6.0
    
    # Account age risk
    if user["account_age_days"] < 30:
        risk += 10.0
    elif user["account_age_days"] < 90:
        risk += 3.0
    
    # High-risk user boost
    if user["is_high_risk_user"]:
        risk += 35.0
    
    # Suspicious merchant boost
    if payee_info.get("is_suspicious"):
        risk += 25.0
    if payee_info.get("risk_score", 0) > 70:
        risk += 15.0
    
    # Add noise (real risk engines have some randomness)
    risk += random.uniform(-5, 5)
    risk = max(0, min(100, risk))
    
    # Fraud probability based on risk score (realistic: 0.5-2% overall)
    if risk > 85:
        fraud_prob = 0.70
    elif risk > 70:
        fraud_prob = 0.35
    elif risk > 50:
        fraud_prob = 0.08
    elif risk > 30:
        fraud_prob = 0.01
    else:
        fraud_prob = 0.001
    
    is_fraud = 1 if random.random() < fraud_prob else 0
    
    # Determine fraud type based on the actual risk factors
    if is_fraud:
        if velocity_1h > 15 and user["account_age_days"] < 90:
            fraud_type = "VELOCITY_ABUSE"
        elif geo_distance > 300 and is_new_beneficiary:
            fraud_type = "LOCATION_ANOMALY"
        elif user["account_age_days"] < 60:
            fraud_type = "ACCOUNT_TAKEOVER"
        elif payee_info.get("is_suspicious"):
            fraud_type = "SUSPICIOUS_MERCHANT"
        elif amount > 30000:
            fraud_type = "SOCIAL_ENGINEERING"
        else:
            fraud_type = random.choice(["MULE_ACCOUNT", "DEVICE_COMPROMISE", "SUSPICIOUS_BENEFICIARY"])
    else:
        fraud_type = "NONE"
    
    return round(risk, 2), is_fraud, fraud_type

# ==========================================
# 10. STATUS/DECISION CONSISTENCY ENGINE
# ==========================================
def compute_decision_and_status(risk_score, is_fraud):
    """Derive decision and status causally from risk score."""
    
    # Decision based on risk score
    if risk_score >= 80:
        decision = random.choices(["DECLINE", "REVIEW", "CHALLENGE"], weights=[0.75, 0.20, 0.05])[0]
    elif risk_score >= 60:
        decision = random.choices(["REVIEW", "CHALLENGE", "DECLINE", "ALLOW"], weights=[0.40, 0.35, 0.10, 0.15])[0]
    elif risk_score >= 40:
        decision = random.choices(["CHALLENGE", "ALLOW", "REVIEW"], weights=[0.50, 0.40, 0.10])[0]
    elif risk_score >= 20:
        decision = random.choices(["ALLOW", "CHALLENGE"], weights=[0.92, 0.08])[0]
    else:
        decision = "ALLOW"
    
    # Processing status based on decision
    if decision == "DECLINE":
        processing_status = "FAILURE"
        response_code = random.choice(["FR", "U30"])
    elif decision == "REVIEW":
        processing_status = random.choices(["SUCCESS", "PENDING", "FAILURE"], weights=[0.55, 0.30, 0.15])[0]
        if processing_status == "SUCCESS":
            response_code = "00"
        elif processing_status == "PENDING":
            response_code = "91"
        else:
            response_code = random.choice(["U30", "U16", "U17"])
    elif decision == "CHALLENGE":
        processing_status = random.choices(["SUCCESS", "FAILURE"], weights=[0.82, 0.18])[0]
        response_code = "00" if processing_status == "SUCCESS" else random.choice(["U16", "U17", "U28"])
    else:  # ALLOW
        processing_status = random.choices(["SUCCESS", "PENDING", "FAILURE"], weights=[0.95, 0.03, 0.02])[0]
        if processing_status == "SUCCESS":
            response_code = "00"
        elif processing_status == "PENDING":
            response_code = "91"
        else:
            response_code = random.choice(["U01", "U30"])
    
    # Small chance of technical failure regardless
    if random.random() < 0.008:
        processing_status = "FAILURE"
        response_code = random.choice(["U01", "U16", "U17", "U28"])
    
    # Auth status consistent with processing
    if processing_status == "SUCCESS":
        auth_status = random.choices(["SUCCESS", "NOT_REQUIRED"], weights=[0.85, 0.15])[0]
    elif processing_status == "FAILURE":
        auth_status = random.choices(["FAILED", "SUCCESS", "NOT_REQUIRED"], weights=[0.55, 0.30, 0.15])[0]
    else:
        auth_status = random.choices(["SUCCESS", "FAILED", "NOT_REQUIRED"], weights=[0.50, 0.30, 0.20])[0]
    
    return decision, processing_status, response_code, auth_status

# ==========================================
# 11. MAIN EVENT GENERATOR
# ==========================================
class RealisticEventGenerator:
    def __init__(self):
        self.user_pool = UserPool(size=5000)
        self.merchant_pool = MerchantPool(size=2000)
        self.user_txn_counts = defaultdict(int)  # Track per-user velocity
    
    def generate_event(self):
        # 1. Generate realistic timestamp
        utc_dt, ist_dt, ist_hour = generate_realistic_timestamp()
        timestamp = utc_dt.isoformat().replace('+00:00', 'Z')
        
        # Time period from IST hour
        if 5 <= ist_hour <= 8: time_period = "EARLY_MORNING"
        elif 9 <= ist_hour <= 12: time_period = "MORNING"
        elif 13 <= ist_hour <= 16: time_period = "AFTERNOON"
        elif 17 <= ist_hour <= 20: time_period = "EVENING"
        elif 21 <= ist_hour <= 23: time_period = "NIGHT"
        else: time_period = "LATE_NIGHT"
        
        unusual_time_flag = ist_hour in [0, 1, 2, 3, 4]
        is_business_hour = (9 <= ist_hour <= 18) and (ist_dt.weekday() < 5)
        is_weekend = ist_dt.weekday() >= 5
        
        # 2. Select payer (user)
        payer = random.choice(list(self.user_pool.users.values()))
        
        # 3. Select payee (user or merchant)
        is_p2m = random.random() < 0.55  # 55% P2M, 45% P2P
        if is_p2m:
            payee_merchant = self.merchant_pool.get()
            payee_user = random.choice(list(self.user_pool.users.values()))
            payee_city = payee_merchant["city"]
            payee_state = payee_merchant["state"]
            payee_zone = payee_merchant["zone"]
            payee_pincode = payee_merchant["pincode"]
            payee_lat = random.uniform(*GEO_MATRIX[payee_city][3])
            payee_lon = random.uniform(*GEO_MATRIX[payee_city][4])
            payee_vpa = payee_merchant["vpa"]
            payee_bank = payee_merchant["bank"]
            payee_psp = random.choice(["PHONEPE", "GOOGLEPAY", "PAYTM", "AMAZONPAY", "BHARATPE"])
        else:
            payee_user = random.choice(list(self.user_pool.users.values()))
            payee_merchant = None
            payee_city = payee_user["home_city"]
            payee_state = payee_user["home_state"]
            payee_zone = payee_user["home_zone"]
            payee_pincode = random.randint(*payee_user["home_pincode_range"])
            payee_lat = random.uniform(*payee_user["home_lat_range"])
            payee_lon = random.uniform(*payee_user["home_lon_range"])
            payee_vpa = payee_user["vpa"]
            payee_bank = payee_user["bank"]
            payee_psp = payee_user["psp"]
        
        # 4. Payer location (usually home, sometimes traveling)
        if random.random() < 0.85:  # 85% at home
            payer_city = payer["home_city"]
            payer_state = payer["home_state"]
            payer_zone = payer["home_zone"]
            payer_lat = random.uniform(*payer["home_lat_range"])
            payer_lon = random.uniform(*payer["home_lon_range"])
            payer_pincode = random.randint(*payer["home_pincode_range"])
        else:  # 15% traveling
            payer_city = random.choice(list(GEO_MATRIX.keys()))
            payer_state, payer_zone, _, lat_r, lon_r, pin_r = GEO_MATRIX[payer_city]
            payer_lat = random.uniform(*lat_r)
            payer_lon = random.uniform(*lon_r)
            payer_pincode = random.randint(*pin_r)
        
        # 5. Compute geo distance
        geo_distance = round(((payer_lat - payee_lat)**2 + (payer_lon - payee_lon)**2)**0.5 * 111, 2)
        location_mismatch = geo_distance > 200 and random.random() < 0.7
        
        # 6. Amount (realistic log-normal distribution)
        if is_p2m:
            amount = round(min(max(random.lognormvariate(5.0, 1.3), 10), 100000), 2)
        else:
            amount = round(min(max(random.lognormvariate(5.5, 1.5), 10), 200000), 2)
        
        # 7. Velocity (per-user tracking)
        self.user_txn_counts[payer["user_id"]] += 1
        velocity_1h = min(self.user_txn_counts[payer["user_id"]], 50)
        velocity_24h = int(velocity_1h * random.uniform(3, 12))
        
        # 8. New beneficiary flag
        is_new_beneficiary = payee_user["account_age_days"] < 14
        
        # 9. Causal risk scoring
        risk_score, is_fraud, fraud_type = compute_risk_and_fraud(
            payer, payee_merchant if payee_merchant else payee_user,
            amount, ist_hour, velocity_1h, geo_distance, is_new_beneficiary
        )
        
        # 10. Derive decision and status
        decision, processing_status, response_code, auth_status = compute_decision_and_status(risk_score, is_fraud)
        
        # 11. Risk level from score
        if risk_score >= 75: risk_level = "CRITICAL"
        elif risk_score >= 50: risk_level = "HIGH"
        elif risk_score >= 25: risk_level = "MEDIUM"
        else: risk_level = "LOW"
        
        # 12. Build risk flags (causally)
        risk_flags = []
        if unusual_time_flag: risk_flags.append("UNUSUAL_TIME")
        if amount > 50000: risk_flags.append("HIGH_AMOUNT")
        if velocity_1h > 15: risk_flags.append("HIGH_VELOCITY")
        if location_mismatch: risk_flags.append("LOCATION_MISMATCH")
        if payer["account_age_days"] < 30: risk_flags.append("NEW_DEVICE")
        if is_new_beneficiary: risk_flags.append("NEW_BENEFICIARY")
        if random.random() < 0.008: risk_flags.append("VPN_DETECTED")
        if random.random() < 0.004: risk_flags.append("PROXY_DETECTED")
        if velocity_1h > 10 and velocity_1h < 15: risk_flags.append("RAPID_SUCCESSION")
        
        # 13. Transaction type distribution
        if is_p2m:
            txn_type = random.choices(
                ["P2M", "COLLECT", "REFUND", "MANDATE"],
                weights=[0.70, 0.15, 0.10, 0.05]
            )[0]
        else:
            txn_type = random.choices(
                ["P2P", "P2A", "REVERSAL"],
                weights=[0.85, 0.10, 0.05]
            )[0]
        
        # 14. App selection (correlated with user)
        app_pkg = random.choice(list(APP_MAP.keys()))
        app_name = APP_MAP[app_pkg]
        
        # 15. Build event
        event = {
            # Core IDs
            "txn_id": f"TXN{uuid.uuid4().hex[:14].upper()}",
            "txn_ref_id": f"REF{uuid.uuid4().hex[:10].upper()}",
            "org_txn_id": f"ORG{uuid.uuid4().hex[:14].upper()}",
            "parent_txn_id": None,
            "rrn": f"{random.randint(100000000000, 999999999999)}",
            "message_id": f"MSG{uuid.uuid4().hex[:16].upper()}",
            "request_id": f"REQ{uuid.uuid4().hex[:16].upper()}",
            "session_id": f"SES{uuid.uuid4().hex[:14].upper()}",
            "customer_ref_id": f"CUS{random.randint(1000000000, 9999999999)}",
            "merchant_ref_id": payee_merchant["merchant_id"] if payee_merchant else None,
            
            # Timestamp (realistic distribution)
            "txn_timestamp": timestamp,
            "txn_date": ist_dt.strftime("%Y-%m-%d"),
            "txn_year": ist_dt.year,
            "txn_month": ist_dt.month,
            "txn_day": ist_dt.day,
            "txn_hour": ist_hour,
            "txn_minute": ist_dt.minute,
            "txn_second": ist_dt.second,
            "day_of_week": ist_dt.weekday() + 1,
            "week_of_year": ist_dt.isocalendar()[1],
            "quarter": (ist_dt.month - 1) // 3 + 1,
            "is_weekend": is_weekend,
            "is_holiday": random.random() < 0.035,
            "is_business_hour": is_business_hour,
            "time_period": time_period,
            "festival_flag": random.random() < 0.06,
            "salary_day_flag": ist_dt.day in [1, 2, 3, 5, 28, 29, 30, 31],
            "month_start_flag": ist_dt.day <= 3,
            "month_end_flag": ist_dt.day >= 28,
            
            # Payer (consistent user profile)
            "payer_id": payer["user_id"],
            "payer_vpa": payer["vpa"],
            "payer_bank_code": payer["bank"][0],
            "payer_bank_name": payer["bank"][1],
            "payer_ifsc": f"{payer['bank'][0]}000{random.randint(1000, 9999)}",
            "payer_account_type": payer["account_type"],
            "payer_customer_type": payer["customer_type"],
            "payer_psp": payer["psp"],
            "payer_mobile_hash": hashlib.sha256(f"mobile-{payer['user_id']}".encode()).hexdigest(),
            
            # Payee
            "payee_id": payee_user["user_id"],
            "payee_vpa": payee_vpa,
            "payee_bank_code": payee_bank[0],
            "payee_bank_name": payee_bank[1],
            "payee_ifsc": f"{payee_bank[0]}000{random.randint(1000, 9999)}",
            "payee_account_type": payee_user["account_type"],
            "payee_customer_type": payee_user["customer_type"],
            "payee_psp": payee_psp,
            
            # Merchant (only for P2M)
            "merchant_id": payee_merchant["merchant_id"] if payee_merchant else None,
            "merchant_vpa": payee_merchant["vpa"] if payee_merchant else None,
            "merchant_type": payee_merchant["merchant_type"] if payee_merchant else None,
            "merchant_size": payee_merchant["size"] if payee_merchant else None,
            "merchant_mcc": payee_merchant["mcc"] if payee_merchant else None,
            "merchant_state": payee_merchant["state"] if payee_merchant else None,
            "merchant_city": payee_merchant["city"] if payee_merchant else None,
            "merchant_pincode": str(payee_merchant["pincode"]) if payee_merchant else None,
            "merchant_account_age_days": payee_merchant["account_age_days"] if payee_merchant else None,
            
            # App/Device (consistent per user)
            "app": app_pkg,
            "app_name": app_name,
            "app_version": f"{random.randint(5, 12)}.{random.randint(0, 9)}.{random.randint(0, 19)}",
            "device_id": payer["device_id"],
            "device_fingerprint": payer["device_fingerprint"],
            "device_type": f"{payer['device_os'].upper()}_PHONE" if random.random() < 0.95 else f"{payer['device_os'].upper()}_TABLET",
            "device_brand": payer["device_brand"],
            "device_model": payer["device_model"],
            "device_os": payer["device_os"],
            "device_os_version": payer["device_os_version"],
            "device_language": payer["home_language"],
            "device_timezone": "Asia/Kolkata",
            "device_rooted": random.random() < 0.003,
            "device_emulator": random.random() < 0.001,
            "device_first_seen_days_ago": payer["device_first_seen_days_ago"],
            "device_last_seen_minutes_ago": random.randint(0, 10080),
            
            # Network
            "mobile_operator": payer["mobile_operator"],
            "network_type": random.choices(["4G", "5G", "WIFI", "3G"], weights=[0.40, 0.38, 0.18, 0.04])[0],
            "ip_address": f"{random.randint(10,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "ip_country": "IN",
            "ip_region": payer_state,
            "ip_city": payer_city,
            "ip_risk_score": round(min(risk_score * 0.6 + random.uniform(0, 20), 100), 2),
            "vpn_flag": "VPN_DETECTED" in risk_flags,
            "proxy_flag": "PROXY_DETECTED" in risk_flags,
            
            # Geography (consistent)
            "payer_country": "IN",
            "payer_state": payer_state,
            "payer_city": payer_city,
            "payer_pincode": str(payer_pincode),
            "payee_country": "IN",
            "payee_state": payee_state,
            "payee_city": payee_city,
            "payee_pincode": str(payee_pincode),
            "payer_latitude": round(payer_lat, 6),
            "payer_longitude": round(payer_lon, 6),
            "payee_latitude": round(payee_lat, 6),
            "payee_longitude": round(payee_lon, 6),
            "geo_distance_km": geo_distance,
            "location_mismatch": location_mismatch,
            
            # Amount
            "amount": amount,
            "requested_amount": amount,
            "settled_amount": amount if processing_status == "SUCCESS" else (amount if random.random() < 0.02 else 0.0),
            "refund_amount": amount if txn_type == "REFUND" and processing_status == "SUCCESS" else 0.0,
            "fee_amount": round(amount * 0.0015, 2),
            "tax_amount": round(amount * 0.0015 * 0.18, 2),
            "discount_amount": round(amount * 0.01, 2) if random.random() < 0.08 else 0.0,
            "cashback_amount": round(amount * 0.005, 2) if random.random() < 0.04 else 0.0,
            "currency": "INR",
            
            # Transaction
            "transaction_type": txn_type,
            "payment_flow": random.choices(["PAY", "COLLECT", "MANDATE", "REFUND", "REVERSAL"], weights=[0.75, 0.12, 0.05, 0.05, 0.03])[0],
            "payment_channel": random.choices(["UPI_APP", "QR", "INTENT", "COLLECT", "IN_APP", "WEB", "BANK_APP"], weights=[0.42, 0.25, 0.12, 0.08, 0.07, 0.03, 0.03])[0],
            "integration_mode": random.choices(["QR", "INTENT", "COLLECT", "APP"], weights=[0.40, 0.28, 0.20, 0.12])[0],
            "purpose_code": random.choice(["00", "01", "02"]),
            "initiation_mode": random.choice(["00", "01", "02", "11"]),
            
            # Authentication
            "authentication_method": random.choice(["UPI_PIN", "BIOMETRIC", "DEVICE_AUTH", "OTP"]),
            "authentication_status": auth_status,
            "authentication_attempt_count": random.randint(1, 3),
            "authentication_failure_count": random.randint(0, 2) if auth_status == "FAILED" else 0,
            "pin_retry_count": random.randint(0, 3) if auth_status == "FAILED" else 0,
            "device_binding_status": random.choices(["BOUND", "REBOUND", "FAILED"], weights=[0.97, 0.025, 0.005])[0],
            "mobile_binding_status": random.choices(["BOUND", "FAILED"], weights=[0.99, 0.01])[0],
            
            # Velocity
            "txn_count_1m": min(velocity_1h, 5),
            "txn_count_5m": min(velocity_1h, 10),
            "txn_count_15m": min(velocity_1h, 15),
            "txn_count_30m": min(velocity_1h, 20),
            "txn_count_1h": velocity_1h,
            "txn_count_6h": velocity_24h,
            "txn_count_24h": velocity_24h,
            "amount_1h": round(amount * max(1, velocity_1h / 2), 2),
            "amount_6h": round(amount * max(1, velocity_24h / 3), 2),
            "amount_24h": round(amount * max(1, velocity_24h / 4), 2),
            "unique_payees_1h": min(velocity_1h, random.randint(1, 5)),
            "unique_payees_24h": min(velocity_24h, random.randint(1, 15)),
            "unique_devices_24h": 1 if random.random() < 0.95 else 2,
            "unique_ips_24h": 1 if random.random() < 0.90 else random.randint(2, 3),
            "unique_banks_24h": random.randint(1, 3),
            
            # Account history
            "payer_account_age_days": payer["account_age_days"],
            "payer_txn_count_24h": velocity_24h,
            "payer_txn_count_7d": velocity_24h * random.randint(3, 7),
            "payer_amount_24h": round(amount * velocity_24h * 0.8, 2),
            "payer_amount_7d": round(amount * velocity_24h * 5, 2),
            "payee_account_age_days": payee_user["account_age_days"],
            "payee_txn_count_24h": random.randint(0, 200),
            "payee_amount_24h": round(random.uniform(0, 300000), 2),
            "merchant_txn_count_24h": random.randint(100, 5000) if payee_merchant else None,
            "merchant_amount_24h": round(random.uniform(100000, 5000000), 2) if payee_merchant else None,
            "merchant_risk_score": payee_merchant["risk_score"] if payee_merchant else None,
            
            # Risk flags
            "new_device_flag": payer["account_age_days"] < 7,
            "new_beneficiary_flag": is_new_beneficiary,
            "high_amount_flag": amount > 50000,
            "high_velocity_flag": velocity_1h > 15,
            "unusual_time_flag": unusual_time_flag,
            "unusual_amount_flag": amount > 100000,
            "multiple_accounts_device_flag": random.random() < 0.02,
            "multiple_devices_account_flag": random.random() < 0.01,
            "suspicious_ip_flag": "VPN_DETECTED" in risk_flags or "PROXY_DETECTED" in risk_flags,
            
            # Risk scores (causally derived)
            "risk_score": risk_score,
            "rule_score": round(min(risk_score * 0.92 + random.uniform(0, 8), 100), 2),
            "ml_score": round(min((risk_score / 100) * 0.95 + random.uniform(0, 0.05), 1.0), 5),
            "velocity_score": round(min(velocity_1h * 2.5, 100), 2),
            "device_score": round(random.uniform(0, 30) + (40 if payer["device_first_seen_days_ago"] < 7 else 0), 2),
            "location_score": 85.0 if location_mismatch else round(random.uniform(5, 25), 2),
            "merchant_score": payee_merchant["risk_score"] if payee_merchant else round(random.uniform(5, 20), 2),
            "customer_score": round(max(0, 100 - risk_score + random.uniform(-10, 10)), 2),
            "network_score": round(random.uniform(0, 30) + (40 if "VPN_DETECTED" in risk_flags else 0), 2),
            "risk_level": risk_level,
            "fraud_type": fraud_type,
            "rule_triggered": risk_score >= 50,
            "rule_id": "EFRM_RISK_075" if risk_score >= 75 else ("EFRM_RISK_050" if risk_score >= 50 else ("EFRM_VELOCITY_001" if velocity_1h > 15 else None)),
            "rule_count": random.randint(1, 4) if risk_score >= 50 else 0,
            
            # Processing (causally consistent)
            "processing_status": processing_status,
            "response_code": response_code,
            "error_code": response_code if response_code != "00" else None,
            "timeout_flag": processing_status == "TIMEOUT",
            "retry_flag": processing_status in ["TIMEOUT", "PENDING"],
            "retry_count": random.randint(1, 3) if processing_status in ["TIMEOUT", "PENDING"] else 0,
            "reprocessed_flag": processing_status in ["TIMEOUT", "PENDING"],
            "decision": decision,
            "decision_reason": "HIGH_RISK" if risk_score >= 75 else ("RULE_TRIGGERED" if risk_score >= 50 else ("ADDITIONAL_AUTHENTICATION" if risk_score >= 30 else "NORMAL")),
            "is_fraud": is_fraud,
            "is_suspicious": 1 if (risk_score > 60 or random.random() < 0.02) else 0,
            "is_declined": 1 if decision == "DECLINE" else 0,
            "is_reversed": 0,
            "is_manual_review": 1 if decision == "REVIEW" else 0,
            
            # Infrastructure
            "online_offline_flag": random.choices(["ONLINE", "OFFLINE"], weights=[0.94, 0.06])[0],
            "processor_type": "ONLINE",
            "processing_node": f"NODE-{random.randint(1, 64)}",
            "server_region": random.choice(["EAST", "WEST", "NORTH", "SOUTH"]),
            "availability_zone": f"{random.choice(['EAST', 'WEST', 'NORTH', 'SOUTH'])}-AZ{random.randint(1, 3)}",
            "service_name": random.choice(["upi-gateway", "upi-switch", "rule-engine", "datalake-consumer", "notification-service", "case-management-service", "model-engine"]),
            "service_version": f"3.{random.randint(0, 4)}.0",
            "kafka_topic": "UPI_TXN_EVENT_TOPIC",
            "kafka_partition": random.randint(0, 255),
            "kafka_offset": random.randint(0, 1000000),
            "consumer_group": "upi-risk-consumer",
            "consumer_instance": f"consumer-{random.randint(1, 64)}",
            "event_lag_ms": random.randint(0, 5000),
            "processing_latency_ms": random.randint(5, 155),
            "queue_latency_ms": random.randint(0, 100),
            "database_latency_ms": random.randint(2, 82),
            "total_latency_ms": random.randint(7, 5337),
            "yugabyte_flag": True,
            "cassandra_flag": random.random() < 0.35,
            "hive_flag": random.random() < 0.06,
            "elasticsearch_flag": random.random() < 0.80,
            "simulator_thread": 0,
        }
        
        return event


# ==========================================
# Worker Thread (Asynchronous & Non-Blocking)
# ==========================================

generator = RealisticEventGenerator()

def worker_thread(thread_id, producer, topic, stop_event, start_time, duration, tps, num_threads, stats):
    sleep_duration = num_threads / tps if tps > 0 else 0
    count = 0
    first_send_logged = False
    
    while not stop_event.is_set() and (time.monotonic() - start_time) < duration:
        loop_start = time.monotonic()
        event = generator.generate_event()
        event['simulator_thread'] = thread_id
        
        try:
            # NON-BLOCKING SEND: Let the Kafka background I/O thread handle batching and delivery.
            producer.send(topic, value=event)
            count += 1
            
            if not first_send_logged:
                print(f"[Thread {thread_id}] Successfully queued first message to Kafka background buffer.")
                first_send_logged = True
                
        except BufferError:
            print(f"[Thread {thread_id}] Kafka buffer is full. Broker may be slow or unreachable.")
            stop_event.set()
            break
        except Exception as e:
            print(f"[Thread {thread_id}] Unexpected send error: {e}")
            stop_event.set()
            break
            
        elapsed = time.monotonic() - loop_start
        sleep_time = sleep_duration - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
            
    stats[thread_id] = count

# ==========================================
# Main Execution
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="Enriched UPI/eFRM Synthetic Data Simulator & Kafka Datapump")
    parser.add_argument("-t", "--time", type=int, default=60, help="Duration to run the simulator in seconds (default: 60)")
    parser.add_argument("-th", "--threads", type=int, default=4, help="Number of concurrent threads (default: 4)")
    parser.add_argument("-r", "--tps", type=int, default=100, help="Target transactions per second (total across all threads, default: 100)")
    parser.add_argument("--topic", type=str, default="upi-efrm-txn", help="Kafka topic to publish to (default: upi-efrm-events)")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers (default: localhost:9092)")
    
    args = parser.parse_args()
    
    print(f"Starting Enriched UPI Simulator...")
    print(f"Target TPS: {args.tps} | Threads: {args.threads} | Duration: {args.time}s")
    print(f"Kafka Topic: {args.topic} | Servers: {args.bootstrap_servers}")
    print("-" * 50)
    
    # Pre-flight connectivity check
    host_port = args.bootstrap_servers.split(',')
    host = host_port[0].split(':')[0]
    port = int(host_port[0].split(':')[1]) if ':' in host_port[0] else 9092
    
    try:
        with socket.create_connection((host, port), timeout=3):
            print("Successfully connected to Kafka broker port.")
    except (ConnectionRefusedError, socket.timeout, OSError):
        print(f"CRITICAL: Cannot connect to Kafka at {host}:{port}")
        print("Please ensure Kafka is running and accessible before starting the simulator.")
        return

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(','),
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
            acks='all',
            linger_ms=5,              
            batch_size=32768,         
            compression_type='gzip',  # <-- CHANGED: 'gzip' is built into Python and works on Windows without extra C-libraries For faster you can use "snappy"
            max_block_ms=5000,        
            request_timeout_ms=5000,
            retries=3
        )
        
    except NoBrokersAvailable:
        print("CRITICAL: No brokers available. Check your Kafka configuration.")
        return

    stop_event = threading.Event()
    stats = {}
    threads = []
    start_time = time.monotonic()
    
    for i in range(args.threads):
        t = threading.Thread(target=worker_thread, args=(i, producer, args.topic, stop_event, start_time, args.time, args.tps, args.threads, stats))
        t.start()
        threads.append(t)
        
    try:
        while time.monotonic() - start_time < args.time and not stop_event.is_set():
            time.sleep(1)
            elapsed = time.monotonic() - start_time
            total_sent = sum(stats.values())
            current_tps = total_sent / elapsed if elapsed > 0 else 0
            print(f"[{elapsed:.1f}s] Total Queued: {total_sent} | Current Avg TPS: {current_tps:.1f}")
    except KeyboardInterrupt:
        print("\nInterrupted by user. Stopping...")
        stop_event.set()
        
    for t in threads:
        t.join()
        
    print("Flushing remaining messages to Kafka broker...")
    try:
        producer.flush(timeout=30)
        print("Flush successful.")
    except Exception as e:
        print(f"Flush failed: {e}")
    finally:
        producer.close()
        print("Producer Closed")
    
    total_sent = sum(stats.values())
    actual_duration = time.monotonic() - start_time
    final_tps = total_sent / actual_duration if actual_duration > 0 else 0
    
    print("-" * 50)
    print(f"Simulation Complete!")
    print(f"Total Transactions Queued: {total_sent}")
    print(f"Actual Duration: {actual_duration:.2f}s")
    print(f"Average TPS: {final_tps:.1f}")

if __name__ == "__main__":
    main()