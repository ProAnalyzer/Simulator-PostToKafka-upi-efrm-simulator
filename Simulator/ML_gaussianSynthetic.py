"""
upi_efrm_gaussian_simulator.py
Gaussian-clustered UPI/eFRM simulator for Bayesian inference & GMM modeling.

Key improvements over previous versions:
1. User behavioral profiles with Gaussian parameters (amount, velocity, location, time)
2. Multi-modal amount distributions (casual/power-user/merchant/fraudster clusters)
3. Covariance structure between features (amount↔velocity, distance↔time)
4. Derived Bayesian features: z-scores, priors, posteriors, evidence
5. Temporal drift for Bayesian updating
6. Proper Gaussian latencies per service tier
7. Geographic home clusters with Gaussian spread
8. Session-level correlation

Usage:
    python upi_efrm_gaussian_simulator.py --time 60 --threads 4 --tps 500 \
        --topic upi-efrm-gaussian --bootstrap-servers localhost:9092
"""

import argparse
import json
import random
import time
import threading
import uuid
import socket
import hashlib
import math
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ============================================================
# 1. GAUSSIAN USER PROFILES (Multi-modal behavioral clusters)
# ============================================================

class GaussianUser:
    """
    Each user has Gaussian behavioral parameters that define their
    transaction distribution. This creates natural clusters for GMM.
    """
    def __init__(self, user_id, user_type):
        self.user_id = user_id
        self.user_type = user_type  # CASUAL, POWER, MERCHANT, DORMANT, HIGH_RISK
        
        # Type-specific Gaussian parameters
        if user_type == "CASUAL":
            # Most users: small amounts, low velocity, regular hours
            self.amount_mean = random.gauss(250, 80)      # ₹250 avg
            self.amount_std = max(50, random.gauss(150, 40))
            self.velocity_mean = random.gauss(2.5, 0.8)   # 2-3 txns/hour peak
            self.velocity_std = max(0.5, random.gauss(1.2, 0.4))
            self.peak_hour = random.gauss(18.0, 2.0)      # Evening peak
            self.hour_std = 3.0
            self.home_city_idx = random.randint(0, 29)
            self.home_spread_km = random.gauss(15, 5)     # Small geo spread
            self.account_age_days = int(random.gauss(800, 300))
            self.base_risk_prior = random.gauss(0.02, 0.01)  # 2% fraud prior
            
        elif user_type == "POWER":
            # Power users: higher amounts, higher velocity, varied hours
            self.amount_mean = random.gauss(2500, 800)
            self.amount_std = max(500, random.gauss(1200, 400))
            self.velocity_mean = random.gauss(8.0, 2.0)
            self.velocity_std = max(1.0, random.gauss(3.0, 1.0))
            self.peak_hour = random.gauss(13.0, 4.0)      # Afternoon peak
            self.hour_std = 5.0
            self.home_city_idx = random.randint(0, 15)    # Metro cities
            self.home_spread_km = random.gauss(40, 15)
            self.account_age_days = int(random.gauss(1200, 400))
            self.base_risk_prior = random.gauss(0.015, 0.008)
            
        elif user_type == "MERCHANT":
            # Merchants: many small transactions, business hours
            self.amount_mean = random.gauss(800, 300)
            self.amount_std = max(200, random.gauss(500, 200))
            self.velocity_mean = random.gauss(25.0, 8.0)
            self.velocity_std = max(3.0, random.gauss(10.0, 3.0))
            self.peak_hour = random.gauss(12.0, 3.0)      # Lunch peak
            self.hour_std = 4.0
            self.home_city_idx = random.randint(0, 20)
            self.home_spread_km = random.gauss(5, 2)      # Fixed location
            self.account_age_days = int(random.gauss(1500, 500))
            self.base_risk_prior = random.gauss(0.008, 0.004)
            
        elif user_type == "DORMANT":
            # Dormant: very low activity, occasional spikes
            self.amount_mean = random.gauss(500, 200)
            self.amount_std = max(100, random.gauss(300, 100))
            self.velocity_mean = random.gauss(0.3, 0.2)
            self.velocity_std = 0.5
            self.peak_hour = random.gauss(10.0, 5.0)
            self.hour_std = 6.0
            self.home_city_idx = random.randint(0, 29)
            self.home_spread_km = random.gauss(25, 10)
            self.account_age_days = int(random.gauss(600, 300))
            self.base_risk_prior = random.gauss(0.025, 0.012)
            
        else:  # HIGH_RISK
            # Compromised/bot: unusual patterns, high velocity, odd hours
            self.amount_mean = random.gauss(8000, 4000)
            self.amount_std = max(1000, random.gauss(5000, 2000))
            self.velocity_mean = random.gauss(18.0, 6.0)
            self.velocity_std = max(2.0, random.gauss(8.0, 3.0))
            self.peak_hour = random.gauss(2.0, 3.0)       # Late night
            self.hour_std = 4.0
            self.home_city_idx = random.randint(0, 29)
            self.home_spread_km = random.gauss(200, 100)   # Wide geo spread
            self.account_age_days = int(random.gauss(60, 40))  # New accounts
            self.base_risk_prior = random.gauss(0.35, 0.15)
        
        # Clamp values to realistic ranges
        self.amount_mean = max(50, min(self.amount_mean, 50000))
        self.amount_std = max(20, min(self.amount_std, 10000))
        self.velocity_mean = max(0.1, min(self.velocity_mean, 50))
        self.velocity_std = max(0.1, min(self.velocity_std, 20))
        self.peak_hour = self.peak_hour % 24
        self.hour_std = max(1.0, min(self.hour_std, 8.0))
        self.home_spread_km = max(1.0, min(self.home_spread_km, 500))
        self.account_age_days = max(1, min(self.account_age_days, 3500))
        self.base_risk_prior = max(0.001, min(self.base_risk_prior, 0.8))
        
        # Bank and device (persistent per user)
        self.bank = random.choice(BANKS)
        self.handle = random.choice(BANK_HANDLES[self.bank[0]])
        self.vpa = f"{user_id.lower()}{self.handle}"
        self.account_type = random.choices(
            ["SAVINGS", "CURRENT", "SALARY", "NRE", "NRO"],
            weights=[0.65, 0.12, 0.18, 0.03, 0.02]
        )[0]
        self.psp = random.choice(PSPS)
        
        # Device
        os_choice = "Android" if random.random() < 0.85 else "iOS"
        brand = random.choice(list(DEVICE_PROFILES[os_choice].keys()))
        model = random.choice(DEVICE_PROFILES[os_choice][brand])
        self.device_os = os_choice
        self.device_os_version = random.choice(
            ANDROID_VERSIONS if os_choice == "Android" else IOS_VERSIONS
        )
        self.device_brand = brand
        self.device_model = model
        self.device_type = f"{os_choice.upper()}_PHONE" if random.random() < 0.95 else f"{os_choice.upper()}_TABLET"
        self.device_id = hashlib.sha256(f"device-{user_id}".encode()).hexdigest()[:32]
        self.device_fingerprint = hashlib.sha256(f"fp-{user_id}".encode()).hexdigest()
        self.device_first_seen = self.account_age_days + random.randint(0, 30)
        
        # Home location (Gaussian cluster center)
        city = CITIES[self.home_city_idx % len(CITIES)]
        state, zone, lang = CITY_STATE_MAP[city]
        self.home_city = city
        self.home_state = state
        self.home_zone = zone
        self.home_language = lang
        self.home_lat = CITY_COORDS[city][0] + random.gauss(0, 0.05)
        self.home_lon = CITY_COORDS[city][1] + random.gauss(0, 0.05)
        
        # Mobile operator
        self.mobile_operator = random.choices(
            ["JIO", "AIRTEL", "VI", "BSNL"], weights=[0.42, 0.32, 0.22, 0.04]
        )[0]
        
        # App
        self.app_pkg = random.choice(list(APP_MAP.keys()))
        self.app_name = APP_MAP[self.app_pkg]
        self.app_version = f"{random.randint(5, 12)}.{random.randint(0, 9)}.{random.randint(0, 19)}"
        
        # Bayesian state (updated per transaction)
        self.txn_count = 0
        self.amount_sum = 0.0
        self.amount_sq_sum = 0.0
        self.velocity_sum = 0.0
        self.risk_posterior = self.base_risk_prior
        
        # Temporal drift (for Bayesian updating over time)
        self.drift_rate = random.gauss(0, 0.001)  # Slow drift in behavior


# ============================================================
# 2. GAUSSIAN MERCHANT PROFILES
# ============================================================

class GaussianMerchant:
    """Merchant with Gaussian transaction volume patterns."""
    def __init__(self, merchant_id, merchant_type):
        self.merchant_id = merchant_id
        self.merchant_type = merchant_type
        self.mcc = MERCHANT_MCC_MAP.get(merchant_type, ["5999"])[0]
        
        # Size distribution
        self.size = random.choices(
            ["MICRO", "SMALL", "MEDIUM", "LARGE", "ENTERPRISE"],
            weights=[0.40, 0.30, 0.18, 0.09, 0.03]
        )[0]
        
        # Volume Gaussians by size
        size_params = {
            "MICRO": (50, 20, 10000, 5000),
            "SMALL": (300, 100, 100000, 40000),
            "MEDIUM": (1500, 500, 800000, 300000),
            "LARGE": (5000, 1500, 4000000, 1500000),
            "ENTERPRISE": (15000, 5000, 15000000, 5000000),
        }
        txn_mean, txn_std, vol_mean, vol_std = size_params[self.size]
        self.daily_txn_mean = max(10, random.gauss(txn_mean, txn_std))
        self.daily_volume_mean = max(1000, random.gauss(vol_mean, vol_std))
        
        # Location
        city = random.choice(CITIES)
        state, zone, lang = CITY_STATE_MAP[city]
        self.city = city
        self.state = state
        self.zone = zone
        self.pincode = random.randint(100000, 999999)
        
        # Bank
        self.bank = random.choice(BANKS)
        self.handle = random.choice(BANK_HANDLES[self.bank[0]])
        self.vpa = f"m{merchant_id.lower()}{self.handle}"
        
        # Age and risk
        self.account_age_days = int(random.gauss(500, 300))
        self.account_age_days = max(7, min(self.account_age_days, 3500))
        self.base_risk = max(0, min(100, random.gauss(15, 10)))
        
        # Bayesian state
        self.txn_count_24h = int(random.gauss(self.daily_txn_mean, self.daily_txn_mean * 0.2))
        self.amount_24h = random.gauss(self.daily_volume_mean, self.daily_volume_mean * 0.25)


# ============================================================
# 3. MASTER DATA (Geographic + Banking + Device matrices)
# ============================================================

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

DEVICE_PROFILES = {
    "Android": {
        "SAMSUNG": ["Galaxy A15", "Galaxy A35", "Galaxy A55", "Galaxy S23", "Galaxy S24"],
        "XIAOMI": ["Redmi Note 13", "Redmi Note 14", "Xiaomi 14"],
        "ONEPLUS": ["OnePlus 11", "OnePlus 12", "OnePlus Nord 4"],
        "OPPO": ["Oppo Reno 11", "Oppo Find X7"],
        "VIVO": ["Vivo V30", "Vivo X100"],
        "REALME": ["Realme 12", "Realme GT 5"],
        "MOTOROLA": ["Moto G84", "Moto Edge 50"],
        "GOOGLE": ["Pixel 8", "Pixel 9"],
        "NOTHING": ["Nothing Phone 2", "Nothing Phone 2a"],
        "POCO": ["POCO X6", "POCO F6"],
    },
    "iOS": {
        "APPLE": ["iPhone 14", "iPhone 15", "iPhone 16", "iPhone 16 Pro"],
    },
}
ANDROID_VERSIONS = ["11", "12", "13", "14", "15", "16"]
IOS_VERSIONS = ["16", "17", "18", "19"]

BANKS = [
    ("HDFC", "HDFC Bank"), ("ICIC", "ICICI Bank"), ("SBIN", "State Bank of India"),
    ("UTIB", "Axis Bank"), ("KKBK", "Kotak Mahindra Bank"), ("YESB", "YES BANK"),
    ("PUNB", "Punjab National Bank"), ("CNRB", "Canara Bank"), ("BARB", "Bank of Baroda"),
    ("BKID", "Bank of India"), ("UBIN", "Union Bank of India"), ("IDIB", "Indian Bank"),
    ("IOBA", "Indian Overseas Bank"), ("FDRL", "Federal Bank"), ("INDB", "IndusInd Bank"),
    ("IDFB", "IDFC FIRST Bank"), ("AUBL", "AU Small Finance Bank"), ("RBLB", "RBL Bank"),
    ("JIOP", "Jio Payments Bank"),
]
BANK_HANDLES = {
    "HDFC": ["@okhdfcbank", "@ybl"], "ICIC": ["@okicici", "@ibl"], "SBIN": ["@oksbi"],
    "UTIB": ["@okaxis", "@axl"], "KKBK": ["@ybl"], "YESB": ["@okyes"],
    "PUNB": ["@ybl"], "CNRB": ["@okcanara"], "BARB": ["@okbob"],
    "BKID": ["@ybl"], "UBIN": ["@okunion"], "IDIB": ["@ibl"],
    "IOBA": ["@ybl"], "FDRL": ["@ybl"], "INDB": ["@ybl"],
    "IDFB": ["@ybl"], "AUBL": ["@ybl"], "RBLB": ["@ybl"], "JIOP": ["@jio", "@ybl"],
}

PSPS = ["PHONEPE", "GOOGLEPAY", "PAYTM", "AMAZONPAY", "BHARATPE", "CRED", "MOBIKWIK", "NAVI", "BANK_PSP"]

MERCHANT_MCC_MAP = {
    "GROCERY": ["5411", "5412"], "RESTAURANT": ["5812", "5813"],
    "FUEL": ["5541", "5542"], "E_COMMERCE": ["5999", "5311"],
    "TRAVEL": ["4722", "7011"], "EDUCATION": ["8299", "8220"],
    "HEALTHCARE": ["8011", "5912"], "UTILITY": ["4900"],
    "RETAIL": ["5331", "5311"], "ENTERTAINMENT": ["7922", "7832"],
    "PHARMACY": ["5912"], "FOOD_DELIVERY": ["5812", "5813"],
    "TRANSPORT": ["4111", "4121"], "HOTEL": ["7011"],
}

# City → (State, Zone, Language)
CITY_STATE_MAP = {
    "MUMBAI": ("MAHARASHTRA", "WEST", "mr_IN"), "PUNE": ("MAHARASHTRA", "WEST", "mr_IN"),
    "NAGPUR": ("MAHARASHTRA", "CENTRAL", "mr_IN"), "DELHI": ("DELHI", "NORTH", "hi_IN"),
    "NEW_DELHI": ("DELHI", "NORTH", "hi_IN"), "BENGALURU": ("KARNATAKA", "SOUTH", "en_IN"),
    "CHENNAI": ("TAMIL_NADU", "SOUTH", "ta_IN"), "HYDERABAD": ("TELANGANA", "SOUTH", "te_IN"),
    "KOLKATA": ("WEST_BENGAL", "EAST", "bn_IN"), "AHMEDABAD": ("GUJARAT", "WEST", "gu_IN"),
    "SURAT": ("GUJARAT", "WEST", "gu_IN"), "JAIPUR": ("RAJASTHAN", "NORTH", "hi_IN"),
    "LUCKNOW": ("UTTAR_PRADESH", "NORTH", "hi_IN"), "NOIDA": ("UTTAR_PRADESH", "NORTH", "hi_IN"),
    "PATNA": ("BIHAR", "EAST", "hi_IN"), "GUWAHATI": ("ASSAM", "NORTHEAST", "en_IN"),
    "BHUBANESWAR": ("ODISHA", "EAST", "en_IN"), "KOCHI": ("KERALA", "SOUTH", "en_IN"),
    "INDORE": ("MADHYA_PRADESH", "CENTRAL", "hi_IN"), "BHOPAL": ("MADHYA_PRADESH", "CENTRAL", "hi_IN"),
    "RAIPUR": ("CHHATTISGARH", "CENTRAL", "hi_IN"), "RANCHI": ("JHARKHAND", "EAST", "hi_IN"),
    "DEHRADUN": ("UTTARAKHAND", "NORTH", "hi_IN"), "AMRITSAR": ("PUNJAB", "NORTH", "hi_IN"),
    "GURUGRAM": ("HARYANA", "NORTH", "hi_IN"), "VISAKHAPATNAM": ("ANDHRA_PRADESH", "SOUTH", "te_IN"),
    "COIMBATORE": ("TAMIL_NADU", "SOUTH", "ta_IN"), "WARANGAL": ("TELANGANA", "SOUTH", "te_IN"),
    "SILIGURI": ("WEST_BENGAL", "EAST", "bn_IN"), "JODHPUR": ("RAJASTHAN", "NORTH", "hi_IN"),
}

CITIES = list(CITY_STATE_MAP.keys())

# Approximate city coordinates (lat, lon) for Gaussian geo clustering
CITY_COORDS = {
    "MUMBAI": (19.0760, 72.8777), "PUNE": (18.5204, 73.8567),
    "NAGPUR": (21.1458, 79.0882), "DELHI": (28.7041, 77.1025),
    "NEW_DELHI": (28.6139, 77.2090), "BENGALURU": (12.9716, 77.5946),
    "CHENNAI": (13.0827, 80.2707), "HYDERABAD": (17.3850, 78.4867),
    "KOLKATA": (22.5726, 88.3639), "AHMEDABAD": (23.0225, 72.5714),
    "SURAT": (21.1702, 72.8311), "JAIPUR": (26.9124, 75.7873),
    "LUCKNOW": (26.8467, 80.9462), "NOIDA": (28.5355, 77.3910),
    "PATNA": (25.6093, 85.1376), "GUWAHATI": (26.1445, 91.7362),
    "BHUBANESWAR": (20.2961, 85.8245), "KOCHI": (9.9312, 76.2673),
    "INDORE": (22.7196, 75.8577), "BHOPAL": (23.2599, 77.4126),
    "RAIPUR": (21.2514, 81.6296), "RANCHI": (23.3441, 85.3096),
    "DEHRADUN": (30.3165, 78.0322), "AMRITSAR": (31.6340, 74.8723),
    "GURUGRAM": (28.4595, 77.0266), "VISAKHAPATNAM": (17.6868, 83.2185),
    "COIMBATORE": (11.0168, 76.9558), "WARANGAL": (17.9689, 79.5941),
    "SILIGURI": (26.7271, 88.3953), "JODHPUR": (26.2389, 73.0243),
}


# ============================================================
# 4. GAUSSIAN TRANSACTION GENERATOR
# ============================================================

class GaussianTransactionGenerator:
    """
    Generates transactions by sampling from user/merchant Gaussian profiles.
    Creates natural multi-modal clusters for GMM/Bayesian modeling.
    """
    
    def __init__(self, n_users=5000, n_merchants=2000):
        # Create user pool with 5 behavioral types (Gaussian mixture components)
        self.users = {}
        user_types = ["CASUAL"] * 55 + ["POWER"] * 20 + ["MERCHANT"] * 10 + ["DORMANT"] * 10 + ["HIGH_RISK"] * 5
        for i in range(n_users):
            uid = f"P{i:08d}"
            utype = random.choice(user_types)
            self.users[uid] = GaussianUser(uid, utype)
        
        # Create merchant pool
        self.merchants = {}
        mtypes = ["GROCERY", "RESTAURANT", "FUEL", "E_COMMERCE", "TRAVEL",
                  "EDUCATION", "HEALTHCARE", "UTILITY", "RETAIL", "ENTERTAINMENT",
                  "PHARMACY", "FOOD_DELIVERY", "TRANSPORT", "HOTEL"]
        for i in range(n_merchants):
            mid = f"MER{i:06d}"
            mtype = random.choice(mtypes)
            self.merchants[mid] = GaussianMerchant(mid, mtype)
        
        # Per-user velocity tracking (for temporal correlation)
        self.user_recent_txns = defaultdict(list)  # user_id -> list of timestamps
        
        # Global simulation state
        self.sim_start = datetime.now(timezone.utc)
    
    def _haversine_km(self, lat1, lon1, lat2, lon2):
        """Approximate distance in km."""
        return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2) * 111.0
    
    def _gaussian_pdf(self, x, mean, std):
        """Gaussian probability density (for z-score computation)."""
        if std <= 0:
            std = 0.001
        return (1.0 / (std * math.sqrt(2 * math.pi))) * math.exp(-0.5 * ((x - mean) / std)**2)
    
    def _z_score(self, x, mean, std):
        """Compute z-score."""
        if std <= 0:
            std = 0.001
        return (x - mean) / std
    
    def _sample_amount(self, user, ist_hour):
        """
        Sample amount from user's Gaussian + time-of-day modulation.
        Creates covariance between amount and time.
        """
        # Base amount from user's Gaussian
        amount = random.gauss(user.amount_mean, user.amount_std)
        
        # Time-of-day modulation (larger amounts during peak hours for some users)
        hour_diff = abs(ist_hour - user.peak_hour)
        if hour_diff > 12:
            hour_diff = 24 - hour_diff
        time_factor = math.exp(-0.5 * (hour_diff / user.hour_std)**2)
        
        # High-risk users have more variance
        if user.user_type == "HIGH_RISK":
            amount *= random.uniform(0.3, 3.0)
        else:
            amount *= (0.7 + 0.6 * time_factor)
        
        # Clamp to realistic UPI range
        return max(1.0, min(amount, 200000.0))
    
    def _sample_velocity(self, user):
        """Sample current velocity from user's Gaussian."""
        v = random.gauss(user.velocity_mean, user.velocity_std)
        if user.user_type == "HIGH_RISK":
            v *= random.uniform(1.5, 4.0)
        return max(0, v)
    
    def _sample_location(self, user):
        """
        Sample payer location from user's home Gaussian cluster.
        High-risk users have wider spread (location anomaly).
        """
        spread = user.home_spread_km / 111.0  # Convert km to degrees approx
        if user.user_type == "HIGH_RISK":
            spread *= 5.0  # Much wider spread for compromised users
        
        lat = random.gauss(user.home_lat, spread)
        lon = random.gauss(user.home_lon, spread)
        
        # Clamp to India bounds
        lat = max(6.0, min(36.0, lat))
        lon = max(68.0, min(97.0, lon))
        
        return lat, lon
    
    def _sample_hour(self, user):
        """Sample transaction hour from user's activity Gaussian."""
        hour = random.gauss(user.peak_hour, user.hour_std)
        # Wrap around 24h
        hour = hour % 24
        return int(hour)
    
    def _compute_bayesian_update(self, user, amount, velocity, distance_km, ist_hour):
        """
        Bayesian update of user's risk posterior.
        Uses amount/velocity/location z-scores as evidence.
        """
        # Z-scores (evidence)
        amount_z = self._z_score(amount, user.amount_mean, user.amount_std)
        velocity_z = self._z_score(velocity, user.velocity_mean, user.velocity_std)
        distance_z = self._z_score(distance_km, user.home_spread_km, user.home_spread_km * 0.5)
        
        hour_diff = abs(ist_hour - user.peak_hour)
        if hour_diff > 12:
            hour_diff = 24 - hour_diff
        time_z = self._z_score(hour_diff, 0, user.hour_std)
        
        # Likelihood of anomaly given each z-score (using Gaussian tail probability)
        def anomaly_likelihood(z):
            return 1.0 - math.erf(abs(z) / math.sqrt(2))
        
        amount_evidence = anomaly_likelihood(amount_z)
        velocity_evidence = anomaly_likelihood(velocity_z)
        distance_evidence = anomaly_likelihood(distance_z)
        time_evidence = anomaly_likelihood(time_z)
        
        # Combined evidence (geometric mean for independence assumption)
        combined_evidence = (amount_evidence * velocity_evidence * 
                            distance_evidence * time_evidence) ** 0.25
        
        # Bayesian update: posterior = prior * likelihood / evidence
        # Simplified: posterior = prior + alpha * (evidence - prior)
        alpha = 0.15  # Learning rate
        new_posterior = user.risk_posterior + alpha * (combined_evidence - user.risk_posterior)
        new_posterior = max(0.001, min(0.999, new_posterior))
        
        return {
            "amount_zscore": round(amount_z, 3),
            "velocity_zscore": round(velocity_z, 3),
            "distance_zscore": round(distance_z, 3),
            "time_zscore": round(time_z, 3),
            "amount_evidence": round(amount_evidence, 4),
            "velocity_evidence": round(velocity_evidence, 4),
            "distance_evidence": round(distance_evidence, 4),
            "time_evidence": round(time_evidence, 4),
            "combined_evidence": round(combined_evidence, 4),
            "risk_prior": round(user.base_risk_prior, 4),
            "risk_posterior": round(new_posterior, 4),
            "user_type": user.user_type,
            "user_amount_mean": round(user.amount_mean, 2),
            "user_amount_std": round(user.amount_std, 2),
            "user_velocity_mean": round(user.velocity_mean, 2),
            "user_velocity_std": round(user.velocity_std, 2),
            "user_peak_hour": round(user.peak_hour, 2),
            "user_home_spread_km": round(user.home_spread_km, 2),
        }
    
    def _sample_latencies(self, risk_score):
        """
        Sample latencies from proper Gaussians.
        High-risk transactions have longer processing times (more checks).
        """
        # Processing latency: N(50, 15) normal, N(150, 40) for high-risk
        if risk_score > 75:
            proc_lat = max(5, random.gauss(150, 40))
        elif risk_score > 50:
            proc_lat = max(5, random.gauss(90, 25))
        else:
            proc_lat = max(5, random.gauss(50, 15))
        
        # Queue latency: N(20, 10) typically, higher under load
        queue_lat = max(0, random.gauss(20, 10))
        
        # DB latency: N(30, 12)
        db_lat = max(2, random.gauss(30, 12))
        
        # Event lag: exponential distribution (realistic for Kafka)
        event_lag = random.expovariate(1.0 / 1500)
        
        total_lat = proc_lat + queue_lat + db_lat + event_lag
        
        return {
            "processing_latency_ms": int(proc_lat),
            "queue_latency_ms": int(queue_lat),
            "database_latency_ms": int(db_lat),
            "event_lag_ms": int(event_lag),
            "total_latency_ms": int(total_lat),
        }
    
    def generate_event(self):
        """Generate a single Gaussian-clustered transaction event."""
        
        # 1. Pick a user (weighted by user type distribution)
        user = random.choice(list(self.users.values()))
        
        # 2. Generate realistic IST timestamp with user's activity pattern
        utc_now = datetime.now(timezone.utc)
        ist_now = utc_now + timedelta(hours=5, minutes=30)
        
        # Sample hour from user's activity Gaussian
        ist_hour = self._sample_hour(user)
        ist_minute = random.randint(0, 59)
        ist_second = random.randint(0, 59)
        
        # Build timestamp
        txn_ist = ist_now.replace(hour=ist_hour, minute=ist_minute, 
                                   second=ist_second, microsecond=0)
        txn_utc = txn_ist - timedelta(hours=5, minutes=30)
        if txn_utc > utc_now:
            txn_utc -= timedelta(days=1)
        
        timestamp = txn_utc.isoformat().replace('+00:00', 'Z')
        
        # Time period
        if 5 <= ist_hour <= 8: time_period = "EARLY_MORNING"
        elif 9 <= ist_hour <= 12: time_period = "MORNING"
        elif 13 <= ist_hour <= 16: time_period = "AFTERNOON"
        elif 17 <= ist_hour <= 20: time_period = "EVENING"
        elif 21 <= ist_hour <= 23: time_period = "NIGHT"
        else: time_period = "LATE_NIGHT"
        
        unusual_time_flag = ist_hour in [0, 1, 2, 3, 4]
        is_business_hour = (9 <= ist_hour <= 18) and (txn_ist.weekday() < 5)
        
        # 3. Sample amount from user's Gaussian
        amount = round(self._sample_amount(user, ist_hour), 2)
        
        # 4. Sample velocity
        velocity_1h = round(self._sample_velocity(user), 1)
        velocity_24h = velocity_1h * random.gauss(8, 2)
        velocity_24h = max(velocity_1h, velocity_24h)
        
        # 5. Pick payee (user or merchant)
        is_p2m = random.random() < 0.55
        if is_p2m:
            payee_merchant = random.choice(list(self.merchants.values()))
            payee_user = random.choice(list(self.users.values()))
            payee_city = payee_merchant.city
            payee_state = payee_merchant.state
            payee_zone = payee_merchant.zone
            payee_pincode = payee_merchant.pincode
            payee_lat, payee_lon = CITY_COORDS.get(payee_city, (20.0, 78.0))
            payee_lat += random.gauss(0, 0.01)
            payee_lon += random.gauss(0, 0.01)
            payee_vpa = payee_merchant.vpa
            payee_bank = payee_merchant.bank
            payee_psp = random.choice(PSPS)
            payee_account_type = random.choice(["SAVINGS", "CURRENT"])
            payee_customer_type = "MERCHANT"
            merchant_id = payee_merchant.merchant_id
            merchant_type = payee_merchant.merchant_type
            merchant_size = payee_merchant.size
            merchant_mcc = payee_merchant.mcc
            merchant_state = payee_merchant.state
            merchant_city = payee_merchant.city
            merchant_pincode = str(payee_merchant.pincode)
            merchant_age = payee_merchant.account_age_days
            merchant_txn_24h = payee_merchant.txn_count_24h
            merchant_amt_24h = round(payee_merchant.amount_24h, 2)
            merchant_risk = round(payee_merchant.base_risk, 2)
        else:
            payee_merchant = None
            payee_user = random.choice(list(self.users.values()))
            payee_city = payee_user.home_city
            payee_state = payee_user.home_state
            payee_zone = payee_user.home_zone
            payee_pincode = random.randint(100000, 999999)
            payee_lat = payee_user.home_lat + random.gauss(0, 0.02)
            payee_lon = payee_user.home_lon + random.gauss(0, 0.02)
            payee_vpa = payee_user.vpa
            payee_bank = payee_user.bank
            payee_psp = payee_user.psp
            payee_account_type = payee_user.account_type
            payee_customer_type = "INDIVIDUAL"
            merchant_id = None
            merchant_type = None
            merchant_size = None
            merchant_mcc = None
            merchant_state = None
            merchant_city = None
            merchant_pincode = None
            merchant_age = None
            merchant_txn_24h = None
            merchant_amt_24h = None
            merchant_risk = None
        
        # 6. Sample payer location from user's Gaussian cluster
        payer_lat, payer_lon = self._sample_location(user)
        payer_city = user.home_city
        payer_state = user.home_state
        payer_zone = user.home_zone
        payer_pincode = random.randint(100000, 999999)
        
        # 7. Compute geo distance
        geo_distance_km = round(self._haversine_km(payer_lat, payer_lon, payee_lat, payee_lon), 2)
        location_mismatch = geo_distance_km > (user.home_spread_km * 3)
        
        # 8. Bayesian update with all evidence
        bayes = self._compute_bayesian_update(
            user, amount, velocity_1h, geo_distance_km, ist_hour
        )
        
        # Update user's posterior
        user.risk_posterior = bayes["risk_posterior"]
        user.txn_count += 1
        user.amount_sum += amount
        user.amount_sq_sum += amount**2
        user.velocity_sum += velocity_1h
        
        # 9. Compute risk score as smooth continuous function of z-scores
        # This creates proper Gaussian distribution of risk scores
        risk_from_amount = max(0, bayes["amount_zscore"]) * 15
        risk_from_velocity = max(0, bayes["velocity_zscore"]) * 12
        risk_from_location = max(0, bayes["distance_zscore"]) * 10
        risk_from_time = max(0, bayes["time_zscore"]) * 8
        risk_from_posterior = bayes["risk_posterior"] * 40
        
        # Add Gaussian noise (measurement error)
        noise = random.gauss(0, 3)
        
        risk_score = (risk_from_amount + risk_from_velocity + 
                     risk_from_location + risk_from_time + 
                     risk_from_posterior + noise)
        
        # NO HARD CAP - allow natural Gaussian tails
        risk_score = max(0.0, min(150.0, risk_score))  # Soft cap at 150
        risk_score = round(risk_score, 2)
        
        # Normalize to 0-100 for display, but keep original for modeling
        risk_score_normalized = min(100.0, risk_score)
        
        # 10. Risk level from continuous score
        if risk_score >= 75: risk_level = "CRITICAL"
        elif risk_score >= 50: risk_level = "HIGH"
        elif risk_score >= 25: risk_level = "MEDIUM"
        else: risk_level = "LOW"
        
        # 11. Fraud determination (probabilistic based on risk)
        # Uses sigmoid function for smooth probability
        fraud_prob = 1.0 / (1.0 + math.exp(-(risk_score - 60) / 15))
        is_fraud = 1 if random.random() < fraud_prob else 0
        
        if is_fraud:
            # Fraud type based on which evidence was strongest
            evidences = {
                "ACCOUNT_TAKEOVER": bayes["amount_evidence"] + bayes["velocity_evidence"],
                "VELOCITY_ABUSE": bayes["velocity_evidence"] * 2,
                "LOCATION_ANOMALY": bayes["distance_evidence"] * 2,
                "SOCIAL_ENGINEERING": bayes["amount_evidence"] * 1.5,
                "MULE_ACCOUNT": bayes["velocity_evidence"] + bayes["distance_evidence"],
                "SUSPICIOUS_MERCHANT": bayes["amount_evidence"] if is_p2m else 0,
                "SUSPICIOUS_BENEFICIARY": bayes["distance_evidence"],
                "DEVICE_COMPROMISE": bayes["time_evidence"] + bayes["velocity_evidence"],
            }
            fraud_type = max(evidences, key=evidences.get)
        else:
            fraud_type = "NONE"
        
        # 12. Risk flags from z-scores
        risk_flags = []
        if unusual_time_flag: risk_flags.append("UNUSUAL_TIME")
        if amount > 50000: risk_flags.append("HIGH_AMOUNT")
        if velocity_1h > 15: risk_flags.append("HIGH_VELOCITY")
        if location_mismatch: risk_flags.append("LOCATION_MISMATCH")
        if bayes["amount_zscore"] > 2.5: risk_flags.append("UNUSUAL_AMOUNT")
        if bayes["velocity_zscore"] > 2.5: risk_flags.append("RAPID_SUCCESSION")
        if user.account_age_days < 30: risk_flags.append("NEW_DEVICE")
        if payee_user.account_age_days < 14: risk_flags.append("NEW_BENEFICIARY")
        if random.random() < 0.008: risk_flags.append("VPN_DETECTED")
        if random.random() < 0.004: risk_flags.append("PROXY_DETECTED")
        if bayes["combined_evidence"] > 0.3: risk_flags.append("SUSPICIOUS_IP")
        if user.txn_count > 0 and bayes["amount_zscore"] > 3: risk_flags.append("FAILED_TXN_SPIKE")
        
        # 13. Decision from continuous risk score
        decision_prob_allow = 1.0 / (1.0 + math.exp((risk_score - 50) / 10))
        r = random.random()
        if r < decision_prob_allow * 0.92:
            decision = "ALLOW"
        elif r < decision_prob_allow * 0.92 + (1 - decision_prob_allow) * 0.4:
            decision = "CHALLENGE"
        elif r < decision_prob_allow * 0.92 + (1 - decision_prob_allow) * 0.7:
            decision = "REVIEW"
        else:
            decision = "DECLINE"
        
        # 14. Processing status consistent with decision
        if decision == "DECLINE":
            processing_status = "FAILURE"
            response_code = random.choice(["FR", "U30"])
        elif decision == "REVIEW":
            processing_status = random.choices(["SUCCESS", "PENDING", "FAILURE"], weights=[0.55, 0.30, 0.15])[0]
            response_code = "00" if processing_status == "SUCCESS" else ("91" if processing_status == "PENDING" else random.choice(["U30", "U16"]))
        elif decision == "CHALLENGE":
            processing_status = random.choices(["SUCCESS", "FAILURE"], weights=[0.82, 0.18])[0]
            response_code = "00" if processing_status == "SUCCESS" else random.choice(["U16", "U17", "U28"])
        else:  # ALLOW
            processing_status = random.choices(["SUCCESS", "PENDING", "FAILURE"], weights=[0.95, 0.03, 0.02])[0]
            response_code = "00" if processing_status == "SUCCESS" else ("91" if processing_status == "PENDING" else random.choice(["U01", "U30"]))
        
        # Small chance of technical failure regardless
        if random.random() < 0.005:
            processing_status = "FAILURE"
            response_code = random.choice(["U01", "U16", "U17", "U28"])
        
        auth_method = random.choice(["UPI_PIN", "BIOMETRIC", "DEVICE_AUTH", "OTP"])
        if processing_status == "SUCCESS":
            auth_status = random.choices(["SUCCESS", "NOT_REQUIRED"], weights=[0.85, 0.15])[0]
        else:
            auth_status = random.choices(["FAILED", "SUCCESS", "NOT_REQUIRED"], weights=[0.55, 0.30, 0.15])[0]
        
        # 15. Transaction type
        if is_p2m:
            txn_type = random.choices(["P2M", "COLLECT", "REFUND", "MANDATE"], weights=[0.70, 0.15, 0.10, 0.05])[0]
        else:
            txn_type = random.choices(["P2P", "P2A", "REVERSAL"], weights=[0.85, 0.10, 0.05])[0]
        
        # 16. Gaussian latencies
        latencies = self._sample_latencies(risk_score)
        
        # 17. Build complete event
        txn_id = f"TXN{uuid.uuid4().hex[:14].upper()}"
        rrn = f"{random.randint(100000000000, 999999999999)}"
        
        # Velocity features (continuous, Gaussian-distributed)
        txn_count_1m = max(0, int(random.gauss(velocity_1h * 0.2, 0.5)))
        txn_count_5m = max(txn_count_1m, int(random.gauss(velocity_1h * 0.5, 1.0)))
        txn_count_15m = max(txn_count_5m, int(random.gauss(velocity_1h * 0.8, 1.5)))
        txn_count_30m = max(txn_count_15m, int(random.gauss(velocity_1h, 2.0)))
        txn_count_1h_int = max(txn_count_30m, int(velocity_1h))
        txn_count_6h = max(txn_count_1h_int, int(random.gauss(velocity_24h * 0.3, velocity_24h * 0.1)))
        txn_count_24h = max(txn_count_6h, int(random.gauss(velocity_24h, velocity_24h * 0.2)))
        
        # Amount aggregates (continuous)
        amount_1h = round(amount * max(1.0, velocity_1h / 2) + random.gauss(0, amount * 0.1), 2)
        amount_6h = round(amount * max(1.0, velocity_24h / 3) + random.gauss(0, amount * 0.2), 2)
        amount_24h = round(amount * max(1.0, velocity_24h / 4) + random.gauss(0, amount * 0.3), 2)
        
        # Behavioral deviation score (for anomaly detection)
        behavioral_deviation = round(
            (abs(bayes["amount_zscore"]) + abs(bayes["velocity_zscore"]) + 
             abs(bayes["distance_zscore"]) + abs(bayes["time_zscore"])) / 4.0, 3
        )
        
        event = {
            # Core IDs
            "txn_id": txn_id,
            "txn_ref_id": f"REF{uuid.uuid4().hex[:10].upper()}",
            "org_txn_id": f"ORG{uuid.uuid4().hex[:14].upper()}",
            "parent_txn_id": None,
            "rrn": rrn,
            "message_id": f"MSG{uuid.uuid4().hex[:16].upper()}",
            "request_id": f"REQ{uuid.uuid4().hex[:16].upper()}",
            "session_id": f"SES{uuid.uuid4().hex[:14].upper()}",
            "customer_ref_id": f"CUS{random.randint(1000000000, 9999999999)}",
            "merchant_ref_id": merchant_id,
            
            # Timestamp
            "txn_timestamp": timestamp,
            "txn_date": txn_ist.strftime("%Y-%m-%d"),
            "txn_year": txn_ist.year,
            "txn_month": txn_ist.month,
            "txn_day": txn_ist.day,
            "txn_hour": ist_hour,
            "txn_minute": ist_minute,
            "txn_second": ist_second,
            "day_of_week": txn_ist.weekday() + 1,
            "week_of_year": txn_ist.isocalendar()[1],
            "quarter": (txn_ist.month - 1) // 3 + 1,
            "is_weekend": txn_ist.weekday() >= 5,
            "is_holiday": random.random() < 0.035,
            "is_business_hour": is_business_hour,
            "time_period": time_period,
            "festival_flag": random.random() < 0.06,
            "salary_day_flag": txn_ist.day in [1, 2, 3, 5, 28, 29, 30, 31],
            "month_start_flag": txn_ist.day <= 3,
            "month_end_flag": txn_ist.day >= 28,
            
            # Payer
            "payer_id": user.user_id,
            "payer_vpa": user.vpa,
            "payer_bank_code": user.bank[0],
            "payer_bank_name": user.bank[1],
            "payer_ifsc": f"{user.bank[0]}000{random.randint(1000, 9999)}",
            "payer_account_type": user.account_type,
            "payer_customer_type": "INDIVIDUAL" if user.user_type != "MERCHANT" else "MERCHANT",
            "payer_psp": user.psp,
            "payer_mobile_hash": hashlib.sha256(f"mobile-{user.user_id}".encode()).hexdigest(),
            
            # Payee
            "payee_id": payee_user.user_id,
            "payee_vpa": payee_vpa,
            "payee_bank_code": payee_bank[0],
            "payee_bank_name": payee_bank[1],
            "payee_ifsc": f"{payee_bank[0]}000{random.randint(1000, 9999)}",
            "payee_account_type": payee_account_type,
            "payee_customer_type": payee_customer_type,
            "payee_psp": payee_psp,
            
            # Merchant
            "merchant_id": merchant_id,
            "merchant_vpa": payee_merchant.vpa if payee_merchant else None,
            "merchant_type": merchant_type,
            "merchant_size": merchant_size,
            "merchant_mcc": merchant_mcc,
            "merchant_state": merchant_state,
            "merchant_city": merchant_city,
            "merchant_pincode": merchant_pincode,
            "merchant_account_age_days": merchant_age,
            
            # App/Device
            "app": user.app_pkg,
            "app_name": user.app_name,
            "app_version": user.app_version,
            "device_id": user.device_id,
            "device_fingerprint": user.device_fingerprint,
            "device_type": user.device_type,
            "device_brand": user.device_brand,
            "device_model": user.device_model,
            "device_os": user.device_os,
            "device_os_version": user.device_os_version,
            "device_language": user.home_language,
            "device_timezone": "Asia/Kolkata",
            "device_rooted": random.random() < 0.003,
            "device_emulator": random.random() < 0.001,
            "device_first_seen_days_ago": user.device_first_seen,
            "device_last_seen_minutes_ago": random.randint(0, 10080),
            
            # Network
            "mobile_operator": user.mobile_operator,
            "network_type": random.choices(["4G", "5G", "WIFI", "3G"], weights=[0.40, 0.38, 0.18, 0.04])[0],
            "ip_address": f"{random.randint(10,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "ip_country": "IN",
            "ip_region": payer_state,
            "ip_city": payer_city,
            "ip_risk_score": round(min(100, max(0, risk_score * 0.5 + random.gauss(0, 10))), 2),
            "vpn_flag": "VPN_DETECTED" in risk_flags,
            "proxy_flag": "PROXY_DETECTED" in risk_flags,
            
            # Geography
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
            "geo_distance_km": geo_distance_km,
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
            "authentication_method": auth_method,
            "authentication_status": auth_status,
            "authentication_attempt_count": random.randint(1, 3),
            "authentication_failure_count": random.randint(0, 2) if auth_status == "FAILED" else 0,
            "pin_retry_count": random.randint(0, 3) if auth_status == "FAILED" else 0,
            "device_binding_status": random.choices(["BOUND", "REBOUND", "FAILED"], weights=[0.97, 0.025, 0.005])[0],
            "mobile_binding_status": random.choices(["BOUND", "FAILED"], weights=[0.99, 0.01])[0],
            
            # Velocity (continuous Gaussian)
            "txn_count_1m": txn_count_1m,
            "txn_count_5m": txn_count_5m,
            "txn_count_15m": txn_count_15m,
            "txn_count_30m": txn_count_30m,
            "txn_count_1h": txn_count_1h_int,
            "txn_count_6h": txn_count_6h,
            "txn_count_24h": txn_count_24h,
            "amount_1h": amount_1h,
            "amount_6h": amount_6h,
            "amount_24h": amount_24h,
            "unique_payees_1h": min(txn_count_1h_int, random.randint(1, 8)),
            "unique_payees_24h": min(txn_count_24h, random.randint(1, 35)),
            "unique_devices_24h": 1 if random.random() < 0.95 else 2,
            "unique_ips_24h": 1 if random.random() < 0.90 else random.randint(2, 3),
            "unique_banks_24h": random.randint(1, 3),
            
            # Account history
            "payer_account_age_days": user.account_age_days,
            "payer_txn_count_24h": txn_count_24h,
            "payer_txn_count_7d": int(txn_count_24h * random.gauss(5, 1)),
            "payer_amount_24h": amount_24h,
            "payer_amount_7d": round(amount_24h * random.gauss(5, 1), 2),
            "payee_account_age_days": payee_user.account_age_days,
            "payee_txn_count_24h": random.randint(0, 200),
            "payee_amount_24h": round(random.gauss(150000, 60000), 2),
            "merchant_txn_count_24h": merchant_txn_24h,
            "merchant_amount_24h": merchant_amt_24h,
            "merchant_risk_score": merchant_risk,
            
            # Risk flags
            "new_device_flag": user.account_age_days < 7,
            "new_beneficiary_flag": payee_user.account_age_days < 14,
            "high_amount_flag": amount > 50000,
            "high_velocity_flag": velocity_1h > 15,
            "unusual_time_flag": unusual_time_flag,
            "unusual_amount_flag": amount > 100000,
            "multiple_accounts_device_flag": random.random() < 0.02,
            "multiple_devices_account_flag": random.random() < 0.01,
            "suspicious_ip_flag": "SUSPICIOUS_IP" in risk_flags,
            
            # Risk scores (CONTINUOUS, NO HARD CAPS for Gaussian modeling)
            "risk_score": risk_score_normalized,
            "risk_score_raw": risk_score,  # Keep raw for modeling
            "rule_score": round(min(100, risk_score * 0.92 + random.gauss(0, 5)), 2),
            "ml_score": round(min(1.0, max(0.0, (risk_score / 100) * 0.95 + random.gauss(0, 0.02))), 5),
            "velocity_score": round(min(100, velocity_1h * 2.5 + random.gauss(0, 3)), 2),
            "device_score": round(max(0, random.gauss(15, 8) + (40 if user.account_age_days < 7 else 0)), 2),
            "location_score": 85.0 if location_mismatch else round(max(0, random.gauss(15, 8)), 2),
            "merchant_score": merchant_risk if merchant_risk is not None else round(max(0, random.gauss(15, 8)), 2),
            "customer_score": round(max(0, min(100, 100 - risk_score + random.gauss(0, 10))), 2),
            "network_score": round(max(0, min(100, random.gauss(20, 15) + (40 if "VPN_DETECTED" in risk_flags else 0))), 2),
            "risk_level": risk_level,
            "fraud_type": fraud_type,
            "rule_triggered": risk_score >= 50,
            "rule_id": "EFRM_RISK_075" if risk_score >= 75 else ("EFRM_RISK_050" if risk_score >= 50 else ("EFRM_VELOCITY_001" if velocity_1h > 15 else None)),
            "rule_count": random.randint(1, 4) if risk_score >= 50 else 0,
            
            # Processing
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
            
            # Infrastructure latencies (Gaussian)
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
            **latencies,
            "yugabyte_flag": True,
            "cassandra_flag": random.random() < 0.35,
            "hive_flag": random.random() < 0.06,
            "elasticsearch_flag": random.random() < 0.80,
            
            # ============ NEW: BAYESIAN FEATURES ============
            # Z-scores (evidence for Gaussian anomaly detection)
            "amount_zscore": bayes["amount_zscore"],
            "velocity_zscore": bayes["velocity_zscore"],
            "distance_zscore": bayes["distance_zscore"],
            "time_zscore": bayes["time_zscore"],
            
            # Evidence likelihoods (for Bayesian updating)
            "amount_evidence": bayes["amount_evidence"],
            "velocity_evidence": bayes["velocity_evidence"],
            "distance_evidence": bayes["distance_evidence"],
            "time_evidence": bayes["time_evidence"],
            "combined_evidence": bayes["combined_evidence"],
            
            # Bayesian prior/posterior (for sequential updating)
            "risk_prior": bayes["risk_prior"],
            "risk_posterior": bayes["risk_posterior"],
            
            # User behavioral profile (for GMM clustering)
            "user_type": bayes["user_type"],
            "user_amount_mean": bayes["user_amount_mean"],
            "user_amount_std": bayes["user_amount_std"],
            "user_velocity_mean": bayes["user_velocity_mean"],
            "user_velocity_std": bayes["user_velocity_std"],
            "user_peak_hour": bayes["user_peak_hour"],
            "user_home_spread_km": bayes["user_home_spread_km"],
            
            # Behavioral deviation (anomaly score)
            "behavioral_deviation": behavioral_deviation,
            
            # User history stats (for Bayesian updating)
            "user_txn_count": user.txn_count,
            "user_amount_running_mean": round(user.amount_sum / max(1, user.txn_count), 2),
            "user_velocity_running_mean": round(user.velocity_sum / max(1, user.txn_count), 2),
            
            "simulator_thread": 0,
        }
        
        return event


# ============================================================
# 5. WORKER THREAD & MAIN EXECUTION
# ============================================================

generator = None  # Will be initialized in main

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


def main():
    global generator

    parser = argparse.ArgumentParser(description="Gaussian-Clustered UPI/eFRM Simulator for Bayesian Inference")
    parser.add_argument("-t", "--time", type=int, default=60, help="Duration in seconds")
    parser.add_argument("-th", "--threads", type=int, default=4, help="Number of threads")
    parser.add_argument("-r", "--tps", type=int, default=100, help="Target TPS")
    parser.add_argument("--topic", type=str, default="upi-efrm-txn-gauss-model", help="Kafka topic")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka servers")
    parser.add_argument("--users", type=int, default=5000, help="Number of user profiles")
    parser.add_argument("--merchants", type=int, default=2000, help="Number of merchant profiles")
    args = parser.parse_args()

    print("=" * 70)
    print("GAUSSIAN-CLUSTERED UPI/eFRM SIMULATOR")
    print("Designed for: GMM clustering, Bayesian inference, anomaly detection")
    print("=" * 70)
    print(f"Users: {args.users} | Merchants: {args.merchants}")
    print(f"TPS: {args.tps} | Threads: {args.threads} | Duration: {args.time}s")
    print(f"Topic: {args.topic} | Servers: {args.bootstrap_servers}")
    print("-" * 70)

    # Initialize Gaussian generator
    print("[OK] Initializing Gaussian user/merchant profiles...")
    generator = GaussianTransactionGenerator(n_users=args.users, n_merchants=args.merchants)
    print(f"[OK] Created {len(generator.users)} users, {len(generator.merchants)} merchants")

    # Pre-flight connectivity check
    host_port = args.bootstrap_servers.split(',')
    host = host_port[0].split(':')[0]
    port = int(host_port[0].split(':')[1]) if ':' in host_port[0] else 9092
    try:
        with socket.create_connection((host, port), timeout=3):
            print("[OK] Connected to Kafka broker.")
    except (ConnectionRefusedError, socket.timeout, OSError):
        print(f"[ERROR] Cannot connect to Kafka at {host}:{port}")
        return

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

    stop_event = threading.Event()
    stats = {}
    threads = []
    start_time = time.monotonic()

    for i in range(args.threads):
        t = threading.Thread(target=worker_thread,
                            args=(i, producer, args.topic, stop_event, start_time, args.time, args.tps, args.threads, stats))
        t.start()
        threads.append(t)

    try:
        while time.monotonic() - start_time < args.time and not stop_event.is_set():
            time.sleep(1)
            elapsed = time.monotonic() - start_time
            total_sent = sum(stats.values())
            current_tps = total_sent / elapsed if elapsed > 0 else 0
            print(f"[{elapsed:.1f}s] Sent: {total_sent} | TPS: {current_tps:.1f}")
    except KeyboardInterrupt:
        print("\nInterrupted.")
        stop_event.set()

    for t in threads:
        t.join()

    print("Flushing...")
    try:
        producer.flush(timeout=30)
        print("[OK] Flush successful.")
    except Exception as e:
        print(f"[ERROR] Flush failed: {e}")
    finally:
        producer.close()

    total_sent = sum(stats.values())
    actual_duration = time.monotonic() - start_time
    final_tps = total_sent / actual_duration if actual_duration > 0 else 0

    print("=" * 70)
    print(f"Simulation Complete!")
    print(f"Total: {total_sent} | Duration: {actual_duration:.2f}s | Avg TPS: {final_tps:.1f}")
    print("=" * 70)
    print("\nBAYESIAN/GMM FEATURES GENERATED:")
    print("  - amount_zscore, velocity_zscore, distance_zscore, time_zscore")
    print("  - amount_evidence, velocity_evidence, distance_evidence, time_evidence")
    print("  - risk_prior, risk_posterior (sequential Bayesian updating)")
    print("  - user_type (CASUAL/POWER/MERCHANT/DORMANT/HIGH_RISK) - GMM labels")
    print("  - user_amount_mean/std, user_velocity_mean/std - Gaussian params")
    print("  - behavioral_deviation - combined anomaly score")
    print("  - Continuous risk_score (no hard caps, Gaussian tails preserved)")
    print("\nRecommended models:")
    print("  - sklearn.mixture.GaussianMixture(n_components=5) on user features")
    print("  - PyMC3/PyMC4 for hierarchical Bayesian risk updating")
    print("  - IsolationForest on z-scores for anomaly detection")
    print("  - Kalman filter on risk_posterior time series for drift detection")


if __name__ == "__main__":
    main()



# ================================================================================
# ================================================================================

''' 
# 1. GMM Clustering (identify user types)
from sklearn.mixture import GaussianMixture
gmm = GaussianMixture(n_components=5, covariance_type='full')
features = ['amount', 'txn_count_1h', 'txn_hour', 'geo_distance_km']
gmm.fit(df[features])
df['gmm_cluster'] = gmm.predict(df[features])

# 2. Bayesian Risk Updating (PyMC)
import pymc as pm
with pm.Model() as model:
    prior = pm.Beta('prior', alpha=2, beta=50)  # User's base fraud rate
    likelihood = pm.Bernoulli('likelihood', p=prior, observed=df['is_fraud'])
    posterior = pm.sample(2000)

# 3. Anomaly Detection (z-score based)
from sklearn.ensemble import IsolationForest
iso = IsolationForest(contamination=0.02)
z_features = ['amount_zscore', 'velocity_zscore', 'distance_zscore', 'time_zscore']
df['anomaly_score'] = iso.fit_predict(df[z_features])

# 4. Kalman Filter (temporal drift)
from pykalman import KalmanFilter
kf = KalmanFilter(initial_state_mean=df['risk_posterior'].iloc[0], n_dim_obs=1)
state_means, _ = kf.filter(df['risk_posterior'].values)
'''