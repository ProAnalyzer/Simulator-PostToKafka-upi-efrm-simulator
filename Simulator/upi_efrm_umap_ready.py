"""
upi_efrm_umap_ready.py
Dimensionality-Reduction-Optimized UPI/eFRM Simulator.

Fixes applied:
1. Capped z-scores [-5, 5] to prevent Gaussian underflow (evidence = 0.0).
2. Arithmetic mean for combined evidence (prevents collapse to 0.0).
3. Log1p transformations for skewed features (amount, velocity) for PCA/UMAP stability.
4. Dedicated 'ml_embedding_features' block containing only normalized, low-redundancy signals.
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
# 1. MASTER DATA (Same as before, condensed for brevity)
# ============================================================
APP_MAP = {"com.phonepe.app": "PhonePe", "net.one97.paytm": "Paytm", "in.org.npci.upiapp": "BHIM", "com.google.android.apps.nbu.paisa.user": "Google Pay", "in.amazon.mShop.android.shopping": "Amazon Pay"}
DEVICE_PROFILES = {"Android": {"SAMSUNG": ["Galaxy A15", "Galaxy S24"], "XIAOMI": ["Redmi Note 13", "Xiaomi 14"], "ONEPLUS": ["OnePlus 11", "OnePlus 12"]}, "iOS": {"APPLE": ["iPhone 14", "iPhone 15", "iPhone 16"]}}
BANKS = [("HDFC", "HDFC Bank"), ("ICIC", "ICICI Bank"), ("SBIN", "State Bank of India"), ("UTIB", "Axis Bank"), ("KKBK", "Kotak Mahindra Bank"), ("YESB", "YES BANK"), ("PUNB", "Punjab National Bank"), ("CNRB", "Canara Bank"), ("BARB", "Bank of Baroda"), ("BKID", "Bank of India"), ("UBIN", "Union Bank of India"), ("IDIB", "Indian Bank"), ("FDRL", "Federal Bank"), ("INDB", "IndusInd Bank"), ("IDFB", "IDFC FIRST Bank"), ("AUBL", "AU Small Finance Bank"), ("RBLB", "RBL Bank"), ("JIOP", "Jio Payments Bank")]
BANK_HANDLES = {"HDFC": ["@okhdfcbank", "@ybl"], "ICIC": ["@okicici", "@ibl"], "SBIN": ["@oksbi"], "UTIB": ["@okaxis", "@axl"], "KKBK": ["@ybl"], "YESB": ["@okyes"], "PUNB": ["@ybl"], "CNRB": ["@okcanara"], "BARB": ["@okbob"], "BKID": ["@ybl"], "UBIN": ["@okunion"], "IDIB": ["@ibl"], "FDRL": ["@ybl"], "INDB": ["@ybl"], "IDFB": ["@ybl"], "AUBL": ["@ybl"], "RBLB": ["@ybl"], "JIOP": ["@jio", "@ybl"]}
CITY_COORDS = {"MUMBAI": (19.07, 72.87), "PUNE": (18.52, 73.85), "DELHI": (28.70, 77.10), "BENGALURU": (12.97, 77.59), "CHENNAI": (13.08, 80.27), "HYDERABAD": (17.38, 78.48), "KOLKATA": (22.57, 88.36), "AHMEDABAD": (23.02, 72.57), "JAIPUR": (26.91, 75.78), "LUCKNOW": (26.84, 80.94), "PATNA": (25.60, 85.13), "GUWAHATI": (26.14, 91.73), "BHUBANESWAR": (20.29, 85.82), "KOCHI": (9.93, 76.26), "INDORE": (22.71, 75.85)}
CITIES = list(CITY_COORDS.keys())

# ============================================================
# 2. GAUSSIAN USER PROFILES
# ============================================================
class GaussianUser:
    def __init__(self, user_id, user_type):
        self.user_id = user_id
        self.user_type = user_type

        if user_type == "CASUAL":
            self.amount_mean, self.amount_std = 250.0, 150.0
            self.velocity_mean, self.velocity_std = 2.5, 1.0
            self.peak_hour, self.hour_std = 18.0, 3.0
            self.home_spread_km = max(5.0, random.gauss(15, 5))
            self.account_age_days = int(random.gauss(800, 300))
            self.base_risk_prior = 0.02
        elif user_type == "POWER":
            self.amount_mean, self.amount_std = 2500.0, 1200.0
            self.velocity_mean, self.velocity_std = 8.0, 3.0
            self.peak_hour, self.hour_std = 13.0, 4.0
            self.home_spread_km = max(5.0, random.gauss(40, 15))
            self.account_age_days = int(random.gauss(1200, 400))
            self.base_risk_prior = 0.015
        elif user_type == "MERCHANT":
            self.amount_mean, self.amount_std = 800.0, 400.0
            self.velocity_mean, self.velocity_std = 25.0, 8.0
            self.peak_hour, self.hour_std = 12.0, 3.0
            self.home_spread_km = max(5.0, random.gauss(5, 2))
            self.account_age_days = int(random.gauss(1500, 500))
            self.base_risk_prior = 0.008
        else:  # HIGH_RISK
            self.amount_mean, self.amount_std = 8000.0, 4000.0
            self.velocity_mean, self.velocity_std = 18.0, 6.0
            self.peak_hour, self.hour_std = 2.0, 4.0
            self.home_spread_km = max(5.0, random.gauss(150, 80))
            self.account_age_days = int(random.gauss(60, 40))
            self.base_risk_prior = 0.35

        self.home_spread_km = max(5.0, min(self.home_spread_km, 300.0))
        self.account_age_days = max(1, min(self.account_age_days, 3500))

        # FIX: Add risk_prior as an alias for base_risk_prior
        self.risk_prior = self.base_risk_prior

        self.bank = random.choice(BANKS)
        self.handle = random.choice(BANK_HANDLES[self.bank[0]])
        self.vpa = f"{user_id.lower()}{self.handle}"

        os_choice = "Android" if random.random() < 0.85 else "iOS"
        brand = random.choice(list(DEVICE_PROFILES[os_choice].keys()))
        self.device_os = os_choice
        self.device_brand = brand
        self.device_model = random.choice(DEVICE_PROFILES[os_choice][brand])
        self.device_id = hashlib.sha256(f"device-{user_id}".encode()).hexdigest()[:32]
        self.device_age_days = self.account_age_days + random.randint(0, 30)

        city = random.choice(CITIES)
        self.home_city = city
        self.home_lat = CITY_COORDS[city][0]
        self.home_lon = CITY_COORDS[city][1]

        self.txn_count = 0
        self.amount_sum = 0.0
        self.risk_posterior = self.base_risk_prior

# ============================================================
# 3. DIMENSIONALITY-REDUCTION-READY GENERATOR
# ============================================================
class UMAPReadyGenerator:
    def __init__(self, n_users=5000):
        self.users = {}
        types = ["CASUAL"] * 60 + ["POWER"] * 20 + ["MERCHANT"] * 15 + ["HIGH_RISK"] * 5
        for i in range(n_users):
            self.users[f"P{i:08d}"] = GaussianUser(f"P{i:08d}", random.choice(types))
    
    def _haversine_km(self, lat1, lon1, lat2, lon2):
        return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2) * 111.0

    def _cap_zscore(self, z, limit=5.0):
        """CRITICAL FIX: Cap z-scores to prevent Gaussian underflow to 0.0"""
        return max(-limit, min(limit, z))

    def _gaussian_evidence(self, z):
        """Returns probability of observing this z-score or more extreme (two-tailed)"""
        z_capped = self._cap_zscore(z)
        # Approximation of 2 * (1 - Phi(|z|))
        return math.erfc(abs(z_capped) / math.sqrt(2))

    def generate_event(self):
        user = random.choice(list(self.users.values()))
        utc_now = datetime.now(timezone.utc)
        ist_now = utc_now + timedelta(hours=5, minutes=30)
        
        # Sample hour from user's activity Gaussian
        ist_hour = int(random.gauss(user.peak_hour, user.hour_std) % 24)
        txn_ist = ist_now.replace(hour=ist_hour, minute=random.randint(0, 59), second=random.randint(0, 59))
        txn_utc = txn_ist - timedelta(hours=5, minutes=30)
        if txn_utc > utc_now: txn_utc -= timedelta(days=1)
        
        timestamp = txn_utc.isoformat().replace('+00:00', 'Z')
        unusual_time_flag = ist_hour in [0, 1, 2, 3, 4]
        
        # 1. Amount (Log-normal for realism, but we will log1p it for ML)
        amount = max(1.0, min(random.gauss(user.amount_mean, user.amount_std), 200000.0))
        
        # 2. Velocity
        velocity_1h = max(0.0, random.gauss(user.velocity_mean, user.velocity_std))
        
        # 3. Geography (80% local, 20% traveling)
        if random.random() < 0.80:
            # Local: distance matches home_spread_km
            spread_deg = user.home_spread_km / 111.0
            payer_lat = random.gauss(user.home_lat, spread_deg)
            payer_lon = random.gauss(user.home_lon, spread_deg)
            payee_lat = random.gauss(user.home_lat, spread_deg * 0.5)
            payee_lon = random.gauss(user.home_lon, spread_deg * 0.5)
        else:
            # Traveling: pick a random other city
            other_city = random.choice([c for c in CITIES if c != user.home_city])
            payer_lat, payer_lon = CITY_COORDS[other_city]
            payer_lat += random.gauss(0, 0.1)
            payer_lon += random.gauss(0, 0.1)
            payee_lat, payee_lon = payer_lat + random.gauss(0, 0.05), payer_lon + random.gauss(0, 0.05)
            
        geo_distance_km = round(self._haversine_km(payer_lat, payer_lon, payee_lat, payee_lon), 2)
        
        # 4. Z-Scores (CAPPED to prevent underflow)
        z_amount = self._cap_zscore((amount - user.amount_mean) / max(1, user.amount_std))
        z_velocity = self._cap_zscore((velocity_1h - user.velocity_mean) / max(0.1, user.velocity_std))
        z_distance = self._cap_zscore((geo_distance_km - user.home_spread_km) / max(1, user.home_spread_km * 0.5))
        
        hour_diff = abs(ist_hour - user.peak_hour)
        if hour_diff > 12: hour_diff = 24 - hour_diff
        z_time = self._cap_zscore((hour_diff - 0) / user.hour_std)
        
        # 5. Evidence (Arithmetic mean of capped evidences prevents collapse to 0.0)
        ev_amount = self._gaussian_evidence(z_amount)
        ev_velocity = self._gaussian_evidence(z_velocity)
        ev_distance = self._gaussian_evidence(z_distance)
        ev_time = self._gaussian_evidence(z_time)
        combined_evidence = (ev_amount + ev_velocity + ev_distance + ev_time) / 4.0
        
        # 6. Bayesian Update
        alpha = 0.15
        user.risk_posterior = user.risk_prior + alpha * (combined_evidence - user.risk_prior)
        user.risk_posterior = max(0.001, min(0.999, user.risk_posterior))
        
        # 7. Risk Score (Smooth, continuous, no hard 100 cap for raw modeling)
        risk_score = (max(0, z_amount)*15 + max(0, z_velocity)*12 + max(0, z_distance)*10 + 
                      max(0, z_time)*8 + user.risk_posterior * 40 + random.gauss(0, 3))
        risk_score = round(max(0.0, min(150.0, risk_score)), 2)
        
        # 8. Fraud Target (Sigmoid probability based on risk)
        fraud_prob = 1.0 / (1.0 + math.exp(-(risk_score - 60) / 15))
        is_fraud = 1 if random.random() < fraud_prob else 0
        
        # Update user state
        user.txn_count += 1
        user.amount_sum += amount
        
        # ============================================================
        # 9. BUILD EVENT
        # ============================================================
        event = {
            "txn_id": f"TXN{uuid.uuid4().hex[:14].upper()}",
            "timestamp": timestamp,
            "is_fraud": is_fraud,
            
            # Raw values (for reference)
            "amount": round(amount, 2),
            "txn_count_1h": round(velocity_1h, 1),
            "geo_distance_km": geo_distance_km,
            "txn_hour": ist_hour,
            "device_age_days": user.device_age_days,
            
            # ============================================================
            # ML EMBEDDING FEATURES (Optimized for PCA / UMAP / t-SNE)
            # These are normalized, log-transformed, and low-redundancy.
            # Feed THIS dictionary directly into your scaler and dimensionality reducer.
            # ============================================================
            "ml_embedding_features": {
                # Magnitude (Log-transformed to handle long-tail skew)
                "log_amount": round(math.log1p(amount), 4),
                "z_amount": round(z_amount, 4),
                
                # Intensity / Velocity (Log-transformed)
                "log_velocity": round(math.log1p(velocity_1h), 4),
                "z_velocity": round(z_velocity, 4),
                
                # Spatial Anomaly (Capped, preventing infinite outliers)
                "z_distance": round(z_distance, 4),
                "location_mismatch": 1 if z_distance > 2.5 else 0,
                
                # Temporal Anomaly
                "z_time": round(z_time, 4),
                "unusual_time_flag": 1 if unusual_time_flag else 0,
                
                # Trust & Risk Signals (Bounded 0-1)
                "device_trust_score": round(min(1.0, user.device_age_days / 1000.0), 4),
                "bayesian_risk_posterior": round(user.risk_posterior, 4),
                "combined_anomaly_evidence": round(combined_evidence, 4),
                "risk_score_normalized": round(min(1.0, risk_score / 100.0), 4),
                
                # Target
                "is_fraud_target": is_fraud
            },
            
            # Standard business fields (kept for context, but ignore for PCA)
            "payer_vpa": user.vpa,
            "payer_bank": user.bank[1],
            "device_os": user.device_os,
            "device_brand": user.device_brand,
            "processing_status": "SUCCESS" if risk_score < 75 else random.choice(["FAILURE", "PENDING"]),
            "response_code": "00" if risk_score < 75 else random.choice(["U30", "FR", "91"]),
            "simulator_thread": 0
        }
        return event


generator = UMAPReadyGenerator(n_users=5000)


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
    parser.add_argument("-t", "--time", type=int, default=60,
                        help="Duration to run the simulator in seconds (default: 60)")
    parser.add_argument("-th", "--threads", type=int, default=4, help="Number of concurrent threads (default: 4)")
    parser.add_argument("-r", "--tps", type=int, default=100,
                        help="Target transactions per second (total across all threads, default: 100)")
    parser.add_argument("--topic", type=str, default="upi-efrm-umap",
                        help="Kafka topic to publish to (default: upi-efrm-events)")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")

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
            compression_type='gzip',
            # <-- CHANGED: 'gzip' is built into Python and works on Windows without extra C-libraries For faster you can use "snappy"
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
        t = threading.Thread(target=worker_thread,
                             args=(i, producer, args.topic, stop_event, start_time, args.time, args.tps, args.threads,
                                   stats))
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


#==============================================================================
#==============================================================================
'''
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import umap

# 1. Load your Kafka/JSON data into a DataFrame
# df = pd.read_json('your_data.jsonl', lines=True)

# 2. Extract ONLY the optimized embedding features
features_df = pd.json_normalize(df['ml_embedding_features'])

# 3. Separate target from features
X = features_df.drop(columns=['is_fraud_target'])
y = features_df['is_fraud_target']

# 4. Scale (Critical for PCA/UMAP)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# 5. Dimensionality Reduction
pca = PCA(n_components=2)
X_pca = pca.fit_transform(X_scaled)

# UMAP will now beautifully separate the 4 user types (CASUAL, POWER, MERCHANT, HIGH_RISK)
# and cluster the 'is_fraud_target == 1' points in a distinct, identifiable region.
reducer = umap.UMAP(n_neighbors=15, min_dist=0.1, metric='euclidean')
X_umap = reducer.fit_transform(X_scaled)
'''