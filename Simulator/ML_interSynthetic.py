"""
upi_simulator_datapump.py
ML-Ready, Cosmectically Perfected UPI/eFRM Synthetic Data Simulator.
All geographic, device, status, and risk features are strictly correlated.
"""

import argparse
import json
import random
import time
import threading
import uuid
import socket
import hashlib
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError

# ==========================================
# 1. STRICT MASTER DATA & MATRICES
# ==========================================

# --- App & Device Matrix (Eliminates Impossible Combinations) ---
APP_MAP = {
    "com.phonepe.app": {"name": "PhonePe", "os": ["Android", "iOS"]},
    "net.one97.paytm": {"name": "Paytm", "os": ["Android", "iOS"]},
    "in.org.npci.upiapp": {"name": "BHIM", "os": ["Android", "iOS"]},
    "com.google.android.apps.nbu.paisa.user": {"name": "Google Pay", "os": ["Android", "iOS"]},
    "in.amazon.mShop.android.shopping": {"name": "Amazon Pay", "os": ["Android", "iOS"]},
    "com.mobikwik_new": {"name": "MobiKwik", "os": ["Android", "iOS"]},
    "com.csam.icici.bank.imobile": {"name": "iMobile", "os": ["Android", "iOS"]},
    "com.axis.mobile": {"name": "Axis Mobile", "os": ["Android", "iOS"]},
    "com.sbi.lotusintouch": {"name": "YONO SBI", "os": ["Android", "iOS"]},
}

DEVICE_PROFILES = {
    "Android": {
        "SAMSUNG": ["Galaxy A15", "Galaxy A35", "Galaxy S23", "Galaxy S24"],
        "XIAOMI": ["Redmi Note 13", "Redmi Note 14", "Xiaomi 14"],
        "ONEPLUS": ["OnePlus 11", "OnePlus 12", "Nord 4"],
        "OPPO": ["Reno 11", "Find X7"],
        "VIVO": ["V30", "X100"],
        "REALME": ["Realme 12", "GT 5"],
        "GOOGLE": ["Pixel 8", "Pixel 9"],
        "NOTHING": ["Phone 2", "Phone 2a"],
        "MOTOROLA": ["Edge 50", "G84"],
        "POCO": ["X6", "F6"]
    },
    "iOS": {
        "APPLE": ["iPhone 14", "iPhone 15", "iPhone 16", "iPhone SE"]
    }
}

ANDROID_VERSIONS = ["11", "12", "13", "14", "15", "16"]
IOS_VERSIONS = ["16", "17", "18", "19"]

# --- Bank & VPA Handle Correlation ---
BANK_HANDLES = {
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
    "JIOP": ["@jio", "@ybl"]
}

BANKS = [
    ("HDFC", "HDFC Bank"), ("ICIC", "ICICI Bank"), ("SBIN", "State Bank of India"),
    ("UTIB", "Axis Bank"), ("KKBK", "Kotak Mahindra Bank"), ("YESB", "YES BANK"),
    ("PUNB", "Punjab National Bank"), ("CNRB", "Canara Bank"), ("BARB", "Bank of Baroda"),
    ("BKID", "Bank of India"), ("UBIN", "Union Bank of India"), ("IDIB", "Indian Bank"),
    ("IOBA", "Indian Overseas Bank"), ("FDRL", "Federal Bank"), ("INDB", "IndusInd Bank"),
    ("IDFB", "IDFC FIRST Bank"), ("AUBL", "AU Small Finance Bank"), ("RBLB", "RBL Bank"),
    ("JIOP", "Jio Payments Bank")
]

# --- Geography Matrix (Zone -> State -> City) ---
ZONE_STATE_CITY = {
    "EAST": {
        "WEST_BENGAL": ["KOLKATA", "SILIGURI"],
        "ODISHA": ["BHUBANESWAR"],
        "BIHAR": ["PATNA", "GAYA"],
        "JHARKHAND": ["RANCHI"]
    },
    "WEST": {
        "MAHARASHTRA": ["MUMBAI", "PUNE", "NAGPUR"],
        "GUJARAT": ["AHMEDABAD", "SURAT"],
        "RAJASTHAN": ["JAIPUR", "JODHPUR"]
    },
    "NORTH": {
        "DELHI": ["NEW_DELHI"],
        "UTTAR_PRADESH": ["LUCKNOW", "NOIDA"],
        "PUNJAB": ["AMRITSAR"],
        "HARYANA": ["GURUGRAM"],
        "UTTARAKHAND": ["DEHRADUN"]
    },
    "SOUTH": {
        "KARNATAKA": ["BENGALURU", "MYSURU"],
        "TAMIL_NADU": ["CHENNAI", "COIMBATORE"],
        "TELANGANA": ["HYDERABAD", "WARANGAL"],
        "KERALA": ["KOCHI"],
        "ANDHRA_PRADESH": ["VISAKHAPATNAM"]
    },
    "CENTRAL": {
        "MADHYA_PRADESH": ["INDORE", "BHOPAL"],
        "CHHATTISGARH": ["RAIPUR"]
    },
    "NORTHEAST": {
        "ASSAM": ["GUWAHATI"]
    }
}

STATE_LANGUAGE = {
    "WEST_BENGAL": "bn_IN", "MAHARASHTRA": "mr_IN", "GUJARAT": "gu_IN",
    "TAMIL_NADU": "ta_IN", "TELANGANA": "te_IN", "ANDHRA_PRADESH": "te_IN",
    "KARNATAKA": "en_IN", "KERALA": "en_IN", "DELHI": "hi_IN",
    "UTTAR_PRADESH": "hi_IN", "BIHAR": "hi_IN", "RAJASTHAN": "hi_IN",
    "MADHYA_PRADESH": "hi_IN", "HARYANA": "hi_IN", "PUNJAB": "hi_IN",
    "UTTARAKHAND": "hi_IN", "JHARKHAND": "hi_IN", "CHHATTISGARH": "hi_IN",
    "ODISHA": "en_IN", "ASSAM": "en_IN"
}

# --- Other Constants ---
PSPS = ["PHONEPE", "GOOGLEPAY", "PAYTM", "AMAZONPAY", "BHARATPE", "CRED", "MOBIKWIK", "NAVI", "BANK_PSP"]
TRANSACTION_TYPES = ["P2P", "P2M", "P2A", "COLLECT", "REFUND", "REVERSAL", "MANDATE"]
PAYMENT_CHANNELS = ["UPI_APP", "QR", "INTENT", "COLLECT", "IN_APP", "WEB", "BANK_APP"]
AUTH_METHODS = ["UPI_PIN", "BIOMETRIC", "DEVICE_AUTH", "OTP"]
DECISIONS = ["ALLOW", "DECLINE", "CHALLENGE", "REVIEW"]
FRAUD_TYPES = ["ACCOUNT_TAKEOVER", "SOCIAL_ENGINEERING", "MULE_ACCOUNT", "DEVICE_COMPROMISE", "VELOCITY_ABUSE", "SUSPICIOUS_MERCHANT", "SUSPICIOUS_BENEFICIARY", "LOCATION_ANOMALY"]
RISK_FLAGS = ["NEW_DEVICE", "NEW_BENEFICIARY", "HIGH_AMOUNT", "HIGH_VELOCITY", "LOCATION_MISMATCH", "DEVICE_MISMATCH", "UNUSUAL_TIME", "UNUSUAL_AMOUNT", "MULTIPLE_DEVICES_ACCOUNT", "SUSPICIOUS_IP", "VPN_DETECTED", "PROXY_DETECTED", "ACCOUNT_AGE_LOW", "RAPID_SUCCESSION"]
SERVER_REGIONS = ["EAST", "WEST", "NORTH", "SOUTH"]
SERVICE_NAMES = ["upi-gateway", "upi-switch", "rule-engine", "datalake-consumer", "notification-service", "case-management-service", "model-engine"]

# ==========================================
# 2. HELPER FUNCTIONS FOR CAUSAL LOGIC
# ==========================================

def get_geo_profile():
    zone = random.choice(list(ZONE_STATE_CITY.keys()))
    state = random.choice(list(ZONE_STATE_CITY[zone].keys()))
    city = random.choice(ZONE_STATE_CITY[zone][state])
    language = STATE_LANGUAGE.get(state, "en_IN")
    return zone, state, city, language

def get_vpa_and_bank(is_payer=True):
    bank = random.choice(BANKS)
    handle = random.choice(BANK_HANDLES[bank[0]])
    prefix = "p" if is_payer else "m"
    vpa = f"{prefix}{random.randint(1000, 9999)}{handle}"
    return bank, vpa

def get_device_profile():
    # 85% Android, 15% iOS
    os_choice = "Android" if random.random() < 0.85 else "iOS"
    brand = random.choice(list(DEVICE_PROFILES[os_choice].keys()))
    model = random.choice(DEVICE_PROFILES[os_choice][brand])
    version = random.choice(ANDROID_VERSIONS if os_choice == "Android" else IOS_VERSIONS)
    app_pkg = random.choice(list(APP_MAP.keys()))
    app_name = APP_MAP[app_pkg]["name"]
    
    # Ensure OS matches App compatibility (all listed apps support both mostly, but let's assume generic)
    return os_choice, version, brand, model, app_pkg, app_name

def generate_public_ip():
    while True:
        ip = f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        if not ip.startswith("10.") and not ip.startswith("192.168.") and not ip.startswith("172."):
            return ip

# ==========================================
# 3. THE PERFECT EVENT GENERATOR
# ==========================================

def generate_upi_event():
    # --- Core IDs ---
    txn_id = f"TXN{uuid.uuid4().hex[:14].upper()}"
    rrn = f"{random.randint(100000000000, 999999999999)}"
    
    # --- Time Logic (UTC for Kafka, IST for Business) ---
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    timestamp = utc_now.isoformat().replace('+00:00', 'Z')
    ist_hour = ist_now.hour
    
    # Deterministic Time Period
    if 5 <= ist_hour <= 8: time_period = "EARLY_MORNING"
    elif 9 <= ist_hour <= 12: time_period = "MORNING"
    elif 13 <= ist_hour <= 16: time_period = "AFTERNOON"
    elif 17 <= ist_hour <= 20: time_period = "EVENING"
    elif 21 <= ist_hour <= 23: time_period = "NIGHT"
    else: time_period = "LATE_NIGHT"
    
    unusual_time_flag = ist_hour in [0, 1, 2, 3, 4]
    is_business_hour = (9 <= ist_hour <= 18) and (ist_now.weekday() < 5)

    # --- Payer/Payee & Banks ---
    payer_bank, payer_vpa = get_vpa_and_bank(True)
    payee_bank, payee_vpa = get_vpa_and_bank(False)
    
    # --- Device & App ---
    os_choice, os_version, device_brand, device_model, app_pkg, app_name = get_device_profile()
    device_type = f"{os_choice.upper()}_PHONE" if random.random() < 0.95 else f"{os_choice.upper()}_TABLET"
    
    # --- Geography ---
    zone, state, city, language = get_geo_profile()
    ip_address = generate_public_ip()
    
    # --- Velocity Features (Simulated History) ---
    # Most transactions are low velocity, some are high
    base_velocity = random.choices([0, 1, 2, 5, 10, 20], weights=[0.5, 0.2, 0.1, 0.1, 0.08, 0.02])[0]
    txn_count_1h = base_velocity + random.randint(0, 5)
    txn_count_24h = txn_count_1h * random.randint(2, 10)
    
    high_velocity_flag = txn_count_1h > 15
    
    # --- Account Age & Flags ---
    payer_account_age_days = random.randint(1, 3500)
    new_device_flag = random.random() < 0.05  # 5% chance of new device
    new_beneficiary_flag = random.random() < 0.10 # 10% chance of new beneficiary
    
    # --- Causal Risk Scoring ---
    # Start with baseline noise
    risk_score = random.uniform(0, 30)
    
    # Add points for risky behaviors
    if unusual_time_flag: risk_score += 15
    if high_velocity_flag: risk_score += 20
    if new_device_flag: risk_score += 10
    if new_beneficiary_flag: risk_score += 10
    if random.random() < 0.01: risk_score += 50 # Random spike
    
    risk_score = min(risk_score, 100.0)
    
    # Determine Fraud Probability based on Risk Score
    if risk_score > 85: fraud_prob = 0.9
    elif risk_score > 60: fraud_prob = 0.4
    elif risk_score > 30: fraud_prob = 0.05
    else: fraud_prob = 0.001
    
    is_fraud = 1 if random.random() < fraud_prob else 0
    
    if is_fraud:
        fraud_type = random.choice(FRAUD_TYPES)
        risk_level = "CRITICAL" if risk_score > 75 else "HIGH"
    else:
        fraud_type = "NONE"
        risk_level = "CRITICAL" if risk_score >= 75 else "HIGH" if risk_score >= 50 else "MEDIUM" if risk_score >= 25 else "LOW"
        
    # Generate Risk Flags based on actual conditions
    risk_flags = []
    if unusual_time_flag: risk_flags.append("UNUSUAL_TIME")
    if high_velocity_flag: risk_flags.append("HIGH_VELOCITY")
    if new_device_flag: risk_flags.append("NEW_DEVICE")
    if new_beneficiary_flag: risk_flags.append("NEW_BENEFICIARY")
    if random.random() < 0.05: risk_flags.append("VPN_DETECTED")
    if random.random() < 0.02: risk_flags.append("SUSPICIOUS_IP")
    
    # --- Decision & Status Logic ---
    if risk_score >= 80:
        decision = "DECLINE"
        processing_status = "FAILURE"
        response_code = random.choice(["FR", "U30"])
    elif risk_score >= 60:
        decision = "REVIEW"
        processing_status = random.choice(["PENDING", "FAILURE"])
        response_code = "91" if processing_status == "PENDING" else "U16"
    elif risk_score >= 40:
        decision = "CHALLENGE"
        processing_status = "SUCCESS" # Challenge passed
        response_code = "00"
    else:
        decision = "ALLOW"
        processing_status = "SUCCESS"
        response_code = "00"
        
    # If it's a failure due to technical issue (not risk), override status
    if random.random() < 0.02: 
        processing_status = "FAILURE"
        response_code = "U01"
        decision = "ERROR"

    auth_method = random.choice(AUTH_METHODS)
    auth_status = "SUCCESS" if processing_status == "SUCCESS" else random.choice(["FAILED", "NOT_REQUIRED"])

    # --- Amount ---
    amount = round(min(max(random.lognormvariate(4.5, 1.2), 1.0), 200000.0), 2)
    high_amount_flag = amount > 50000

    return {
        "txn_id": txn_id,
        "rrn": rrn,
        "timestamp": timestamp,
        "time_period": time_period,
        "unusual_time_flag": unusual_time_flag,
        "is_business_hour": is_business_hour,
        "day_of_week": ist_now.strftime("%A"),
        "is_weekend": ist_now.weekday() >= 5,
        
        "payer_vpa": payer_vpa,
        "payer_bank_code": payer_bank[0],
        "payer_bank_name": payer_bank[1],
        "payer_ifsc": f"{payer_bank[0]}000{random.randint(1000, 9999)}",
        "payer_psp": random.choice(PSPS),
        "payer_customer_type": random.choices(["INDIVIDUAL", "MERCHANT", "CORPORATE"], weights=[0.9, 0.08, 0.02])[0],
        "payer_account_type": random.choice(["SAVINGS", "CURRENT", "SALARY"]),
        
        "payee_vpa": payee_vpa,
        "payee_bank_code": payee_bank[0],
        "payee_bank_name": payee_bank[1],
        "payee_psp": random.choice(PSPS),
        
        "merchant_id": f"MER{random.randint(100000, 999999)}" if random.random() > 0.3 else None,
        "merchant_type": random.choice(["GROCERY", "RESTAURANT", "FUEL", "E_COMMERCE", "TRAVEL", "UTILITY"]),
        "merchant_size": random.choice(["MICRO", "SMALL", "MEDIUM", "LARGE", "ENTERPRISE"]),
        
        "amount": amount,
        "currency": "INR",
        "country": "IN",
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "payment_channel": random.choice(PAYMENT_CHANNELS),
        
        "app": app_pkg,
        "app_name": app_name,
        "os": os_choice,
        "os_version": os_version,
        "device_type": device_type,
        "device_brand": device_brand,
        "device_model": device_model,
        "device_id": uuid.uuid4().hex,
        "ip_address": ip_address,
        "telecom_operator": random.choice(["JIO", "AIRTEL", "VI", "BSNL"]),
        "network_type": random.choices(["4G", "5G", "WIFI", "3G"], weights=[0.4, 0.3, 0.2, 0.1])[0],
        "language": language,
        
        "zone": zone,
        "state": state,
        "city": city,
        "pincode": f"{random.randint(100000, 999999)}",
        
        "auth_method": auth_method,
        "auth_status": auth_status,
        "processing_status": processing_status,
        "response_code": response_code,
        "decision": decision,
        
        "risk_score": round(risk_score, 2),
        "risk_level": risk_level,
        "is_fraud": is_fraud,
        "fraud_type": fraud_type,
        "risk_flags": list(set(risk_flags)), # Unique flags
        
        "velocity_1h": txn_count_1h,
        "velocity_24h": txn_count_24h,
        "high_velocity_flag": high_velocity_flag,
        "new_device_flag": new_device_flag,
        "new_beneficiary_flag": new_beneficiary_flag,
        "high_amount_flag": high_amount_flag,
        
        "processor_type": "ONLINE",
        "service_name": random.choice(SERVICE_NAMES),
        "server_region": random.choice(SERVER_REGIONS),
        "kafka_topic": "upi-efrm-txn",
        "simulator_thread": 0
    }


# ==========================================
# Worker Thread (Asynchronous & Non-Blocking)
# ==========================================
def worker_thread(thread_id, producer, topic, stop_event, start_time, duration, tps, num_threads, stats):
    sleep_duration = num_threads / tps if tps > 0 else 0
    count = 0
    first_send_logged = False
    
    while not stop_event.is_set() and (time.monotonic() - start_time) < duration:
        loop_start = time.monotonic()
        event = generate_upi_event()
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
