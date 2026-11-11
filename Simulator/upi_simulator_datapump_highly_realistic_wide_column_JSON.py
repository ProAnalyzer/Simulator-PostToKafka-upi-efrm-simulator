# Generated from: upi_simulator_datapump_highly_realistic_wide_column_JSON.py.ipynb
# Converted at: 2026-11-10T14:11:09.904Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

"""
upi_simulator_datapump.py
A high-throughput Python simulator and datapump for enriched synthetic UPI/eFRM transaction events.
Publishes realistic synthetic transactions to a Kafka topic at a controlled TPS.

Usage:
    python upi_simulator_datapump.py --time 60 --threads 8 --tps 1000 --topic upi-events --bootstrap-servers localhost:9092
"""

import argparse
import json
import random
import time
import threading
import uuid
from datetime import datetime, timezone
import socket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError

# ==========================================
# Enriched Synthetic Master Data
# ==========================================
APPS = [
    "com.phonepe.app", "net.one97.paytm", "in.org.npci.upiapp",
    "com.google.android.apps.nbu.paisa.user", "in.amazon.mShop.android.shopping",
    "com.mobikwik_new", "com.freecharge.android", "com.csam.icici.bank.imobile", 
    "com.axis.mobile", "com.sbi.upi", "com.dbs.in.digitalbank"
]
APP_NAMES = [
    "PhonePe", "Paytm", "BHIM", "Google Pay", "Amazon Pay",
    "MobiKwik", "Freecharge", "iMobile", "Axis Mobile", "YONO", "DBS digibank"
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
    ("PUNB", "Punjab National Bank"), ("CNRB", "Canara Bank"), ("BARB", "Bank of Baroda"), 
    ("BKID", "Bank of India"), ("UBIN", "Union Bank of India"), ("IDIB", "Indian Bank"),
    ("IOBA", "Indian Overseas Bank"), ("FDRL", "Federal Bank"), ("INDB", "IndusInd Bank"), 
    ("IDFB", "IDFC FIRST Bank"), ("AUBL", "AU Small Finance Bank"), ("RBLB", "RBL Bank"),
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
    "WEST_BENGAL", "MAHARASHTRA", "KARNATAKA", "TAMIL_NADU", "TELANGANA", "DELHI", 
    "GUJARAT", "RAJASTHAN", "UTTAR_PRADESH", "BIHAR", "ODISHA", "ASSAM", "KERALA", 
    "PUNJAB", "HARYANA", "MADHYA_PRADESH", "ANDHRA_PRADESH", "JHARKHAND", "CHHATTISGARH", "UTTARAKHAND"
]
CITIES = [
    "KOLKATA", "MUMBAI", "BENGALURU", "CHENNAI", "HYDERABAD", "NEW_DELHI", "PUNE", 
    "AHMEDABAD", "JAIPUR", "BHUBANESWAR", "LUCKNOW", "PATNA", "GUWAHATI", "KOCHI", "INDORE", "SURAT"
]

CUSTOMER_TYPES = ["INDIVIDUAL", "MERCHANT", "CORPORATE", "SMALL_BUSINESS"]
ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "SALARY", "NRE", "NRO"]
MERCHANT_TYPES = [
    "GROCERY", "RESTAURANT", "FUEL", "E_COMMERCE", "TRAVEL", "EDUCATION", 
    "HEALTHCARE", "UTILITY", "RETAIL", "ENTERTAINMENT", "PHARMACY", "FOOD_DELIVERY", "TRANSPORT", "HOTEL"
]
MERCHANT_SIZES = ["MICRO", "SMALL", "MEDIUM", "LARGE", "ENTERPRISE"]
MCC_CODES = ["5411", "5812", "5541", "5311", "5912", "4111", "4722", "5732", "6012", "6300", "7011", "7999", "4900", "8299"]

AUTH_METHODS = ["UPI_PIN", "BIOMETRIC", "DEVICE_AUTH", "OTP"]
AUTH_STATUSES = ["SUCCESS", "FAILED", "NOT_REQUIRED"]
DECISIONS = ["ALLOW", "DECLINE", "CHALLENGE", "REVIEW"]
RISK_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
FRAUD_TYPES = [
    "NONE", "ACCOUNT_TAKEOVER", "SOCIAL_ENGINEERING", "MULE_ACCOUNT",
    "DEVICE_COMPROMISE", "VELOCITY_ABUSE", "SUSPICIOUS_MERCHANT",
    "SUSPICIOUS_BENEFICIARY", "LOCATION_ANOMALY"
]
RISK_FLAGS = [
    "NEW_DEVICE", "NEW_BENEFICIARY", "HIGH_AMOUNT", "HIGH_VELOCITY",
    "LOCATION_MISMATCH", "DEVICE_MISMATCH", "UNUSUAL_TIME", "UNUSUAL_AMOUNT",
    "MULTIPLE_ACCOUNTS_DEVICE", "MULTIPLE_DEVICES_ACCOUNT", "MULTIPLE_IP_DEVICE",
    "SUSPICIOUS_IP", "VPN_DETECTED", "PROXY_DETECTED", "ACCOUNT_AGE_LOW",
    "BENEFICIARY_AGE_LOW", "RAPID_SUCCESSION", "FAILED_TXN_SPIKE"
]

RESULT_ERR_CODES = [
    ("SUCCESS", "00"), ("FAILURE", "U30"), ("FAILURE", "FR"), ("FAILURE", "U01"),
    ("FAILURE", "U16"), ("FAILURE", "U17"), ("FAILURE", "U28"), ("PENDING", "91"),
]

SERVER_REGIONS = ["EAST", "WEST", "NORTH", "SOUTH"]
SERVICE_NAMES = [
    "upi-gateway", "upi-switch", "rule-engine", "datalake-consumer",
    "notification-service", "case-management-service", "model-engine"
]
PROCESSOR_TYPES = ["ONLINE", "OFFLINE"]
LANGUAGES = ["en_IN", "bn_IN", "hi_IN", "ta_IN", "te_IN", "mr_IN", "gu_IN"]
TIME_PERIODS = ["EARLY_MORNING", "MORNING", "AFTERNOON", "EVENING", "NIGHT", "LATE_NIGHT"]


# ==========================================
# Enriched Event Generator
# ==========================================
def generate_upi_event():
    """Generates a highly detailed synthetic UPI/eFRM transaction event."""
    # Core IDs & Timestamp
    txn_id = f"TXN{uuid.uuid4().hex[:14].upper()}"
    txn_ref_id = f"REF{uuid.uuid4().hex[:10].upper()}"
    rrn = f"{random.randint(100000000000, 999999999999)}"
    timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    
    # Payer & Payee Banks
    payer_bank = random.choice(BANKS)
    payee_bank = random.choice(BANKS)
    
    # Amount (Log-normal distribution for realistic skew)
    amount = round(random.lognormvariate(4.5, 1.2), 2)
    amount = min(max(amount, 1.0), 200000.0)
    
    # Device & OS
    os_choice = random.choice(OS_LIST)
    os_version = random.choice(ANDROID_VERSIONS if os_choice == "Android" else IOS_VERSIONS)
    
    # App mapping
    app_idx = random.randint(0, len(APPS)-1)
    app_pkg = APPS[app_idx]
    app_name = APP_NAMES[app_idx] if app_idx < len(APP_NAMES) else random.choice(APP_NAMES)

    # Risk & Fraud Logic
    risk_score = round(random.uniform(0, 100), 2)
    if random.random() < 0.03:  # 3% high risk spike
        risk_score = round(random.uniform(75, 100), 2)
        
    is_fraud = 1 if risk_score > 85 and random.random() > 0.2 else 0
    
    if risk_score >= 75: risk_level = "CRITICAL"
    elif risk_score >= 50: risk_level = "HIGH"
    elif risk_score >= 25: risk_level = "MEDIUM"
    else: risk_level = "LOW"
    
    fraud_type = random.choice([f for f in FRAUD_TYPES if f != "NONE"]) if is_fraud else "NONE"
    risk_flags = random.sample(RISK_FLAGS, k=random.randint(1, 4)) if risk_score > 50 else []
    
    # Processing Status & Response Code
    status_tuple = random.choice(RESULT_ERR_CODES)
    processing_status = status_tuple[0]
    response_code = status_tuple[1]

    event = {
        "txn_id": txn_id,
        "txn_ref_id": txn_ref_id,
        "rrn": rrn,
        "timestamp": timestamp,
        "time_period": random.choice(TIME_PERIODS),
        
        # Payer Details
        "payer_vpa": f"p{random.randint(1000,9999)}{random.choice(HANDLES)}",
        "payer_bank_code": payer_bank[0],
        "payer_bank_name": payer_bank[1],
        "payer_bank_type": random.choice(BANK_TYPES),
        "payer_psp": random.choice(PSPS),
        "payer_customer_type": random.choice(CUSTOMER_TYPES),
        "payer_account_type": random.choice(ACCOUNT_TYPES),
        
        # Payee / Merchant Details
        "payee_vpa": f"m{random.randint(1000,9999)}{random.choice(HANDLES)}",
        "payee_bank_code": payee_bank[0],
        "payee_bank_name": payee_bank[1],
        "payee_psp": random.choice(PSPS),
        "merchant_id": f"MER{random.randint(100000, 999999)}" if random.random() > 0.3 else None,
        "merchant_type": random.choice(MERCHANT_TYPES),
        "merchant_size": random.choice(MERCHANT_SIZES),
        "merchant_mcc": random.choice(MCC_CODES),
        
        # Transaction Details
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "country": random.choice(COUNTRIES),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "payment_channel": random.choice(PAYMENT_CHANNELS),
        "payment_flow": random.choice(PAYMENT_FLOWS),
        "integration_mode": random.choice(INTEGRATION_MODES),
        "purpose_code": random.choice(PURPOSE_CODES),
        "initiation_mode": random.choice(INITIATION_MODES),
        
        # Device & Network
        "app": app_pkg,
        "app_name": app_name,
        "os": os_choice,
        "os_version": os_version,
        "device_type": random.choice(DEVICE_TYPES),
        "device_brand": random.choice(DEVICE_BRANDS),
        "device_model": random.choice(DEVICE_MODELS),
        "device_id": uuid.uuid4().hex,
        "ip_address": f"{random.randint(10,250)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "telecom_operator": random.choice(TELECOM_OPERATORS),
        "network_type": random.choice(NETWORK_TYPES),
        "language": random.choice(LANGUAGES),
        
        # Location
        "zone": random.choice(ZONES),
        "state": random.choice(STATES),
        "city": random.choice(CITIES),
        
        # Auth & Processing
        "auth_method": random.choice(AUTH_METHODS),
        "auth_status": random.choice(AUTH_STATUSES),
        "processing_status": processing_status,
        "response_code": response_code,
        "decision": random.choice(DECISIONS),
        
        # Risk & eFRM Metadata
        "risk_score": risk_score,
        "risk_level": risk_level,
        "is_fraud": is_fraud,
        "fraud_type": fraud_type,
        "risk_flags": risk_flags,
        "processor_type": random.choice(PROCESSOR_TYPES),
        "service_name": random.choice(SERVICE_NAMES),
        "server_region": random.choice(SERVER_REGIONS)
    }
    return event


# ==========================================
# Worker Thread
# ==========================================
''' def worker_thread(thread_id, producer, topic, stop_event, start_time, duration, tps, num_threads, stats):
    sleep_duration = num_threads / tps if tps > 0 else 0
    count = 0
    
    while not stop_event.is_set() and (time.monotonic() - start_time) < duration:
        loop_start = time.monotonic()
        event = generate_upi_event()
        event['simulator_thread'] = thread_id
        try:
            future = producer.send(topic, value=event)

            # Wait for Kafka broker acknowledgement
            record_metadata = future.get(timeout=15)

            count += 1

            if count % 10 == 0:
                print(
                    f"[Thread {thread_id}] "
                    f"Published: {count} | "
                    f"Topic: {record_metadata.topic} | "
                    f"Partition: {record_metadata.partition} | "
                    f"Offset: {record_metadata.offset}"
                )

        except Exception as e:
            print(f"[Thread {thread_id}] Kafka publishing failed: {e}")
            stop_event.set()
            break
            
        elapsed = time.monotonic() - loop_start
        sleep_time = sleep_duration - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
            
    stats[thread_id] = count '''

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
