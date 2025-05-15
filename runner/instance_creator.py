import os
import time
import requests
from dotenv import load_dotenv
import subprocess
import json
from requests.exceptions import ConnectionError

load_dotenv()

LAMBDA_API_URL = "https://cloud.lambdalabs.com/api/v1/instance-operations"
LAMBDA_API_KEY = os.getenv("LAMBDA_API_KEY")
GPU_INSTANCE_TYPE = os.getenv("GPU_INSTANCE_TYPE", "gpu_1x_a10")
SSH_KEY_NAME = os.getenv("SSH_KEY_NAME")
SSH_KEY = os.getenv("SSH_KEY")
PRIVATE_SSH_KEY_PATH = os.path.expanduser(os.getenv("PRIVATE_SSH_KEY_PATH", "~/.ssh/TheSauce"))

# Configurable parameters
REGIONS = [
    "us-west-1", "us-west-2", "us-west-3", "us-south-1", "us-south-2", "us-south-3",
    "us-east-1", "us-east-2", "us-midwest-1", "europe-central-1", "asia-northeast-1",
    "asia-northeast-2", "asia-south-1", "me-west-1"
]
OUTER_TRIES = 5  # Number of times to repeat the region cycle
PAUSE_BETWEEN_CYCLES = 300  # Seconds to wait between cycles

# Note: This script is intended to be run from WSL or Linux. Activate your venv before running:
# source /path/to/venv/bin/activate

headers = {
    "Authorization": f"Bearer {LAMBDA_API_KEY}",
    "Content-Type": "application/json"
}

def create_instance(region, max_retries=3, retry_delay=5):
    payload = {
        "region_name":   region,
        "instance_type_name": GPU_INSTANCE_TYPE,
        "ssh_key_names": [SSH_KEY_NAME],
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(f"{LAMBDA_API_URL}/launch", headers=headers, json=payload)
            data = resp.json()
            print("DEBUG: launch response →", json.dumps(data, indent=2))  # <— inspect this output!

            if resp.status_code != 200:
                print(f"Error launching in {region}: {resp.status_code} {resp.text}")
                return None

            # try various paths to the instance ID:
            instance_id = (
                data.get("instance_id")
                or data.get("id")
                or (data.get("data", {}) or {}).get("instance_id")
                or (data.get("data", {}) or {}).get("id")
                or ((data.get("data", {}) or {}).get("instance_ids", [None])[0])
            )

            if not instance_id:
                print("ERROR: no instance_id found in response!")
                return None

            print(f"[+] Launched {GPU_INSTANCE_TYPE} in {region}, instance_id={instance_id}")
            return instance_id
        except ConnectionError as e:
            print(f"[!] Network error (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                print(f"[!] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("[!] Max retries reached. Giving up on this region.")
                return None

def wait_for_ip(instance_id, poll_interval=5, max_retries=3, retry_delay=5):
    print(f"[…] Waiting for public IP of instance {instance_id} …")
    poll_count = 0
    while True:
        poll_count += 1
        print(f"[ ] Poll #{poll_count}: Checking instance status…")
        for attempt in range(1, max_retries + 1):
            try:
                r = requests.get("https://cloud.lambdalabs.com/api/v1/instances", headers=headers)
                r.raise_for_status()
                break  # Success, break out of retry loop
            except ConnectionError as e:
                print(f"[!] Network error while polling for IP (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    print(f"[!] Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("[!] Max retries reached while polling for IP. Exiting.")
                    raise
        # Print the full response for debugging
        print("[DEBUG] Full API response:")
        try:
            print(json.dumps(r.json(), indent=2))
        except Exception as e:
            print(f"[DEBUG] Could not decode JSON response: {e}")
        found = False
        for inst in r.json().get("data", []):
            print(f"[DEBUG] Instance: id={inst.get('id')}, status={inst.get('status')}, ip={inst.get('ip')}")
            if inst.get("id") == instance_id:
                if inst.get("ip"):
                    ip = inst["ip"]
                    print(f"[+] Instance {instance_id} is up at {ip}")
                    return ip
                found = True
        if not found:
            print(f"[ ] Instance {instance_id} not found in API response yet.")
        time.sleep(poll_interval)

if __name__ == "__main__":
    if not LAMBDA_API_KEY or not SSH_KEY_NAME:
        print("Missing required environment variables. Please set LAMBDA_API_KEY and SSH_KEY_NAME.")
        exit(1)
    instance_id = None
    for attempt in range(1, OUTER_TRIES + 1):
        print(f"\n=== Attempt {attempt}/{OUTER_TRIES} ===")
        for region in REGIONS:
            print(f"[ ] Trying {GPU_INSTANCE_TYPE} in {region}…")
            inst_id = create_instance(region)
            if inst_id:
                instance_id = inst_id
                break
            time.sleep(30)
        if instance_id:
            print("[+] Successfully launched. Moving on.")
            break
        if attempt < OUTER_TRIES:
            print(f"[ ] Sleeping {PAUSE_BETWEEN_CYCLES}s before retry cycle…")
            time.sleep(PAUSE_BETWEEN_CYCLES)

    if not instance_id:
        print("[-] All attempts exhausted. No instance launched.")
        exit(1)

    # ── At this point we have instance_id, now wait for its IP ──
    ip = wait_for_ip(instance_id)

    # ── SSH in, install dependencies, pull repo, and run clean_events.py ──
    print(f"[ ] Installing dependencies and pulling repo on {ip}…")
    ssh_base = [
        "ssh",
        "-i", PRIVATE_SSH_KEY_PATH,
        "-o", "StrictHostKeyChecking=no",
        f"ubuntu@{ip}"
    ]
    # Update and install system deps, python, git, venv
    subprocess.run(ssh_base + [
        "sudo apt-get update && sudo apt-get install -y python3 python3-pip python3-venv git"
    ], check=True)
    # Clone or pull repo
    subprocess.run(ssh_base + [
        "if [ ! -d TheSauceo3StrategyNew ]; then git clone https://github.com/MTAleadgen/TheSauceo3StrategyNew.git; else cd TheSauceo3StrategyNew && git pull && cd ..; fi"
    ], check=True)
    # Set up venv and install requirements
    subprocess.run(ssh_base + [
        "cd TheSauceo3StrategyNew && python3 -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt"
    ], check=True)
    # Run clean_events.py
    print(f"[ ] Running clean_events.py on {ip}…")
    subprocess.run(ssh_base + [
        "cd TheSauceo3StrategyNew && source venv/bin/activate && python runner/clean_events.py"
    ], check=True)
    print(f"[✓] Remote instance {ip} is set up and clean_events.py has run.") 