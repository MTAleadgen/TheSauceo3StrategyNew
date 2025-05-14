#!/usr/bin/env python3
import os, time, subprocess, json, requests, sys, pathlib

LAMBDA = "https://cloud.lambdalabs.com/api/v1/instance-types"
# Move TOKEN and HEAD initialization inside main conditional block
# TOKEN  = os.environ["LAMBDA_API_KEY"]
# HEAD   = {"Authorization": f"Bearer {TOKEN}"}

def spin_up(headers):
    print("🟢  Requesting GPU…")
    resp = requests.post(
        "https://cloud.lambdalabs.com/api/v1/instance-operations/launch",
        json = {
            "region_name":  os.getenv("GPU_REGION"),
            "instance_type_name": os.getenv("GPU_INSTANCE_TYPE"),
            "ssh_key_name": os.getenv("SSH_KEY_NAME"),
            "template_id":  os.getenv("LAMBDA_TEMPLATE_ID"),
            "billing_type": "hourly"
        },
        headers = headers, timeout = 30 # Pass headers in
    ); resp.raise_for_status()
    return resp.json()["instance_id"]

def wait_for_ip(iid, headers):
    while True:
        r = requests.get(f"https://cloud.lambdalabs.com/api/v1/instances/{iid}",
                         headers=headers, timeout=10).json() # Pass headers in
        state = r["data"]["instance"]["status"]
        if state == "active":
            ip = r["data"]["instance"]["ip"]
            print(f"🟢  Instance ready @ {ip}")
            return ip
        print("⌛  waiting (state =", state, ")"); time.sleep(15)

def ssh(ip, cmd):
    subprocess.check_call(["ssh", f"ubuntu@{ip}", "-o", "StrictHostKeyChecking=no", cmd])

def main(token, headers):
    iid = spin_up(headers)
    ip  = wait_for_ip(iid, headers)

    # 1) make sure deps for the LLM are installed
    #    Assumes the code and requirements.txt are already on the instance template
    #    at /home/ubuntu/llm/
    print("📦 Installing dependencies on GPU via SSH...")
    ssh(ip, "pip install -r /home/ubuntu/llm/requirements.txt") # Path on the GPU instance
    print("✅ Dependencies installed.")

    # 2) export dynamic URL + token into this shell & run the pipeline
    #    The LAMBDA_TOKEN for the Qwen endpoint is the same as LAMBDA_API_KEY for Lambda Labs API
    os.environ["LAMBDA_QWEN_URL"] = f"http://{ip}:8000/inference" # Assuming Qwen served on port 8000
    os.environ["LAMBDA_TOKEN"]    = token                              # Use passed token
    
    print(f"🔧 LAMBDA_QWEN_URL set to: {os.environ['LAMBDA_QWEN_URL']}")
    print("🚀 Starting SERPAPI Events pipeline...")
    # serpapi_events first
    # Ensure the main project directory is correctly referenced if needed, or that cli.py is findable
    # The command below assumes runner.cli can be found by python -m
    # This also assumes that the cities file is accessible or its path correctly handled by runner.cli
    # Default cities file is data/cities_shortlist.csv, ensure it's part of the GPU template or accessible.
    subprocess.check_call(
        [sys.executable, "-m", "runner.cli", "--mode", "serpapi_events",
         "--cities", "data/cities_shortlist.csv", # Explicitly providing default path
         "--max-cities", "1250", "--days-forward", "31"]
    )
    print("✅ SERPAPI Events pipeline finished.")
    
    print("🚀 Starting Clean pipeline...")
    # then clean & enrich
    subprocess.check_call(
        [sys.executable, "-m", "runner.cli", "--mode", "clean"]
    )
    print("✅ Clean pipeline finished.")

    # 3) destroy GPU to save $$
    print("🛑  Turning instance off")
    requests.post(
        "https://cloud.lambdalabs.com/api/v1/instance-operations/terminate",
        json={"instance_id": iid}, headers=headers, timeout=30 # Pass headers in
    ).raise_for_status()
    print("🎉 GPU instance terminated. Weekly run complete.")

if __name__ == "__main__":
    # Load .env variables if this script is run directly and an .env file is present
    # For systemd, EnvironmentFile in the .service unit is preferred.
    from dotenv import load_dotenv
    dotenv_path = pathlib.Path(__file__).resolve().parent.parent / '.env' # Assuming .env is in project root
    if dotenv_path.exists():
        print(f"Found .env file at {dotenv_path}, loading variables for local run.")
        load_dotenv(dotenv_path=dotenv_path)
    else:
        print("No .env file found in project root for local run, relying on pre-set environment variables.")

    # Initialize TOKEN and HEAD *after* potential .env load
    if "LAMBDA_API_KEY" in os.environ:
        TOKEN  = os.environ["LAMBDA_API_KEY"]
        HEAD   = {"Authorization": f"Bearer {TOKEN}"}
    else:
        print("Error: LAMBDA_API_KEY not found in environment. Cannot proceed.")
        sys.exit(1)
        
    # Pass token and headers into main function
    main(TOKEN, HEAD) 