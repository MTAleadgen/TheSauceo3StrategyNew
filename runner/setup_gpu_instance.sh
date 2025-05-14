#!/bin/bash
set -e

# Update and install system dependencies
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv git

# Clone or update the repo
REPO_URL="https://github.com/MTAleadgen/TheSauceo3StrategyNew.git"
REPO_DIR="TheSauceo3StrategyNew"
if [ ! -d "$REPO_DIR" ]; then
  git clone "$REPO_URL"
else
  cd "$REPO_DIR"
  git pull
  cd ..
fi

cd "$REPO_DIR"

# Set up Python venv
if [ ! -d venv ]; then
  python3 -m venv venv
fi
source venv/bin/activate

# Install requirements
if [ -f requirements.txt ]; then
  pip install -r requirements.txt
else
  pip install requests python-dotenv supabase-py
fi

# Show GPU info
if command -v nvidia-smi &> /dev/null; then
  echo "\nCUDA/GPU info:"
  nvidia-smi
else
  echo "nvidia-smi not found. CUDA drivers may not be installed."
fi

echo "\nSetup complete. You can now run:"
echo "python3 runner/clean_events.py" 