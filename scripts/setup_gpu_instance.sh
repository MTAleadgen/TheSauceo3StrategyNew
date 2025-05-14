#!/usr/bin/env bash
set -euo pipefail

# 1) System deps
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv git

# 2) Clone or update your repo
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

# 3) Python venv & deps
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
if [ -f requirements.txt ]; then
  pip install -r requirements.txt
else
  pip install requests python-dotenv supabase-py
fi

# 4) (Optional) Copy .env if you have secrets there
# scp your .env from your control host, or echo vars here.

# 5) GPU check
if command -v nvidia-smi &>/dev/null; then
  echo "✅ GPU detected:"
  nvidia-smi
else
  echo "⚠️ GPU not detected—check drivers."
fi

echo "✅ Bootstrap complete. You can now run your pipeline, e.g.:"
echo "   source venv/bin/activate"
echo "   python -m runner.cli --mode serpapi_events …"
echo "   python -m runner.cli --mode clean …" 