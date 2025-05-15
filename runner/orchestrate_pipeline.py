import subprocess
import sys

def run_step(description, command):
    print(f"\n[ORCHESTRATOR] {description}...")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"[ORCHESTRATOR] Step failed: {description}")
        sys.exit(result.returncode)
    print(f"[ORCHESTRATOR] Step completed: {description}")

if __name__ == "__main__":
    # 1. Run cli.py to collect events from SerpAPI
    run_step(
        "Collecting events from SerpAPI (cli.py)",
        "python runner/cli.py --mode serpapi_events --cities data/cities_shortlist.csv"
    )

    # 2. Run deduplicate_events.py to remove duplicates
    run_step(
        "Deduplicating events (deduplicate_events.py)",
        "python runner/deduplicate_events.py"
    )

    # 3. Run enrich_venues.py to enrich venues
    run_step(
        "Enriching venues (enrich_venues.py)",
        "python runner/enrich_venues.py"
    )

    # 4. Run instance_creator.py to launch GPU, enrich events with LLM, and move to events_clean
    run_step(
        "Launching GPU and running LLM event cleaning (instance_creator.py)",
        "python runner/instance_creator.py"
    )

    print("\n[ORCHESTRATOR] Pipeline completed successfully!") 