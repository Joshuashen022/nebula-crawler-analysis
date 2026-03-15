
import subprocess
import time
from pathlib import Path

import config

INTERVAL_SECONDS = 5 * 60  # 5 minutes

ROOT_DIR = Path(__file__).resolve().parent

DB_HOST = config.DB_HOST
INTERVAL_COUNT = config.INTERVAL_COUNT

CMD1 = f"../dist/nebula --db-user joshua --db-name nebula_local --db-host {DB_HOST} crawl --neighbors"
CMD2 = "../dist/nebula --json-out ./results/ crawl --neighbors"
CMD3 = f"../dist/nebula --db-user joshua --db-name nebula_local --db-host {DB_HOST} resolve --maxmind-asn ../database/GeoLite2-ASN.mmdb --maxmind-country ../database/GeoLite2-Country.mmdb"
CMD4 = f"../dist/nebula --db-user joshua --db-name nebula_local --db-host {DB_HOST} monitor"
analysis_count = 0
run_count = 0
intervals_since_last_crawl = 0

def run_cmd(cmd: str) -> bool:
    """Run a command and return whether it succeeded"""
    print(f"==> running: {cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=ROOT_DIR,
        )
    except Exception as e:
        print(f"command failed with error: {e}")
        return False
    if result.returncode != 0:
        print(f"command failed with code {result.returncode}")
        return False
    print("command succeeded")
    return True

def run_analysis():
    global analysis_count
    ok = run_cmd(CMD3)
    analysis_count += 1
    print(f"CMD3: {ok} time: {time.time()}")
    return ok

def run_monitor():
    ok = run_cmd(CMD4)
    print(f"CMD4: {ok} time: {time.time()}")
    return ok

def run():
    global run_count, intervals_since_last_crawl
    print("start scheduler, crawl every 30 minutes, checkalive every 5 minutes. press Ctrl+C to stop.")
    try:
        # run crawl commands immediately once at start
        cycle_start = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n=== crawl cycle at {cycle_start} ===")
        ok = run_cmd(CMD1)
        print(f"CMD1: {ok} time: {time.time()}")
        ok = run_cmd(CMD2)
        print(f"CMD2: {ok} time: {time.time()}")
        run_count += 1
        # then enter 5-minute heartbeat loop, triggering crawl every 6 intervals

        while True:
            cycle_start = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{cycle_start}] checkalive")
            intervals_since_last_crawl += 1

            # every 6 * 5 minutes = 30 minutes, run crawl again
            if intervals_since_last_crawl >= INTERVAL_COUNT:
                print(f"\n=== crawl cycle at {cycle_start} ===")
                ok = run_cmd(CMD1)
                print(f"CMD1: {ok} time: {time.time()}")
                ok = run_cmd(CMD2)
                print(f"CMD2: {ok} time: {time.time()}")
                intervals_since_last_crawl = 0
                run_count += 1

            print(f"sleep {INTERVAL_SECONDS} seconds...\n")
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nstopped by user.")


if __name__ == "__main__":
    run()