"""Collect results from all jobs and send a single summary email."""
import glob
import json
import os
from notify import NotificationManager


def main():
    all_success = []
    all_fail = []
    total = 0

    # Find all result files from job artifacts
    for path in sorted(glob.glob("results/*/results_job_*.json")):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        total += data.get("total", 0)
        all_success.extend(data.get("success", []))
        all_fail.extend(data.get("fail", []))
        print(f"Loaded {path}: {len(data.get('success', []))} success, {len(data.get('fail', []))} fail")

    if total == 0:
        print("No results found.")
        return

    print(f"\nTotal: {total} | Success: {len(all_success)} | Failed: {len(all_fail)}")

    summary_lines = [
        f"Total: {total} | Success: {len(all_success)} | Failed: {len(all_fail)}",
        "",
        f"✅ Successful ({len(all_success)}):",
    ]
    summary_lines += [f"  - {u}" for u in all_success] if all_success else ["  (none)"]
    summary_lines += ["", f"❌ Failed ({len(all_fail)}):"]
    summary_lines += [f"  - {u}" for u in all_fail] if all_fail else ["  (none)"]

    notifier = NotificationManager()
    notifier.send_email("LinuxDo Check-in Summary", "\n".join(summary_lines))


if __name__ == "__main__":
    main()
