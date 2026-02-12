"""Collect results from all jobs and send a single summary email."""
import glob
import json
import os
from notify import NotificationManager

TRUST_LEVEL_NAMES = {
    0: "新用户 (TL0)",
    1: "基本用户 (TL1)",
    2: "成员 (TL2)",
    3: "活跃用户 (TL3)",
    4: "领导者 (TL4)",
}


def main():
    all_success = []
    all_fail = []
    all_replies = []
    all_connect_infos = {}  # username -> {trust_level, table: [{item, current, requirement}, ...]}
    total = 0

    # Find all result files from job artifacts
    for path in sorted(glob.glob("results/*/results_job_*.json")):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        total += data.get("total", 0)
        all_success.extend(data.get("success", []))
        all_fail.extend(data.get("fail", []))
        all_replies.extend(data.get("replied_accounts", []))
        all_connect_infos.update(data.get("connect_infos", {}))
        print(f"Loaded {path}: {len(data.get('success', []))} success, {len(data.get('fail', []))} fail, {len(data.get('replied_accounts', []))} replies")

    if total == 0:
        print("No results found.")
        return

    print(f"\nTotal: {total} | Success: {len(all_success)} | Failed: {len(all_fail)} | Replies: {len(all_replies)}")

    summary_lines = [
        f"Total: {total} | Success: {len(all_success)} | Failed: {len(all_fail)} | Replies: {len(all_replies)}",
        "",
        f"✅ Successful ({len(all_success)}):",
    ]
    summary_lines += [f"  - {u}" for u in all_success] if all_success else ["  (none)"]
    summary_lines += ["", f"❌ Failed ({len(all_fail)}):"]
    summary_lines += [f"  - {u}" for u in all_fail] if all_fail else ["  (none)"]

    summary_lines += ["", f"💬 Replies ({len(all_replies)}):"]
    if all_replies:
        for r in all_replies:
            topic_url = f"https://linux.do/t/{r['topic_id']}"
            summary_lines.append(f"  - {r['username']} -> {r['topic_title']} ({topic_url})")
            summary_lines.append(f"    \"{r['reply_text']}\"")
    else:
        summary_lines.append("  (none)")

    # Account level & connect info section
    summary_lines += ["", f"📊 Account Levels & Connect Info ({len(all_connect_infos)}):"]
    if all_connect_infos:
        for username in sorted(all_connect_infos.keys()):
            info = all_connect_infos[username]
            trust_level = info.get("trust_level")
            level_name = TRUST_LEVEL_NAMES.get(trust_level, f"Unknown ({trust_level})")
            summary_lines.append(f"  [{username}] Trust Level: {level_name}")

            table = info.get("table", [])
            if table:
                for row in table:
                    item = row.get("item", "")
                    current = row.get("current", "0")
                    requirement = row.get("requirement", "0")
                    summary_lines.append(f"    {item}: {current} / {requirement}")
            summary_lines.append("")  # blank line between accounts
    else:
        summary_lines.append("  (none)")

    notifier = NotificationManager()
    notifier.send_email("LinuxDo Check-in Summary", "\n".join(summary_lines))


if __name__ == "__main__":
    main()
