"""Auto-reply engine for LinuxDo forum accounts.

Each account replies on 2 random days per week, 1 reply per day, to 1 topic.
Replies use preset natural Chinese phrases from REPLY_POOL.
"""

import hashlib
import random
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Set

from loguru import logger

REPLY_POOL = [
    # Check-in style
    "来了来了，每日打卡",
    "前排支持一下",
    "先赞后看，养成好习惯",
    "前排占座，支持楼主",
    "每天都来报个到",
    "打卡签到，顺便支持",
    # Supportive style
    "感谢分享，收藏了",
    "好东西，马克一下",
    "支持支持，加油加油",
    "学到了，感谢大佬",
    "这个不错，先收藏",
    "感谢楼主无私分享",
    "涨知识了，谢谢分享",
    "大佬出品，必属精品",
    # Discussion style
    "不错不错，持续关注",
    "看看有什么新东西",
    "又学到新东西了",
    "路过看看，顺便支持",
    "好帖必须顶一下",
    "一直在关注这个方向",
    # Casual style
    "刚好需要，太及时了",
    "这个值得收藏起来",
    "终于等到更新了",
    "坐等后续更新内容",
    "越来越好了，继续加油",
    "日常逛论坛，支持一波",
    "有意思，回头试试看",
    "正好在找这个，谢了",
    "持续关注中，期待后续",
    "不明觉厉，先收藏了",
]

# Beijing timezone (UTC+8)
_BJT = timezone(timedelta(hours=8))


def get_reply_run(username: str) -> str:
    """Return 'morning' or 'evening' — which daily run this account replies in."""
    h = int(hashlib.md5(username.encode()).hexdigest(), 16)
    return "morning" if h % 2 == 0 else "evening"


def get_active_days(username: str, week_number: int) -> list:
    """Return 2 day indices (0=Mon..6=Sun) for this account this week."""
    seed = int(hashlib.md5(f"{username}:{week_number}".encode()).hexdigest(), 16)
    rng = random.Random(seed)
    return sorted(rng.sample(range(7), 2))


def _current_run_slot() -> str:
    """Determine if this is a 'morning' or 'evening' run based on UTC hour."""
    utc_hour = datetime.now(timezone.utc).hour
    # cron 23 2 * * * (02:23 UTC) -> morning
    # cron 47 14 * * * (14:47 UTC) -> evening
    return "morning" if utc_hour < 10 else "evening"


def should_reply_today(username: str) -> bool:
    """Check if this account should reply in the current run."""
    now_bjt = datetime.now(_BJT)
    week_number = now_bjt.isocalendar()[1]
    weekday = now_bjt.weekday()  # 0=Mon..6=Sun

    active_days = get_active_days(username, week_number)
    if weekday not in active_days:
        logger.info(f"[Reply] {username}: not an active day (active={active_days}, today={weekday})")
        return False

    run_slot = get_reply_run(username)
    current_slot = _current_run_slot()
    if run_slot != current_slot:
        logger.info(f"[Reply] {username}: wrong run slot (assigned={run_slot}, current={current_slot})")
        return False

    logger.info(f"[Reply] {username}: should reply today (day={weekday}, slot={current_slot})")
    return True


def select_topic(session, bot_usernames: set, impersonate: str) -> Optional[Dict]:
    """Fetch recent topics and pick one suitable for replying.

    Returns dict with 'id' and 'title', or None if no suitable topic found.
    """
    try:
        resp = session.get(
            "https://linux.do/latest.json",
            impersonate=impersonate,
        )
        if resp.status_code != 200:
            logger.warning(f"[Reply] Failed to fetch /latest.json: {resp.status_code}")
            return None

        data = resp.json()
        topics = data.get("topic_list", {}).get("topics", [])
    except Exception as e:
        logger.error(f"[Reply] Error fetching topics: {e}")
        return None

    now = datetime.now(timezone.utc)
    candidates = []

    for topic in topics:
        # Skip pinned/banner topics
        if topic.get("pinned") or topic.get("pinned_globally"):
            continue

        # Skip topics by bot accounts
        poster_username = ""
        posters = topic.get("posters", [])
        if posters:
            # The first poster with description containing "Original Poster" is the creator
            for p in posters:
                if "Original Poster" in (p.get("description", "") or ""):
                    poster_username = str(p.get("user_id", ""))
                    break

        # Also check last_poster_username which is always available
        creator = topic.get("last_poster_username", "")

        # Skip if created within the last 3 days check via created_at
        created_at = topic.get("created_at", "")
        if created_at:
            try:
                created_time = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                age_days = (now - created_time).total_seconds() / 86400
                if age_days > 3:
                    continue
            except (ValueError, TypeError):
                continue

        # Skip mega-threads (>100 replies)
        if topic.get("posts_count", 0) > 100:
            continue

        # Skip closed/archived topics
        if topic.get("closed") or topic.get("archived"):
            continue

        topic_id = topic.get("id")
        title = topic.get("title", "")
        if topic_id:
            candidates.append({"id": topic_id, "title": title})

    if not candidates:
        logger.warning("[Reply] No suitable topics found")
        return None

    # Pick a random candidate
    chosen = random.choice(candidates)
    logger.info(f"[Reply] Selected topic: [{chosen['id']}] {chosen['title']}")
    return chosen


def _check_already_replied(session, topic_id: int, username: str, impersonate: str) -> bool:
    """Check if the current user already replied to this topic."""
    try:
        resp = session.get(
            f"https://linux.do/t/{topic_id}.json",
            impersonate=impersonate,
        )
        if resp.status_code != 200:
            return False

        data = resp.json()
        post_stream = data.get("post_stream", {})
        posts = post_stream.get("posts", [])
        for post in posts:
            if post.get("username", "").lower() == username.lower():
                return True
    except Exception:
        pass
    return False


def post_reply(session, topic_id: int, text: str, csrf_token: str, impersonate: str) -> bool:
    """Post a reply to a topic via Discourse API."""
    typing_duration = random.randint(5000, 15000)
    composer_duration = random.randint(10000, 30000)

    # Simulate actual typing/composing time
    wait_secs = composer_duration / 1000
    logger.info(f"[Reply] Simulating compose time: {wait_secs:.1f}s")
    time.sleep(wait_secs)

    payload = {
        "raw": text,
        "topic_id": topic_id,
        "typing_duration_msecs": typing_duration,
        "composer_open_duration_msecs": composer_duration,
        "nested_post": True,
    }

    headers = {
        "X-CSRF-Token": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
    }

    try:
        resp = session.post(
            "https://linux.do/posts.json",
            json=payload,
            headers=headers,
            impersonate=impersonate,
        )
        if resp.status_code == 200:
            post_data = resp.json()
            post_id = post_data.get("id", "?")
            logger.success(f"[Reply] Posted successfully! post_id={post_id}, topic_id={topic_id}")
            return True
        else:
            logger.error(f"[Reply] Post failed: {resp.status_code} - {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"[Reply] Post request error: {e}")
        return False


def execute_reply(browser, bot_usernames: set = None, used_topics: set = None,
                  used_phrases: set = None) -> Optional[Dict]:
    """Main entry point: decide whether to reply and do it.

    Args:
        browser: LinuxDoBrowser instance (needs .session, ._impersonate, .username, ._csrf_token)
        bot_usernames: set of usernames belonging to bot accounts (for filtering)
        used_topics: set of topic IDs already replied to in this job (anti-same-IP detection)
        used_phrases: set of phrases already used in this job (anti-duplicate detection)

    Returns:
        Dict with reply details on success, None otherwise.
    """
    if bot_usernames is None:
        bot_usernames = set()
    if used_topics is None:
        used_topics = set()
    if used_phrases is None:
        used_phrases = set()

    username = browser.username
    if not should_reply_today(username):
        return None

    csrf_token = getattr(browser, "_csrf_token", None)
    if not csrf_token:
        logger.warning(f"[Reply] {username}: no CSRF token available, skipping reply")
        return None

    # Select a topic
    topic = select_topic(browser.session, bot_usernames, browser._impersonate)
    if not topic:
        return None

    topic_id = topic["id"]
    topic_title = topic["title"]

    # Anti-detection: skip if another account in this job already replied to this topic
    if topic_id in used_topics:
        logger.info(f"[Reply] {username}: topic {topic_id} already used by another account in this job, skipping")
        return None

    # Check if we already replied to this topic
    if _check_already_replied(browser.session, topic_id, username, browser._impersonate):
        logger.info(f"[Reply] {username}: already replied to topic {topic_id}, skipping")
        return None

    # Pick a phrase that hasn't been used by another account in this job
    available_phrases = [p for p in REPLY_POOL if p not in used_phrases]
    if not available_phrases:
        available_phrases = list(REPLY_POOL)  # fallback: reuse if all 30 are used
    reply_text = random.choice(available_phrases)

    logger.info(f"[Reply] {username}: replying to topic {topic_id} with: {reply_text}")

    success = post_reply(
        browser.session, topic_id, reply_text, csrf_token, browser._impersonate
    )

    if success:
        used_topics.add(topic_id)
        used_phrases.add(reply_text)
        return {
            "username": username,
            "topic_id": topic_id,
            "topic_title": topic_title,
            "reply_text": reply_text,
        }

    return None
