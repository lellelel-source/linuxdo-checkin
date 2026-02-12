"""Auto-reply engine for LinuxDo forum accounts.

Each account replies on 2 random days per week, 1 reply per day, to 1 topic.
Replies use preset natural Chinese phrases from REPLY_POOL.
"""

import hashlib
import json
import os
import random
import re
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
    "准时报到，风雨无阻",
    "日常签到，顺手点赞",
    # Supportive style
    "感谢分享，收藏了",
    "好东西，马克一下",
    "支持支持，加油加油",
    "学到了，感谢大佬",
    "这个不错，先收藏",
    "感谢楼主无私分享",
    "涨知识了，谢谢分享",
    "大佬出品，必属精品",
    "写得很用心，感谢分享",
    "干货满满，必须收藏",
    # Discussion style
    "不错不错，持续关注",
    "看看有什么新东西",
    "又学到新东西了",
    "路过看看，顺便支持",
    "好帖必须顶一下",
    "一直在关注这个方向",
    "思路很清晰，赞一个",
    "这个话题值得深入讨论",
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
    "刷论坛看到好帖，留个脚印",
    "这个帖子来得正是时候",
    # Question style (NEW)
    "请问有后续更新计划吗",
    "这个方案在生产环境验证过吗",
    "想问下性能表现怎么样",
    "有没有更详细的教程链接",
    "好奇这个是怎么实现的",
    # Humor style (NEW)
    "膜拜大佬，我先跪了",
    "看完感觉自己又行了",
    "先收藏，指不定哪天用上",
    "默默点赞然后溜了",
    "我什么时候才能写出这种东西",
    "这波操作我给满分",
    "实名羡慕，什么时候能教教我",
]

# Beijing timezone (UTC+8)
_BJT = timezone(timedelta(hours=8))

# LinuxDo category ID -> display name (used to give AI board-specific context)
CATEGORY_MAP = {
    1: "bug反馈",
    2: "功能",
    4: "一般讨论",
    5: "扯淡闲聊",
    7: "开发调优",
    10: "文档",
    11: "资源荟萃",
    13: "跳蚤市场",
    14: "非我莫属",
    15: "深度学习",
    17: "运营反馈",
    19: "福利羊毛",
    22: "搞七捻三",
    24: "靠谱推荐",
    25: "前沿快讯",
    27: "LINUX DO Wiki",
    28: "插件开发",
    34: "读书会",
    35: "AI探索",
    36: "起始页",
}


def generate_semantic_reply(title: str, content_excerpt: str = "",
                            category_name: str = "") -> Optional[str]:
    """Use Gemini Flash to generate a context-aware reply based on topic title and content.

    Returns the generated text, or None if API key is missing or call fails.
    Returns "SKIP" if AI determines the post is too negative for a casual reply.
    Fallback to REPLY_POOL is handled by the caller.
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None

    try:
        from google import genai
        client = genai.Client(api_key=api_key)

        context_part = f"\n帖子内容摘要：{content_excerpt}\n" if content_excerpt else ""
        category_part = f"这是一个发布在【{category_name}】板块的帖子。" if category_name else ""
        prompt = (
            f"你是一个热心的技术论坛用户。{category_part}"
            f"请根据帖子标题《{title}》，{context_part}"
            "写一句简短、自然、友善的中文评论。"
            "要求：1. 不要带引号 2. 不要是机器人口吻 "
            "3. 字数在 10-30 字之间 4. 可以适当带一点幽默或鼓励 "
            "5. 如果帖子内容包含强烈的负面情绪（如愤怒、悲伤、抱怨、骂人），"
            "请只输出 SKIP（不要回复这种帖子）。"
            "只输出评论内容，不要有任何前缀或解释。"
        )

        response = client.models.generate_content(
            model="gemma-3-27b-it",
            contents=prompt,
        )
        reply_text = response.text.strip().strip('"\'')

        # Sentiment filter: AI returns "SKIP" for negative/hostile posts
        if reply_text.upper().startswith("SKIP"):
            logger.info("[AI Reply] Post flagged as negative sentiment, skipping")
            return "SKIP"

        if not reply_text or len(reply_text) < 5:
            return None

        logger.info(f"[AI Reply] Generated: {reply_text}")
        return reply_text
    except Exception as e:
        logger.warning(f"[AI Reply] Gemini call failed: {e}")
        return None


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


def _fetch_topic_candidates(page, bot_usernames: set) -> list:
    """Fetch /latest.json once and return filtered candidate list.

    Returns list of dicts with 'id' and 'title', or empty list on failure.
    """
    try:
        result = page.run_js("""
            return fetch('/latest.json', {
                headers: {'X-Requested-With': 'XMLHttpRequest'}
            }).then(r => r.ok ? r.text() : '');
        """)
        if not result:
            logger.warning("[Reply] Failed to fetch /latest.json via browser")
            return []

        data = json.loads(result)
        topics = data.get("topic_list", {}).get("topics", [])

        # Build user_id -> username map from top-level users array
        users_list = data.get("users", [])
        uid_to_username = {u["id"]: u["username"] for u in users_list if "id" in u and "username" in u}
    except Exception as e:
        logger.error(f"[Reply] Error fetching topics: {e}")
        return []

    now = datetime.now(timezone.utc)
    candidates = []

    for topic in topics:
        topic_id = topic.get("id")

        # Skip pinned/banner topics
        if topic.get("pinned") or topic.get("pinned_globally"):
            continue

        # Skip topics by bot accounts — resolve user_id to username via map
        poster_username = ""
        posters = topic.get("posters", [])
        for p in posters:
            if "Original Poster" in (p.get("description", "") or ""):
                poster_username = uid_to_username.get(p.get("user_id"), "")
                break
        if poster_username in bot_usernames:
            continue
        # Also skip if last poster is a bot (prevents bot-to-bot chains)
        if topic.get("last_poster_username", "") in bot_usernames:
            continue

        # Skip topics older than 3 days
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

        title = topic.get("title", "")
        if topic_id:
            candidates.append({"id": topic_id, "title": title})

    return candidates


def select_topic(page, bot_usernames: set, exclude_ids: set = None,
                 _cached_candidates: list = None) -> Optional[Dict]:
    """Pick a suitable topic for replying.

    If _cached_candidates is provided, picks from that list (avoids re-fetching).
    Otherwise fetches /latest.json via browser JS.
    Returns dict with 'id' and 'title', or None if no suitable topic found.
    """
    if exclude_ids is None:
        exclude_ids = set()

    candidates = _cached_candidates if _cached_candidates is not None else \
        _fetch_topic_candidates(page, bot_usernames)

    # Filter out already-tried topics
    filtered = [c for c in candidates if c["id"] not in exclude_ids]

    if not filtered:
        logger.warning("[Reply] No suitable topics found")
        return None

    # Pick a random candidate
    chosen = random.choice(filtered)
    logger.info(f"[Reply] Selected topic: [{chosen['id']}] {chosen['title']}")
    return chosen


def _check_topic_status(page, topic_id: int, username: str) -> dict:
    """Check topic status: whether user already replied, plus extract first post content.

    Returns dict with:
        - already_replied: bool
        - first_post_excerpt: str (first post content, stripped HTML, max 200 chars)
        - category_id: int or None
    """
    result_dict = {"already_replied": False, "first_post_excerpt": "", "category_id": None}
    try:
        result = page.run_js(f"""
            return fetch('/t/{topic_id}.json', {{
                headers: {{'X-Requested-With': 'XMLHttpRequest'}}
            }}).then(r => r.ok ? r.text() : '');
        """)
        if not result:
            return result_dict

        data = json.loads(result)

        # Extract category_id
        result_dict["category_id"] = data.get("category_id")

        # Extract first post content (raw or cooked, strip HTML, truncate)
        post_stream = data.get("post_stream", {})
        posts = post_stream.get("posts", [])
        if posts:
            first_post = posts[0]
            # Prefer raw (markdown) over cooked (HTML)
            content = first_post.get("raw", "") or first_post.get("cooked", "")
            # Strip HTML tags if present
            content = re.sub(r"<[^>]+>", "", content).strip()
            # Truncate to 200 chars
            if len(content) > 200:
                content = content[:200] + "..."
            result_dict["first_post_excerpt"] = content

        # Check participants list — covers ALL posts regardless of pagination
        participants = data.get("details", {}).get("participants", [])
        for p in participants:
            if p.get("username", "").lower() == username.lower():
                result_dict["already_replied"] = True
                return result_dict

        # Fallback: check first page of posts (in case participants is missing)
        for post in posts:
            if post.get("username", "").lower() == username.lower():
                result_dict["already_replied"] = True
                return result_dict
    except Exception:
        pass
    return result_dict


def post_reply(page, topic_id: int, text: str, csrf_token: str) -> bool:
    """Post a reply to a topic via browser JS fetch to bypass Cloudflare."""
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

    # Escape the payload for JS
    payload_json = json.dumps(payload, ensure_ascii=False)

    try:
        result = page.run_js(f"""
            return fetch('/posts.json', {{
                method: 'POST',
                headers: {{
                    'X-CSRF-Token': '{csrf_token}',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Content-Type': 'application/json'
                }},
                body: JSON.stringify({payload_json})
            }}).then(r => r.text().then(t => JSON.stringify({{status: r.status, body: t}})));
        """)
        if not result:
            logger.error("[Reply] Post failed: no response from browser fetch")
            return False

        resp = json.loads(result)
        status = resp.get("status", 0)
        body = resp.get("body", "")

        if status == 200:
            try:
                post_data = json.loads(body)
                post_id = post_data.get("id", "?")
                logger.success(f"[Reply] Posted successfully! post_id={post_id}, topic_id={topic_id}")
            except Exception:
                logger.success(f"[Reply] Posted successfully! topic_id={topic_id}")
            return True
        else:
            logger.error(f"[Reply] Post failed: {status} - {body[:200]}")
            return False
    except Exception as e:
        logger.error(f"[Reply] Post request error: {e}")
        return False


def execute_reply(browser, bot_usernames: set = None, used_topics: set = None,
                  used_phrases: set = None, force: bool = False) -> Optional[Dict]:
    """Main entry point: decide whether to reply and do it.

    Args:
        browser: LinuxDoBrowser instance (needs .page, .username, ._csrf_token)
        bot_usernames: set of usernames belonging to bot accounts (for filtering)
        used_topics: set of topic IDs already replied to in this job (anti-same-IP detection)
        used_phrases: set of phrases already used in this job (anti-duplicate detection)
        force: if True, skip day/slot scheduling check (one-time force-reply mode)

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
    if force:
        logger.info(f"[Reply] {username}: FORCE_REPLY_ALL mode — skipping schedule check")
    elif not should_reply_today(username):
        return None

    page = browser.page

    # Ensure page is on linux.do before relative JS fetch calls
    try:
        current_url = page.url or ""
        if "linux.do" not in current_url:
            logger.info(f"[Reply] {username}: navigating back to linux.do (was on {current_url})")
            page.get("https://linux.do/")
            time.sleep(random.uniform(2, 4))
    except Exception as e:
        logger.warning(f"[Reply] {username}: domain check failed: {e}")

    # Refresh CSRF token — the one from login may be stale after a long browse session
    csrf_token = None
    try:
        fresh_csrf = page.run_js("""
            return fetch('/session/csrf', {
                headers: {'X-Requested-With': 'XMLHttpRequest'}
            }).then(r => r.json()).then(d => d.csrf);
        """)
        if fresh_csrf:
            csrf_token = fresh_csrf
            browser._csrf_token = fresh_csrf
            logger.info(f"[Reply] {username}: refreshed CSRF token: {fresh_csrf[:10]}...")
    except Exception as e:
        logger.warning(f"[Reply] {username}: CSRF refresh failed: {e}")

    if not csrf_token:
        csrf_token = getattr(browser, "_csrf_token", None)
    if not csrf_token:
        logger.warning(f"[Reply] {username}: no CSRF token available, skipping reply")
        return None

    # Fetch topic candidates once, then pick from cached list in retry loop
    cached_candidates = _fetch_topic_candidates(page, bot_usernames)

    # Select a topic with retry loop (max 3 attempts)
    tried_ids = set()
    topic = None
    topic_status = None
    for attempt in range(3):
        candidate = select_topic(
            page, bot_usernames, exclude_ids=tried_ids,
            _cached_candidates=cached_candidates
        )
        if not candidate:
            break
        tried_ids.add(candidate["id"])

        # Anti-detection: skip if another account in this job already replied to this topic
        if candidate["id"] in used_topics:
            logger.info(f"[Reply] {username}: topic {candidate['id']} used by another account, retry {attempt+1}/3")
            continue

        # Check topic status (already replied + extract first post content)
        status = _check_topic_status(page, candidate["id"], username)
        if status["already_replied"]:
            logger.info(f"[Reply] {username}: already replied to {candidate['id']}, retry {attempt+1}/3")
            continue

        topic = candidate
        topic_status = status
        break

    if not topic:
        logger.info(f"[Reply] {username}: no suitable topic found after retries")
        return None

    topic_id = topic["id"]
    topic_title = topic["title"]
    first_post_excerpt = topic_status.get("first_post_excerpt", "") if topic_status else ""
    category_id = topic_status.get("category_id") if topic_status else None
    category_name = CATEGORY_MAP.get(category_id, "") if category_id else ""

    # Generate reply text: try Gemini AI first (with content + category context), fall back to REPLY_POOL
    reply_text = generate_semantic_reply(
        topic_title, content_excerpt=first_post_excerpt, category_name=category_name
    )

    # Sentiment filter: AI flagged this post as negative, skip entirely
    if reply_text == "SKIP":
        logger.info(f"[Reply] {username}: skipping topic {topic_id} (negative sentiment)")
        return None

    is_ai = reply_text is not None

    if not reply_text:
        # Fallback: pick a phrase that hasn't been used by another account in this job
        available_phrases = [p for p in REPLY_POOL if p not in used_phrases]
        if not available_phrases:
            available_phrases = list(REPLY_POOL)
        reply_text = random.choice(available_phrases)

    source = "AI" if is_ai else "pool"
    logger.info(f"[Reply] {username}: replying to topic {topic_id} ({source}): {reply_text}")

    # C4: Navigate to topic page and simulate reading before posting
    try:
        logger.info(f"[Reply] {username}: navigating to topic {topic_id} for read simulation")
        page.get(f"https://linux.do/t/{topic_id}")
        time.sleep(random.uniform(2, 5))
        for _ in range(random.randint(2, 4)):
            page.run_js(f"window.scrollBy({{top: {random.randint(200, 600)}, behavior: 'smooth'}})")
            time.sleep(random.uniform(1.5, 4))
        page.run_js("window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'})")
        time.sleep(random.uniform(1, 2))

        # "Like what you reply to" — 80% chance to like OP before replying
        if random.random() < 0.80:
            try:
                liked = page.run_js("""
                    // Scroll back to top to find OP's like button
                    window.scrollTo({top: 0, behavior: 'smooth'});
                    // Wait a moment for scroll
                    return new Promise(resolve => setTimeout(() => {
                        const firstPost = document.querySelector('article#post_1')
                                       || document.querySelector('.topic-post:first-child');
                        if (!firstPost) { resolve('no_post'); return; }
                        const btn = firstPost.querySelector('.discourse-reactions-reaction-button')
                                 || firstPost.querySelector('button.toggle-like')
                                 || firstPost.querySelector('.like-button');
                        if (!btn) { resolve('no_btn'); return; }
                        if (btn.classList.contains('has-like') || btn.classList.contains('liked'))
                            { resolve('already_liked'); return; }
                        btn.click();
                        resolve('liked');
                    }, 800));
                """)
                if liked == 'liked':
                    logger.info(f"[Reply] {username}: liked OP in topic {topic_id}")
                    time.sleep(random.uniform(0.8, 2.0))
                elif liked == 'already_liked':
                    logger.info(f"[Reply] {username}: OP already liked in topic {topic_id}")
                else:
                    logger.info(f"[Reply] {username}: couldn't like OP ({liked})")
            except Exception as e:
                logger.warning(f"[Reply] {username}: like-on-reply failed (non-fatal): {e}")
    except Exception as e:
        logger.warning(f"[Reply] {username}: read simulation failed (non-fatal): {e}")

    success = post_reply(page, topic_id, reply_text, csrf_token)

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
