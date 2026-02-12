"""
cron: 0 */6 * * *
new Env("Linux.Do 签到")
"""

import os
import random
import time
import json
import hashlib
import functools
import threading
import subprocess
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from DrissionPage import ChromiumOptions, Chromium
from tabulate import tabulate
from curl_cffi import requests
from bs4 import BeautifulSoup
from notify import NotificationManager


def retry_decorator(retries=3, base_delay=3, backoff_factor=2, max_delay=30):
    """Retry with exponential backoff + jitter.

    Delay sequence example (base=3, factor=2): 3s, 6s, 12s (capped at max_delay).
    Jitter of +/-25% prevents thundering-herd when multiple instances retry.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries - 1:
                        logger.error(f"函数 {func.__name__} 最终执行失败: {str(e)}")
                    logger.warning(
                        f"函数 {func.__name__} 第 {attempt + 1}/{retries} 次尝试失败: {str(e)}"
                    )
                    if attempt < retries - 1:
                        delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                        jitter = delay * random.uniform(-0.25, 0.25)
                        sleep_s = delay + jitter
                        logger.info(f"将在 {sleep_s:.2f}s 后重试 (exponential backoff)")
                        time.sleep(sleep_s)
            return None

        return wrapper

    return decorator



os.environ.pop("DISPLAY", None)
os.environ.pop("DYLD_LIBRARY_PATH", None)

BROWSE_ENABLED = os.environ.get("BROWSE_ENABLED", "true").strip().lower() not in [
    "false",
    "0",
    "off",
]

REPLY_ENABLED = os.environ.get("REPLY_ENABLED", "false").strip().lower() in [
    "true",
    "1",
    "on",
]

# Beijing timezone (UTC+8)
_BJT = timezone(timedelta(hours=8))


def should_read_bookmarks_today(username: str) -> bool:
    """Decide if this account should read from bookmarks in this run.

    Each account reads bookmarks on 1-3 random days per week (deterministic).
    Only triggers in the account's assigned run slot (morning/evening).
    """
    now_bjt = datetime.now(_BJT)
    week_number = now_bjt.isocalendar()[1]
    weekday = now_bjt.weekday()  # 0=Mon..6=Sun

    # Pick 1-3 days this week, seeded by username + week
    seed = int(hashlib.md5(f"bookmark:{username}:{week_number}".encode()).hexdigest(), 16)
    rng = random.Random(seed)
    count = rng.randint(1, 3)
    days = sorted(rng.sample(range(7), count))

    if weekday not in days:
        return False

    # Morning/evening slot — same slot the account uses for replies
    h = int(hashlib.md5(username.encode()).hexdigest(), 16)
    assigned_slot = "morning" if h % 2 == 0 else "evening"
    utc_hour = datetime.now(timezone.utc).hour
    current_slot = "morning" if utc_hour < 10 else "evening"

    if assigned_slot != current_slot:
        return False

    logger.info(f"[Bookmark] {username}: should read bookmarks today (day={weekday}, slot={current_slot})")
    return True

# Randomized Chrome versions for varied fingerprints
CHROME_VERSIONS = [
    "120.0.0.0", "121.0.0.0", "122.0.0.0", "123.0.0.0", "124.0.0.0",
    "125.0.0.0", "126.0.0.0", "127.0.0.0", "128.0.0.0", "129.0.0.0",
    "130.0.0.0", "131.0.0.0", "132.0.0.0", "133.0.0.0", "134.0.0.0",
]

VIEWPORTS = [
    (1366, 768), (1440, 900), (1536, 864), (1600, 900),
    (1920, 1080), (1280, 720), (1280, 800), (1680, 1050),
]

HOME_URL = "https://linux.do/"
LOGIN_URL = "https://linux.do/login"
SESSION_URL = "https://linux.do/session"
CSRF_URL = "https://linux.do/session/csrf"


def _get_memory_percent() -> float:
    """Return memory usage percentage on Linux via /proc/meminfo (no psutil needed)."""
    try:
        with open("/proc/meminfo", "r") as f:
            lines = f.readlines()
        mem_info = {}
        for line in lines:
            parts = line.split()
            if len(parts) >= 2:
                mem_info[parts[0].rstrip(":")] = int(parts[1])
        total = mem_info.get("MemTotal", 1)
        available = mem_info.get("MemAvailable", total)
        return (1 - available / total) * 100
    except Exception:
        return 0.0  # can't read — assume OK


def _cleanup_chrome_processes():
    """Kill orphaned chrome/chromium processes to prevent memory buildup.

    Safe to call on Linux (GitHub Actions) — uses pkill which only targets
    processes owned by the current user.
    """
    try:
        from sys import platform
        if platform.startswith("linux"):
            subprocess.run(
                ["pkill", "-f", "chrome.*--headless"],
                timeout=5, capture_output=True
            )
            subprocess.run(
                ["pkill", "-f", "chromium.*--headless"],
                timeout=5, capture_output=True
            )
    except Exception:
        pass  # non-critical — best effort cleanup


def _check_memory_and_cleanup():
    """Circuit-breaker: force Chrome cleanup if memory usage exceeds 90%.

    GitHub Actions runners have ~7GB RAM. Multiple headless Chrome instances
    can easily exhaust this, causing OOM kills that cascade to all remaining accounts.
    """
    from sys import platform
    if not platform.startswith("linux"):
        return
    mem_pct = _get_memory_percent()
    if mem_pct > 90:
        logger.warning(f"Memory critical: {mem_pct:.1f}% used, forcing Chrome cleanup")
        _cleanup_chrome_processes()
        time.sleep(2)  # let OS reclaim memory
    elif mem_pct > 75:
        logger.info(f"Memory usage: {mem_pct:.1f}%")


class LinuxDoBrowser:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        # Short trace ID for log correlation across parallel jobs
        self.trace_id = hashlib.md5(username.encode()).hexdigest()[:6]
        self.log = self.log.bind(user=username, tid=self.trace_id)
        from sys import platform

        if platform == "linux" or platform == "linux2":
            platformIdentifier = "X11; Linux x86_64"
        elif platform == "darwin":
            platformIdentifier = "Macintosh; Intel Mac OS X 10_15_7"
        elif platform == "win32":
            platformIdentifier = "Windows NT 10.0; Win64; x64"
        else:
            platformIdentifier = "X11; Linux x86_64"

        # Randomize Chrome version and viewport per account
        chrome_ver = random.choice(CHROME_VERSIONS)
        viewport = random.choice(VIEWPORTS)
        ua = f"Mozilla/5.0 ({platformIdentifier}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver} Safari/537.36"

        # Pick a consistent impersonation for this session
        # Use well-supported targets: "chrome" alias auto-resolves to latest available
        self._impersonate = random.choice(["chrome", "chrome124", "chrome131"])

        # Randomize Accept-Language per account
        accept_lang = random.choice([
            "zh-CN,zh;q=0.9",
            "zh-CN,zh;q=0.9,en;q=0.8",
            "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "zh-TW,zh;q=0.9,en-US;q=0.8",
            "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        ])

        # User "personality" — consistent browsing speed within a session
        # speed_factor < 1 = fast reader, > 1 = slow reader
        self._speed = random.uniform(0.6, 1.6)

        co = (
            ChromiumOptions()
            .headless(True)
            .incognito(True)
            .set_argument("--no-sandbox")
            .set_argument(f"--window-size={viewport[0]},{viewport[1]}")
            .set_argument("--disable-blink-features=AutomationControlled")
            .set_argument("--disable-features=AutomationControlled")
            .set_argument("--disable-infobars")
            .set_argument("--disable-dev-shm-usage")
        )
        co.set_user_agent(ua)
        self.browser = Chromium(co)
        self.page = self.browser.new_tab()

        # Hide webdriver property to avoid bot detection
        self.page.run_js("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            window.chrome = {runtime: {}};
        """)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": ua,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": accept_lang,
            }
        )
        self._accept_lang = accept_lang
        # 初始化通知管理器
        self.notifier = NotificationManager()

    def _wait(self, base_min, base_max):
        """Sleep for a personality-adjusted random duration."""
        t = random.uniform(base_min, base_max) * self._speed
        time.sleep(t)
        return t

    def login(self):
        self.log.info("开始登录")

        # Step 1: Navigate to login page in browser (passes Cloudflare automatically)
        self.log.info("通过浏览器访问登录页面...")
        try:
            self.page.get(LOGIN_URL)
            time.sleep(random.uniform(2, 4))
        except Exception as e:
            self.log.error(f"无法访问登录页面: {e}")
            return False

        # Step 2: Fill in login form
        self.log.info("填写登录表单...")
        try:
            # Find and fill username field
            username_input = self.page.ele("#login-account-name", timeout=10)
            if not username_input:
                username_input = self.page.ele("@name=login", timeout=5)
            if not username_input:
                self.log.error("未找到用户名输入框")
                return False

            username_input.clear()
            username_input.input(self.username)
            self._wait(0.5, 1.5)

            # Find and fill password field
            password_input = self.page.ele("#login-account-password", timeout=5)
            if not password_input:
                password_input = self.page.ele("@name=password", timeout=5)
            if not password_input:
                self.log.error("未找到密码输入框")
                return False

            password_input.clear()
            password_input.input(self.password)
            self._wait(0.5, 1.0)
        except Exception as e:
            self.log.error(f"填写表单失败: {e}")
            return False

        # Step 3: Click login button
        self.log.info("点击登录按钮...")
        try:
            login_btn = self.page.ele("#login-button", timeout=5)
            if not login_btn:
                login_btn = self.page.ele("@type=submit", timeout=3)
            if not login_btn:
                self.log.error("未找到登录按钮")
                return False

            login_btn.click()
            time.sleep(random.uniform(3, 6))
        except Exception as e:
            self.log.error(f"点击登录按钮失败: {e}")
            return False

        # Check for rate limiting or error messages in the page
        try:
            page_text = self.page.html or ""
            if "rate limit" in page_text.lower() or "too many" in page_text.lower():
                self.log.warning("触发速率限制")
                self._rate_limit_wait = 60
                return "rate_limited"
        except Exception:
            pass

        # Step 4: Verify login
        self.log.info("验证登录状态...")
        for attempt in range(3):
            time.sleep(random.uniform(3, 7))
            try:
                user_ele = self.page.ele("@id=current-user")
                if user_ele:
                    self.log.info("登录验证成功")
                    break
            except Exception:
                pass

            # Fallback check for avatar
            try:
                if "avatar" in self.page.html:
                    self.log.info("登录验证成功 (通过 avatar)")
                    break
            except Exception:
                pass

            if attempt < 2:
                self.log.warning(f"登录验证失败，第 {attempt + 1}/3 次尝试，刷新页面重试...")
                self.page.get(HOME_URL)
            else:
                self.log.error("登录验证失败 (3次尝试后仍未找到 current-user)")
                return False

        # Step 5: Get CSRF token via browser JS (stays in browser context, no Cloudflare issue)
        self.log.info("通过浏览器获取 CSRF token...")
        try:
            csrf_token = self.page.run_js("""
                return fetch('/session/csrf', {
                    headers: {'X-Requested-With': 'XMLHttpRequest'}
                }).then(r => r.json()).then(d => d.csrf);
            """)
            if csrf_token:
                self._csrf_token = csrf_token
                self.log.info(f"CSRF Token obtained: {csrf_token[:10]}...")
            else:
                self.log.warning("未能获取 CSRF token (reply 功能将不可用)")
        except Exception as e:
            self.log.warning(f"获取 CSRF token 失败: {e}")

        # Step 6: Sync browser cookies to curl_cffi session (for reply API calls)
        self.log.info("同步 Cookie 到 API session...")
        try:
            browser_cookies = self.page.cookies()
            for cookie in browser_cookies:
                self.session.cookies.set(
                    cookie.get("name", ""),
                    cookie.get("value", ""),
                    domain=cookie.get("domain", ".linux.do"),
                )
            self.log.info(f"已同步 {len(browser_cookies)} 个 Cookie")
        except Exception as e:
            self.log.warning(f"Cookie 同步失败: {e}")

        self.print_connect_info()

        # Navigate to homepage for browsing
        self.page.get(HOME_URL)
        self._wait(1, 3)

        return True

    def browse_homepage(self):
        """Scroll the homepage a bit before clicking topics, like a real user."""
        self.log.info("浏览首页...")
        self._wait(2, 5)
        for _ in range(random.randint(1, 3)):
            scroll = random.randint(300, 700)
            self.page.run_js(f"window.scrollBy({{top: {scroll}, behavior: 'smooth'}})")
            self._wait(1.5, 4)
        self.page.run_js("window.scrollTo({top: 0, behavior: 'smooth'})")
        self._wait(1, 2)

    def click_topic(self):
        topic_list = self.page.ele("@id=list-area").eles(".:title")
        if not topic_list:
            self.log.error("未找到主题帖")
            return False
        browse_count = random.randint(3, 8)
        browse_count = min(browse_count, len(topic_list))
        self.log.info(f"发现 {len(topic_list)} 个主题帖，随机选择{browse_count}个")
        for i, topic in enumerate(random.sample(topic_list, browse_count)):
            self.click_one_topic(topic.attr("href"))
            if i < browse_count - 1:
                gap = self._wait(3, 10)
                self.log.info(f"浏览下一个帖子前等待 {gap:.1f}s...")
        return True

    @retry_decorator()
    def click_one_topic(self, topic_url):
        new_page = self.browser.new_tab()
        try:
            new_page.get(topic_url)

            # ~10% chance to bail out quickly ("not what I expected")
            if random.random() < 0.10:
                self.log.info("快速浏览后离开帖子 (不感兴趣)")
                self._wait(1, 3)
                return

            # 20% chance to like a post — split across before/during/after for natural timing
            should_like = random.random() < 0.20
            like_timing = random.choice(["before", "during", "after"]) if should_like else "none"
            if like_timing == "before":
                self.click_like(new_page)

            # 10% chance to bookmark — decided before browsing, executed after
            should_bookmark = random.random() < 0.10

            self.browse_post(new_page, like_during=(like_timing == "during"))

            if like_timing == "after":
                self.click_like(new_page)

            # Bookmark after reading — most natural moment to save a post
            if should_bookmark:
                self.click_bookmark(new_page)
        finally:
            try:
                new_page.close()
            except Exception:
                pass

    def browse_post(self, page, like_during=False):
        prev_url = None
        max_scrolls = random.randint(5, 15)
        like_at_scroll = random.randint(2, max_scrolls - 1) if like_during else -1

        initial_pause = self._wait(2, 6)
        self.log.info(f"阅读帖子顶部，等待 {initial_pause:.1f}s...")

        for i in range(max_scrolls):
            if random.random() < 0.15:
                scroll_distance = random.randint(100, 300)
            elif random.random() < 0.1:
                scroll_distance = -random.randint(100, 250)
            else:
                scroll_distance = random.randint(300, 800)

            direction = "上" if scroll_distance < 0 else "下"
            self.log.info(f"向{direction}滚动 {abs(scroll_distance)} 像素...")
            page.run_js(f"window.scrollBy({{top: {scroll_distance}, behavior: 'smooth'}})")
            time.sleep(random.uniform(0.3, 0.8))
            self.log.info(f"已加载页面: {page.url}")

            if i == like_at_scroll:
                self.click_like(page)

            # Progressive exit probability: starts at 5%, increases each scroll
            # By scroll 10 it's ~30%, preventing timeout on long posts
            exit_prob = 0.05 + (i / max_scrolls) * 0.25
            if random.random() < exit_prob:
                self.log.success("随机退出浏览")
                break

            at_bottom = page.run_js(
                "window.scrollY + window.innerHeight >= document.body.scrollHeight"
            )
            current_url = page.url
            if current_url != prev_url:
                prev_url = current_url
            elif at_bottom and prev_url == current_url:
                self.log.success("已到达页面底部，退出浏览")
                break

            # Personality-adjusted wait times
            if random.random() < 0.2:
                wait_time = self._wait(5, 12)
            elif random.random() < 0.3:
                wait_time = self._wait(1, 2)
            else:
                wait_time = self._wait(2, 5)
            self.log.info(f"等待 {wait_time:.2f} 秒...")

    def run(self):
        self.reply_result = None  # Track reply result (dict or None)
        self.login_success = False  # Track whether login actually worked
        try:
            login_res = self.login()
            if login_res == "rate_limited":
                raise Exception(f"RATE_LIMITED:{getattr(self, '_rate_limit_wait', 60)}")
            if not login_res:
                self.log.warning("登录验证失败，跳过浏览")
                return

            self.login_success = True

            if BROWSE_ENABLED:
                # Some users just login and leave (~15% chance)
                if random.random() < 0.15:
                    self.log.info("模拟快速登录用户，跳过浏览")
                else:
                    # Occasionally check notifications or profile first (~20%)
                    if random.random() < 0.20:
                        self.visit_side_page()

                    # Browse homepage first like a real user
                    self.browse_homepage()
                    click_topic_res = self.click_topic()
                    if not click_topic_res:
                        self.log.error("点击主题失败，程序终止")
                        return
                    self.log.info("完成浏览任务")

                    # Read from bookmarks on scheduled days (1-3 days/week)
                    if should_read_bookmarks_today(self.username):
                        self.read_from_bookmarks()

                    # Sometimes check notifications after browsing too (~15%)
                    if random.random() < 0.15:
                        self.visit_side_page()

            self.send_notifications(BROWSE_ENABLED)  # 发送通知

            # Auto-reply phase (after browse, gated by REPLY_ENABLED)
            if REPLY_ENABLED:
                try:
                    from reply_engine import execute_reply
                    self.reply_result = execute_reply(
                        self,
                        bot_usernames=getattr(self, "_bot_usernames", set()),
                        used_topics=getattr(self, "_used_topics", set()),
                        used_phrases=getattr(self, "_used_phrases", set()),
                    )
                except Exception as e:
                    self.log.error(f"[Reply] Reply phase failed: {e}")
                    self.reply_result = None
        finally:
            try:
                self.page.close()
            except Exception:
                pass
            try:
                self.browser.quit()
            except Exception:
                pass
            # Kill any orphaned chrome processes left by this instance
            _cleanup_chrome_processes()

    def visit_side_page(self):
        """Occasionally visit notifications, profile, or categories like a real user."""
        side_pages = [
            ("https://linux.do/notifications", "通知页面"),
            ("https://linux.do/categories", "分类页面"),
            ("https://linux.do/latest", "最新帖子"),
            ("https://linux.do/top", "热门帖子"),
        ]
        url, name = random.choice(side_pages)
        self.log.info(f"访问{name}: {url}")
        try:
            self.page.get(url)
            self._wait(3, 8)
            # Scroll a bit
            if random.random() < 0.5:
                scroll = random.randint(200, 500)
                self.page.run_js(f"window.scrollBy({{top: {scroll}, behavior: 'smooth'}})")
                self._wait(2, 5)
            # Go back to homepage
            self.page.get(HOME_URL)
            self._wait(2, 4)
        except Exception as e:
            self.log.warning(f"访问{name}失败: {e}")

    def click_like(self, page):
        try:
            # Use pure JS to find and click the like button — avoids stale element issues
            # from Discourse's Ember.js re-rendering during scroll
            result = page.run_js("""
                // Try reaction button first, then standard like button
                const btn = document.querySelector('.discourse-reactions-reaction-button')
                           || document.querySelector('button.toggle-like')
                           || document.querySelector('.like-button');
                if (!btn) return 'not_found';
                // Check if already liked (has .has-like or .liked class)
                if (btn.classList.contains('has-like') || btn.classList.contains('liked')) return 'already_liked';
                btn.scrollIntoView({behavior: 'smooth', block: 'center'});
                return 'found';
            """)

            if result == 'not_found':
                self.log.info("未找到点赞按钮")
                return
            if result == 'already_liked':
                self.log.info("帖子已经点过赞了")
                return

            self.log.info("找到未点赞的帖子，准备点赞")
            self._wait(0.5, 1.5)

            # Click via JS — immune to stale element references
            clicked = page.run_js("""
                const btn = document.querySelector('.discourse-reactions-reaction-button')
                           || document.querySelector('button.toggle-like')
                           || document.querySelector('.like-button');
                if (btn) { btn.click(); return true; }
                return false;
            """)
            if clicked:
                self.log.info("点赞成功")
                self._wait(0.8, 2.5)
            else:
                self.log.info("点赞按钮刷新后消失")
        except Exception as e:
            self.log.error(f"点赞失败: {str(e)}")

    def click_bookmark(self, page):
        """Bookmark the first post in the topic, like a user saving it for later."""
        try:
            # Use JS to find and click bookmark — avoids stale element issues
            result = page.run_js("""
                const btn = document.querySelector('.topic-footer-main-buttons .bookmark')
                         || document.querySelector('button.bookmark')
                         || document.querySelector('.bookmark-btn');
                if (!btn) return 'not_found';
                // Check if already bookmarked
                if (btn.classList.contains('bookmarked')) return 'already_bookmarked';
                btn.scrollIntoView({behavior: 'smooth', block: 'center'});
                return 'found';
            """)

            if result == 'not_found':
                self.log.info("未找到书签按钮，跳过收藏")
                return
            if result == 'already_bookmarked':
                self.log.info("帖子已经收藏过了")
                return

            self._wait(0.8, 2.0)

            # Click via JS
            page.run_js("""
                const btn = document.querySelector('.topic-footer-main-buttons .bookmark')
                         || document.querySelector('button.bookmark')
                         || document.querySelector('.bookmark-btn');
                if (btn) btn.click();
            """)
            self.log.info("收藏帖子成功")
            self._wait(1.0, 2.5)

            # If a bookmark popup/modal appeared, click the save/confirm button
            try:
                save_clicked = page.run_js("""
                    const btn = document.querySelector('button.btn-primary.bookmark-save')
                             || document.querySelector('.bookmark-reminder-modal .btn-primary');
                    if (btn) { btn.click(); return true; }
                    return false;
                """)
                if save_clicked:
                    self.log.info("确认收藏成功")
                    self._wait(0.5, 1.5)
            except Exception:
                pass
        except Exception as e:
            self.log.error(f"收藏失败: {str(e)}")

    def read_from_bookmarks(self):
        """Visit the bookmarks page and read one bookmarked topic, like revisiting saved content."""
        self.log.info("[Bookmark] 访问书签列表...")
        try:
            self.page.get("https://linux.do/bookmarks")
            self._wait(2, 5)

            # Scroll the bookmark list a bit — scanning what we saved
            if random.random() < 0.6:
                scroll = random.randint(200, 500)
                self.page.run_js(f"window.scrollBy({{top: {scroll}, behavior: 'smooth'}})")
                self._wait(1.5, 3)

            # Find bookmarked topic links
            bookmark_links = self.page.eles("css:.bookmark-list .topic-link a") or \
                             self.page.eles("css:.topic-list-item .link-top-line a") or \
                             self.page.eles("css:a.title")

            if not bookmark_links:
                self.log.info("[Bookmark] 书签列表为空或未找到帖子链接")
                self.page.get(HOME_URL)
                self._wait(1, 3)
                return

            # Pick one random bookmarked topic
            target = random.choice(bookmark_links)
            topic_url = target.attr("href")
            topic_title = target.text.strip()
            self.log.info(f"[Bookmark] 从书签中选择帖子: {topic_title}")

            # Pause before clicking — scanning the list, deciding which to re-read
            self._wait(1, 3)

            # Open and browse the bookmarked topic in a new tab, reusing existing browse logic
            self.click_one_topic(topic_url)

            self.log.info("[Bookmark] 书签帖子阅读完成")

            # Return to homepage
            self.page.get(HOME_URL)
            self._wait(1, 3)
        except Exception as e:
            self.log.warning(f"[Bookmark] 阅读书签帖子失败: {e}")
            try:
                self.page.get(HOME_URL)
            except Exception:
                pass

    def print_connect_info(self):
        self.log.info("获取连接信息")
        try:
            # Use browser to visit connect.linux.do (curl_cffi gets Cloudflare 403)
            connect_tab = self.browser.new_tab()
            try:
                connect_tab.get("https://connect.linux.do/")
                time.sleep(random.uniform(2, 4))

                # Extract table data via JS to avoid BeautifulSoup dependency on curl_cffi
                info = connect_tab.run_js("""
                    const rows = document.querySelectorAll('table tr');
                    const data = [];
                    rows.forEach(row => {
                        const cells = row.querySelectorAll('td');
                        if (cells.length >= 3) {
                            data.push([
                                cells[0].textContent.trim(),
                                cells[1].textContent.trim() || '0',
                                cells[2].textContent.trim() || '0'
                            ]);
                        }
                    });
                    return JSON.stringify(data);
                """)

                if info:
                    table_data = json.loads(info)
                    print("--------------Connect Info-----------------")
                    print(tabulate(table_data, headers=["项目", "当前", "要求"], tablefmt="pretty"))
                else:
                    print("--------------Connect Info-----------------")
                    print("(no data)")
            finally:
                try:
                    connect_tab.close()
                except Exception:
                    pass
        except Exception as e:
            self.log.warning(f"获取连接信息失败: {e}")

    def send_notifications(self, browse_enabled):
        """发送签到通知"""
        status_msg = f"✅每日登录成功: {self.username}"
        if browse_enabled:
            status_msg += " + 浏览任务完成"
        
        # 使用通知管理器发送所有通知
        self.notifier.send_all("LINUX DO", status_msg)


def get_accounts():
    """Get account list from ACCOUNTS_JSON or single LINUXDO_USERNAME/PASSWORD env vars."""
    accounts_json = os.environ.get("ACCOUNTS_JSON")
    if accounts_json:
        try:
            accounts = json.loads(accounts_json)
            if isinstance(accounts, list) and len(accounts) > 0:
                logger.info(f"Loaded {len(accounts)} accounts from ACCOUNTS_JSON")
                return accounts
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse ACCOUNTS_JSON: {e}")
            exit(1)

    # Fallback to single account env vars
    username = os.environ.get("LINUXDO_USERNAME") or os.environ.get("USERNAME")
    password = os.environ.get("LINUXDO_PASSWORD") or os.environ.get("PASSWORD")
    if username and password:
        logger.info("Using single account from LINUXDO_USERNAME/PASSWORD")
        return [{"username": username, "password": password}]

    return []


# Thread-safe lists for tracking results
_results_lock = threading.Lock()


def process_account(account, index, total):
    """Process a single account. Returns (username, success: bool)."""
    username = account.get("username", "")
    password = account.get("password", "")
    if not username or not password:
        logger.warning(f"[{index}/{total}] Skipping account with missing username/password")
        return (username or f"account_{index}", False)

    logger.info(f"========== [{index}/{total}] Processing: {username} ==========")
    try:
        browser = LinuxDoBrowser(username, password)
        browser.run()
        logger.success(f"[{index}/{total}] Account {username} completed successfully")
        return (username, True)
    except Exception as e:
        logger.error(f"[{index}/{total}] Account {username} failed: {e}")
        return (username, False)


if __name__ == "__main__":
    import sys
    # Configure loguru to include trace_id when available (bound via logger.bind)
    logger.configure(extra={"user": "", "tid": ""})
    fmt = "<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{extra[tid]}</cyan> | {message}"
    logger.remove()
    logger.add(sys.stderr, format=fmt, colorize=True)

    all_accounts = get_accounts()
    if not all_accounts:
        print("No accounts configured. Set ACCOUNTS_JSON or LINUXDO_USERNAME/PASSWORD.")
        exit(1)

    # Job splitting: JOB_INDEX (0-based) and JOB_TOTAL split accounts across parallel jobs
    JOB_INDEX = int(os.environ.get("JOB_INDEX") or "0")
    JOB_TOTAL = int(os.environ.get("JOB_TOTAL") or "1")

    # Stagger job start times with randomness to look more natural
    if JOB_INDEX > 0:
        startup_delay = JOB_INDEX * 30 + random.randint(0, 45)
        logger.info(f"Job {JOB_INDEX + 1}/{JOB_TOTAL} | Waiting {startup_delay}s before starting...")
        time.sleep(startup_delay)

    # Shuffle accounts using today's date as seed so all jobs agree on the order
    from datetime import date
    daily_seed = int(date.today().strftime("%Y%m%d"))
    shuffled = list(all_accounts)
    random.Random(daily_seed).shuffle(shuffled)

    # Split shuffled accounts evenly across jobs
    accounts = [a for idx, a in enumerate(shuffled) if idx % JOB_TOTAL == JOB_INDEX]
    logger.info(f"Job {JOB_INDEX + 1}/{JOB_TOTAL} | Assigned {len(accounts)}/{len(all_accounts)} accounts")

    total = len(accounts)
    success_list = []
    fail_list = []
    replied_accounts = []  # List of dicts with reply details
    rate_limited_queue = []  # accounts to retry after rate limit

    # Build set of bot usernames for reply anti-sockpuppet filtering
    bot_usernames = {a.get("username", "") for a in all_accounts if a.get("username")}
    # Shared sets within this job to prevent same-IP collisions
    used_topics = set()
    used_phrases = set()

    # Process accounts one by one with delay to avoid rate limiting
    ACCOUNT_DELAY = int(os.environ.get("ACCOUNT_DELAY") or "60")  # seconds between accounts
    logger.info(f"Total accounts: {total} | Delay between accounts: {ACCOUNT_DELAY}s")

    for i, account in enumerate(accounts, 1):
        username = account.get("username", "")
        password = account.get("password", "")
        if not username or not password:
            logger.warning(f"[{i}/{total}] Skipping account with missing username/password")
            fail_list.append(username or f"account_{i}")
            continue

        logger.info(f"========== [{i}/{total}] Processing: {username} ==========")
        _check_memory_and_cleanup()  # circuit-breaker: cleanup if memory > 90%
        try:
            browser = LinuxDoBrowser(username, password)
            browser._bot_usernames = bot_usernames
            browser._used_topics = used_topics
            browser._used_phrases = used_phrases
            browser.run()
            if browser.login_success:
                success_list.append(username)
                if browser.reply_result:
                    replied_accounts.append(browser.reply_result)
                logger.success(f"[{i}/{total}] Account {username} completed successfully")
            else:
                fail_list.append(username)
                logger.warning(f"[{i}/{total}] Account {username} login failed")
        except Exception as e:
            error_msg = str(e)
            if "RATE_LIMITED" in error_msg:
                # Extract wait time from error message
                try:
                    wait_secs = int(error_msg.split(":")[1])
                except (IndexError, ValueError):
                    wait_secs = 120
                logger.warning(f"[{i}/{total}] Account {username} hit rate limit, queued for retry")
                rate_limited_queue.append(account)
                # Wait for the rate limit to expire, then continue
                wait_secs = min(wait_secs + 30, 2100)  # add 30s buffer, cap at 35min
                logger.info(f"Rate limit detected. Waiting {wait_secs}s before continuing...")
                time.sleep(wait_secs)
                continue  # skip the normal delay since we already waited
            else:
                logger.error(f"[{i}/{total}] Account {username} failed: {e}")
                fail_list.append(username)

        # Delay between accounts to avoid rate limiting
        if i < total:
            delay = random.uniform(ACCOUNT_DELAY, ACCOUNT_DELAY + 15)
            logger.info(f"Waiting {delay:.1f}s before next account...")
            time.sleep(delay)

    # Retry rate-limited accounts
    if rate_limited_queue:
        logger.info(f"========== Retrying {len(rate_limited_queue)} rate-limited accounts ==========")
        for i, account in enumerate(rate_limited_queue, 1):
            username = account.get("username", "")
            password = account.get("password", "")
            logger.info(f"========== [Retry {i}/{len(rate_limited_queue)}] Processing: {username} ==========")
            try:
                browser = LinuxDoBrowser(username, password)
                browser._bot_usernames = bot_usernames
                browser._used_topics = used_topics
                browser._used_phrases = used_phrases
                browser.run()
                if browser.login_success:
                    success_list.append(username)
                    if browser.reply_result:
                        replied_accounts.append(browser.reply_result)
                    logger.success(f"[Retry {i}] Account {username} completed successfully")
                else:
                    fail_list.append(username)
                    logger.warning(f"[Retry {i}] Account {username} login failed")
            except Exception as e:
                logger.error(f"[Retry {i}] Account {username} failed: {e}")
                fail_list.append(username)

            if i < len(rate_limited_queue):
                delay = random.uniform(ACCOUNT_DELAY, ACCOUNT_DELAY + 15)
                logger.info(f"Waiting {delay:.1f}s before next retry...")
                time.sleep(delay)

    logger.info("========== Summary ==========")
    logger.info(f"Total: {total} | Success: {len(success_list)} | Failed: {len(fail_list)} | Replies: {len(replied_accounts)}")
    if success_list:
        logger.success(f"Successful accounts: {', '.join(success_list)}")
    if fail_list:
        logger.warning(f"Failed accounts: {', '.join(fail_list)}")
    if replied_accounts:
        for r in replied_accounts:
            logger.info(f"  Reply: {r['username']} -> [{r['topic_id']}] {r['topic_title']}")

    # Save results to JSON file for the summary job to collect
    results = {
        "job_index": JOB_INDEX,
        "total": total,
        "success": success_list,
        "fail": fail_list,
        "replied_accounts": replied_accounts,
    }
    results_file = f"results_job_{JOB_INDEX}.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False)
    logger.info(f"Results saved to {results_file}")
