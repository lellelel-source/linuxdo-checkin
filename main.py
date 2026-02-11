"""
cron: 0 */6 * * *
new Env("Linux.Do 签到")
"""

import os
import random
import time
import json
import functools
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from DrissionPage import ChromiumOptions, Chromium
from tabulate import tabulate
from curl_cffi import requests
from bs4 import BeautifulSoup
from notify import NotificationManager


def retry_decorator(retries=3, min_delay=5, max_delay=10):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries - 1:  # 最后一次尝试
                        logger.error(f"函数 {func.__name__} 最终执行失败: {str(e)}")
                    logger.warning(
                        f"函数 {func.__name__} 第 {attempt + 1}/{retries} 次尝试失败: {str(e)}"
                    )
                    if attempt < retries - 1:
                        sleep_s = random.uniform(min_delay, max_delay)
                        logger.info(
                            f"将在 {sleep_s:.2f}s 后重试 ({min_delay}-{max_delay}s 随机延迟)"
                        )
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

HOME_URL = "https://linux.do/"
LOGIN_URL = "https://linux.do/login"
SESSION_URL = "https://linux.do/session"
CSRF_URL = "https://linux.do/session/csrf"


class LinuxDoBrowser:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        from sys import platform

        if platform == "linux" or platform == "linux2":
            platformIdentifier = "X11; Linux x86_64"
        elif platform == "darwin":
            platformIdentifier = "Macintosh; Intel Mac OS X 10_15_7"
        elif platform == "win32":
            platformIdentifier = "Windows NT 10.0; Win64; x64"
        else:
            platformIdentifier = "X11; Linux x86_64"

        co = (
            ChromiumOptions()
            .headless(True)
            .incognito(True)
            .set_argument("--no-sandbox")
        )
        co.set_user_agent(
            f"Mozilla/5.0 ({platformIdentifier}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
        )
        self.browser = Chromium(co)
        self.page = self.browser.new_tab()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "zh-CN,zh;q=0.9",
            }
        )
        # 初始化通知管理器
        self.notifier = NotificationManager()

    def login(self):
        logger.info("开始登录")
        # Step 1: Get CSRF Token
        logger.info("获取 CSRF token...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": LOGIN_URL,
        }
        resp_csrf = self.session.get(CSRF_URL, headers=headers, impersonate="firefox135")
        if resp_csrf.status_code != 200:
            logger.error(f"获取 CSRF token 失败: {resp_csrf.status_code}")
            return False        
        csrf_data = resp_csrf.json()
        csrf_token = csrf_data.get("csrf")
        logger.info(f"CSRF Token obtained: {csrf_token[:10]}...")

        # Step 2: Login
        logger.info("正在登录...")
        headers.update(
            {
                "X-CSRF-Token": csrf_token,
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": "https://linux.do",
            }
        )

        data = {
            "login": self.username,
            "password": self.password,
            "second_factor_method": "1",
            "timezone": "Asia/Shanghai",
        }

        try:
            resp_login = self.session.post(
                SESSION_URL, data=data, impersonate="chrome136", headers=headers
            )

            if resp_login.status_code == 200:
                response_json = resp_login.json()
                if response_json.get("error"):
                    logger.error(f"登录失败: {response_json.get('error')}")
                    return False
                logger.info("登录成功!")
            elif resp_login.status_code == 429:
                # Rate limited - extract wait time and signal caller to retry
                try:
                    err_json = resp_login.json()
                    wait_seconds = err_json.get("extras", {}).get("wait_seconds", 60)
                    time_left = err_json.get("extras", {}).get("time_left", "unknown")
                    logger.warning(f"触发速率限制，需要等待 {time_left} ({wait_seconds}s)")
                    self._rate_limit_wait = wait_seconds
                except Exception:
                    self._rate_limit_wait = 60
                return "rate_limited"
            else:
                logger.error(f"登录失败，状态码: {resp_login.status_code}")
                logger.error(resp_login.text[:200])
                return False
        except Exception as e:
            logger.error(f"登录请求异常: {e}")
            return False

        self.print_connect_info()  # 打印连接信息

        # Step 3: Pass cookies to DrissionPage
        logger.info("同步 Cookie 到 DrissionPage...")

        # Convert requests cookies to DrissionPage format
        # Using standard requests.utils to parse cookiejar if possible, or manual extraction
        # requests.Session().cookies is a specialized object, but might support standard iteration

        # We can iterate over the cookies manually if dict_from_cookiejar doesn't work perfectly
        # or convert to dict first.
        # Assuming requests behaves like requests:

        cookies_dict = self.session.cookies.get_dict()

        dp_cookies = []
        for name, value in cookies_dict.items():
            dp_cookies.append(
                {
                    "name": name,
                    "value": value,
                    "domain": ".linux.do",
                    "path": "/",
                }
            )

        self.page.set.cookies(dp_cookies)

        logger.info("Cookie 设置完成，导航至 linux.do...")
        self.page.get(HOME_URL)

        time.sleep(5)
        try:
            user_ele = self.page.ele("@id=current-user")
        except Exception as e:
            logger.warning(f"登录验证失败: {str(e)}")
            return True
        if not user_ele:
            # Fallback check for avatar
            if "avatar" in self.page.html:
                logger.info("登录验证成功 (通过 avatar)")
                return True
            logger.error("登录验证失败 (未找到 current-user)")
            return False
        else:
            logger.info("登录验证成功")
            return True

    def click_topic(self):
        topic_list = self.page.ele("@id=list-area").eles(".:title")
        if not topic_list:
            logger.error("未找到主题帖")
            return False
        browse_count = random.randint(3, 8)
        browse_count = min(browse_count, len(topic_list))
        logger.info(f"发现 {len(topic_list)} 个主题帖，随机选择{browse_count}个")
        for topic in random.sample(topic_list, browse_count):
            self.click_one_topic(topic.attr("href"))
        return True

    @retry_decorator()
    def click_one_topic(self, topic_url):
        new_page = self.browser.new_tab()
        try:
            new_page.get(topic_url)
            if random.random() < 0.3:  # 0.3 * 30 = 9
                self.click_like(new_page)
            self.browse_post(new_page)
        finally:
            try:
                new_page.close()
            except Exception:
                pass

    def browse_post(self, page):
        prev_url = None
        # 开始自动滚动，最多滚动10次
        for _ in range(10):
            # 随机滚动一段距离
            scroll_distance = random.randint(550, 650)  # 随机滚动 550-650 像素
            logger.info(f"向下滚动 {scroll_distance} 像素...")
            page.run_js(f"window.scrollBy(0, {scroll_distance})")
            logger.info(f"已加载页面: {page.url}")

            if random.random() < 0.03:  # 33 * 4 = 132
                logger.success("随机退出浏览")
                break

            # 检查是否到达页面底部
            at_bottom = page.run_js(
                "window.scrollY + window.innerHeight >= document.body.scrollHeight"
            )
            current_url = page.url
            if current_url != prev_url:
                prev_url = current_url
            elif at_bottom and prev_url == current_url:
                logger.success("已到达页面底部，退出浏览")
                break

            # 动态随机等待
            wait_time = random.uniform(2, 4)  # 随机等待 2-4 秒
            logger.info(f"等待 {wait_time:.2f} 秒...")
            time.sleep(wait_time)

    def run(self):
        try:
            login_res = self.login()
            if login_res == "rate_limited":
                raise Exception(f"RATE_LIMITED:{getattr(self, '_rate_limit_wait', 60)}")
            if not login_res:  # 登录
                logger.warning("登录验证失败")

            if BROWSE_ENABLED:
                click_topic_res = self.click_topic()  # 点击主题
                if not click_topic_res:
                    logger.error("点击主题失败，程序终止")
                    return
                logger.info("完成浏览任务")

            self.send_notifications(BROWSE_ENABLED)  # 发送通知
        finally:
            try:
                self.page.close()
            except Exception:
                pass
            try:
                self.browser.quit()
            except Exception:
                pass

    def click_like(self, page):
        try:
            # 专门查找未点赞的按钮
            like_button = page.ele(".discourse-reactions-reaction-button")
            if like_button:
                logger.info("找到未点赞的帖子，准备点赞")
                like_button.click()
                logger.info("点赞成功")
                time.sleep(random.uniform(1, 2))
            else:
                logger.info("帖子可能已经点过赞了")
        except Exception as e:
            logger.error(f"点赞失败: {str(e)}")

    def print_connect_info(self):
        logger.info("获取连接信息")
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        }
        resp = self.session.get(
            "https://connect.linux.do/", headers=headers, impersonate="chrome136"
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("table tr")
        info = []

        for row in rows:
            cells = row.select("td")
            if len(cells) >= 3:
                project = cells[0].text.strip()
                current = cells[1].text.strip() if cells[1].text.strip() else "0"
                requirement = cells[2].text.strip() if cells[2].text.strip() else "0"
                info.append([project, current, requirement])

        print("--------------Connect Info-----------------")
        print(tabulate(info, headers=["项目", "当前", "要求"], tablefmt="pretty"))

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
    all_accounts = get_accounts()
    if not all_accounts:
        print("No accounts configured. Set ACCOUNTS_JSON or LINUXDO_USERNAME/PASSWORD.")
        exit(1)

    # Job splitting: JOB_INDEX (0-based) and JOB_TOTAL split accounts across parallel jobs
    JOB_INDEX = int(os.environ.get("JOB_INDEX") or "0")
    JOB_TOTAL = int(os.environ.get("JOB_TOTAL") or "1")

    # Stagger job start times to avoid all jobs hitting the server at once
    if JOB_INDEX > 0:
        startup_delay = JOB_INDEX * 30
        logger.info(f"Job {JOB_INDEX + 1}/{JOB_TOTAL} | Waiting {startup_delay}s before starting...")
        time.sleep(startup_delay)

    # Split accounts evenly across jobs
    accounts = [a for idx, a in enumerate(all_accounts) if idx % JOB_TOTAL == JOB_INDEX]
    logger.info(f"Job {JOB_INDEX + 1}/{JOB_TOTAL} | Assigned {len(accounts)}/{len(all_accounts)} accounts")

    total = len(accounts)
    success_list = []
    fail_list = []
    rate_limited_queue = []  # accounts to retry after rate limit

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
        try:
            browser = LinuxDoBrowser(username, password)
            browser.run()
            success_list.append(username)
            logger.success(f"[{i}/{total}] Account {username} completed successfully")
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
                browser.run()
                success_list.append(username)
                logger.success(f"[Retry {i}] Account {username} completed successfully")
            except Exception as e:
                logger.error(f"[Retry {i}] Account {username} failed: {e}")
                fail_list.append(username)

            if i < len(rate_limited_queue):
                delay = random.uniform(ACCOUNT_DELAY, ACCOUNT_DELAY + 15)
                logger.info(f"Waiting {delay:.1f}s before next retry...")
                time.sleep(delay)

    logger.info("========== Summary ==========")
    logger.info(f"Total: {total} | Success: {len(success_list)} | Failed: {len(fail_list)}")
    if success_list:
        logger.success(f"Successful accounts: {', '.join(success_list)}")
    if fail_list:
        logger.warning(f"Failed accounts: {', '.join(fail_list)}")

    # Send a single summary email with all results
    summary_notifier = NotificationManager()
    summary_lines = [
        f"Total: {total} | Success: {len(success_list)} | Failed: {len(fail_list)}",
        "",
        f"✅ Successful ({len(success_list)}):",
    ]
    summary_lines += [f"  - {u}" for u in success_list] if success_list else ["  (none)"]
    summary_lines += ["", f"❌ Failed ({len(fail_list)}):"]
    summary_lines += [f"  - {u}" for u in fail_list] if fail_list else ["  (none)"]
    summary_notifier.send_email("LinuxDo Check-in Summary", "\n".join(summary_lines))
