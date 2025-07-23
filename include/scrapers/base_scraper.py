# base_scraper.py
import os
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from abc import ABC, abstractmethod
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
from typing import Optional
import re
import unicodedata
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
)
logger = logging.getLogger("scraper")

class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.
    Handles robots.txt and user agent logic.
    """
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    def __init__(self, base_url: str, robots_txt_path: str = "robots.txt", user_agent: str = ""):
        if not base_url.endswith('/'):
            base_url += '/'
        self.base_url = base_url
        self.robots_txt_url = urljoin(self.base_url, robots_txt_path or "robots.txt")
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.robot_parser = urllib.robotparser.RobotFileParser()
        self._load_robots_txt()

    def _load_robots_txt(self) -> None:
        try:
            self.robot_parser.set_url(self.robots_txt_url)
            self.robot_parser.read()
            logger.info(f"Successfully loaded robots.txt from {self.robots_txt_url}")
        except Exception as e:
            logger.warning(f"Error loading robots.txt from {self.robots_txt_url}: {e}. Proceeding without robots.txt rules enforced (not recommended).")

    def is_scraping_allowed(self, url: str) -> bool:
        parsed_url = urlparse(url)
        return self.robot_parser.can_fetch(self.user_agent, parsed_url.path)
    
    @staticmethod
    def _preprocess_title(title: str, sep: str) -> str:
        """Convert title to a URL-safe slug."""
        title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
        title = title.lower()
        title = re.sub(r"[\s_\-]+", sep, title) 
        title = re.sub(rf"[^{re.escape(sep)}a-z0-9]", "", title)
        title = re.sub(rf"{re.escape(sep)}{{2,}}", sep, title)
        return title.strip(sep)

    @abstractmethod
    def _fetch_page(self, url: str) -> Optional[str]:
        pass

    @abstractmethod
    def _parse_content(self, html_content: str):
        pass

    @abstractmethod
    def get_ratings(self, identifier: str):
        pass

class HtmlScraper(BaseScraper):
    """
    Scraper for static HTML pages using requests.
    """
    REQUEST_DELAY_SECONDS = 1

    def __init__(self, base_url: str, robots_txt_path: str = "robots.txt", user_agent: str = ""):
        super().__init__(base_url, robots_txt_path, user_agent)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

    def _fetch_page(self, url: str) -> Optional[str]:
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            time.sleep(self.REQUEST_DELAY_SECONDS)
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    


class SeleniumScraper(BaseScraper):
    def __init__(self, base_url: str, user_agent: str = '', driver_path: str = '', profile_path: str = ''):
        super().__init__(base_url, user_agent=user_agent)
        self.driver_path = driver_path or os.getenv("CHROME_DRIVER") or r"C:/Users/hamed/OneDrive/Desktop/Projects/TopSeries/chromedriver.exe"
        self.profile_path = profile_path or os.getenv("SELENIUM_PROFILE_DIR") or ""
        if not self.driver_path or not os.path.exists(self.driver_path):
            raise FileNotFoundError(f"ChromeDriver not found at {self.driver_path}. Set CHROME_DRIVER in your .env file or pass driver_path explicitly.")
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-3d-apis")
        chrome_options.add_argument("--disable-webgl")
        chrome_options.add_argument("--disable-webgl2")
        chrome_options.add_argument("--use-gl=swiftshader")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument(f"user-agent={self.user_agent}")
        if self.profile_path:
            logger.info(f"[Selenium] Using user data dir: {self.profile_path}")
            chrome_options.add_argument(f'--user-data-dir={self.profile_path}')
        else:
            logger.info("[Selenium] Using incognito mode (no profile)")
            chrome_options.add_argument('--incognito')
        logger.info(f"[Selenium] Starting Chrome with driver at: {self.driver_path}")
        service = Service(self.driver_path, log_path="NUL")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.quit()
    def _fetch_page(self, url: str) -> str | None:
        try:
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 8)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "media-scorecard")))
            return self.driver.page_source
        except Exception as e:
            logger.error(f"Error fetching {url} with Selenium: {e}")
            return None
    def quit(self):
        self.driver.quit()