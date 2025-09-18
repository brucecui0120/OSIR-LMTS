import queue
import threading
from typing import Generator
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.webdriver import WebDriver
from webdriver_manager.chrome import ChromeDriverManager
from loguru import logger

def init_driver() -> WebDriver:
    options = ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    return driver


class WebDriverPool:

    def __init__(self, size: int = 1, options: ChromeOptions | None = None):
        if options is None:
            options = ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu') 

        self.options = options
        self._pool = queue.Queue(maxsize=size)
        self._lock = threading.Lock()
        self._current_size = 0
        self.target_size = size
        self._shutdown = False
        self._initialize_pool()

    def _initialize_pool(self):
        with self._lock:
            while self._current_size < self.target_size:
                try:
                    driver = self._create_driver()
                    self._pool.put(driver)
                    self._current_size += 1
                except Exception:
                    logger.exception("Failed to create driver during initialization.")
                    break

    def _create_driver(self) -> webdriver.Chrome:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=self.options)
        return driver

    def _is_driver_healthy(self, driver: webdriver.Chrome) -> bool:
        try:
            _ = driver.window_handles
            return True
        except Exception:
            logger.warning("Web driver health check failed.", exc_info=True)
            return False

    def _recreate_driver_if_needed(self, old_driver: webdriver.Chrome | None):
        if old_driver:
            try:
                old_driver.quit()
            except Exception:
                logger.warning("Failed to quit unhealthy driver.", exc_info=True)
            self._current_size -= 1

        if not self._shutdown and self._current_size < self.target_size:
            try:
                new_driver = self._create_driver()
                self._pool.put(new_driver)
                self._current_size += 1
                logger.info("Replaced an unhealthy driver. Pool size is now correct.")
            except Exception:
                logger.exception("Failed to create new driver during replacement.")

    @contextmanager
    def get_driver(self) -> Generator[webdriver.Chrome, None, None]:
        if self._shutdown:
            raise RuntimeError("WebDriverPool has been shut down.")

        driver = self._pool.get()
        is_healthy = self._is_driver_healthy(driver)

        while not is_healthy:
            logger.warning("Got an unhealthy driver from the pool. Attempting to replace.")
            with self._lock:
                self._recreate_driver_if_needed(driver)

            if self._shutdown:
                 raise RuntimeError("WebDriverPool has been shut down while waiting for a healthy driver.")
            driver = self._pool.get()
            is_healthy = self._is_driver_healthy(driver)

        try:
            yield driver
        finally:
            if not self._shutdown:
                self._pool.put(driver)
            else:
                try:
                    driver.quit()
                except Exception:
                    pass

    def cleanup(self):
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
            
            while not self._pool.empty():
                try:
                    driver = self._pool.get_nowait()
                    driver.quit()
                except queue.Empty:
                    break 
                except Exception:
                    logger.warning("Error quitting driver during cleanup.", exc_info=True)
            
            self._current_size = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False


def str2int(s: str) -> int:
    """
    Examples:
    -----
    >>> str2int("295,137")
    295137
    >>> str2int("1.7k")
    1700
    >>> str2int("3.1m")
    3100000
    >>> str2int("38k")
    38000
    >>> str2int("")
    0
    >>> str2int(None)
    0
    >>> str2int("-")
    0
    >>> str2int(1234)
    1234
    """
    if isinstance(s, int):
        return s
    if s is None or s == "" or s == "-":
        return 0
    if "," in s:
        s = s.replace(",", "")
    try:
        if "k" in s or "K" in s:
            s = s.replace("k", "").replace("K", "")
            return int(float(s) * 1_000)
        elif "m" in s or "M" in s:
            s = s.replace("m", "").replace("M", "")
            return int(float(s) * 1_000_000)
        elif "b" in s or "B" in s:
            s = s.replace("b", "").replace("B", "")
            return int(float(s) * 1_000_000_000)
        else:
            return int(s)
    except ValueError as e:
        raise Exception("str2int error") from e