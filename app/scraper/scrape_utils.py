import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time

def get_driver():
    options = uc.ChromeOptions()
    options.binary_location = "/opt/google/chrome/chrome"  # Explicit binary path
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("start-maximized")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")

    driver = uc.Chrome(
        options=options,
        desired_capabilities=DesiredCapabilities.CHROME,
    )
    return driver

def get_page(driver, link, wait_time=2):
    driver.get(link)
    time.sleep(wait_time)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return soup