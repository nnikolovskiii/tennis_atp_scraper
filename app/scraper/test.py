import uuid
import time
import cloudscraper
from bs4 import BeautifulSoup

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.scraper.scrape_utils import get_driver, get_page
from app.utils.subject_scraper import Link
from tqdm import tqdm
from pydantic import BaseModel

scraper = cloudscraper.create_scraper()

for link in tqdm([
    # "https://www.atptour.com/en/scores/stats-centre/archive/2023/5014/ms125",
    #"https://www.atptour.com/en/scores/stats-centre/archive/2023/416/ms077",
    "https://www.atptour.com/en/scores/match-stats/archive/2007/8996/ms004"
]):
    url = link
    response = scraper.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        print(soup.prettify())
