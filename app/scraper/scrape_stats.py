import uuid
from datetime import datetime
from typing import Union, Optional

import cloudscraper
from bs4 import BeautifulSoup

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.scraper.scrape_tournaments import Tournament
from tqdm import tqdm
from pydantic import BaseModel

class Match(BaseModel):
    id: str
    level: Union[str, int]
    time: Optional[str] = None
    url: str
    tournament: str
    date: Optional[datetime] = None

def scrape_matches():
    scraper = cloudscraper.create_scraper()
    mdb = MongoDBDatabase()
    tournaments = mdb.get_entries(Tournament)

    for tournament in tqdm(tournaments[1679:]):
        url = tournament.url

        response = scraper.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            matches = soup.find_all('div', class_="match")

            for match in matches:
                header = match.find('div', class_="match-header")
                spans = header.find_all('span')

                link = match.find('a', text=lambda text: text and "Stats" in text)

                level = time = None
                if len(spans) >= 1:
                    level = spans[0].text.strip()

                if len(spans) >= 2:
                    time = spans[1].text.strip()

                if link:
                    mdb.add_entry(Match(
                        id =str(uuid.uuid4()),
                        url="https://www.atptour.com" + link.get('href'),
                        level=level,
                        time=time,
                        tournament=tournament.url
                    ))
