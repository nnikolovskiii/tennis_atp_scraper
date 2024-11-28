import uuid
from io import text_encoding

import cloudscraper
from bs4 import BeautifulSoup

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.models.models import PlainText
from app.scraper.scrape_ratings import Rank
from app.utils.subject_scraper import Link
from tqdm import tqdm
from pydantic import BaseModel


def scrape_ratings_updated():
    scraper = cloudscraper.create_scraper()

    mdb = MongoDBDatabase()
    links = mdb.get_entries(Link, doc_filter={"link_type": "tennis_ratings"})

    ranks = mdb.get_entries(Rank)
    dates = set()
    for rank in tqdm(ranks):
        date = rank.date
        if date[0] != "h":
            dates.add(date)
        else:
            new_date= date.split("dateWeek=")[1].split("&")[0]
            dates.add(new_date)
    print(dates)
    # for link in tqdm(links[55+944:]):
    #     url = link.url
    #     response = scraper.get(url)
    #
    #     if response.status_code == 200:
    #         soup = BeautifulSoup(response.text, 'html.parser')
    #         table = soup.find("table", class_=["mobile-table", "mega-table", "non-live"])
    #         trs = table.find_all("tr", class_="lower-row")
    #
    #         for tr in trs:
    #             rank = tr.find("td", class_=["rank", "bold", "heavy", "tiny-cell"]).text.strip()
    #             player = tr.find("li", class_= "name").find("a").get("href")
    #             points = tr.find("td", class_=["points"]).text.strip()
    #
    #             mdb.add_entry(
    #                 Rank(
    #                     id=str(uuid.uuid4()),
    #                     player="https://www.atptour.com"+player,
    #                     rank=rank,
    #                     points=points,
    #                     date = url
    #                 )
    #             )

scrape_ratings_updated()