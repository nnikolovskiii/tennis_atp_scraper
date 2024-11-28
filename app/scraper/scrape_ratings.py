import uuid
from datetime import datetime
from io import text_encoding
from concurrent.futures import ThreadPoolExecutor, as_completed
import cloudscraper
from bs4 import BeautifulSoup
from nltk.app.wordnet_app import explanation

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.models.models import PlainText
from app.models.rdl import Statistic
from app.utils.subject_scraper import Link
from tqdm import tqdm
from pydantic import BaseModel


class Rank(BaseModel):
    id: str
    date: str
    player: str
    rank: str
    points: str

class RatingLink(BaseModel):
    id: str
    url: str
    date: str

def _split_list(data, num_parts):
    avg = len(data) // num_parts
    return [data[i * avg: (i + 1) * avg] for i in range(num_parts - 1)] + [data[(num_parts - 1) * avg:]]

def scrape_rating_years():
    scraper = cloudscraper.create_scraper()

    mdb = MongoDBDatabase()

    url = "https://www.atptour.com/en/rankings/singles?dateWeek=1992-11-09&rankRange=0-5000"
    response = scraper.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        select = soup.find("select", id="dateWeek-filter")

        options = select.find_all("option")
        for option in options:
            date = option.text.strip()
            year = int(date.split(".")[0])
            if year < 2000:
                continue
            mdb.add_entry(
                RatingLink(
                    id=str(uuid.uuid4()),
                    url=f"https://www.atptour.com/en/rankings/singles?dateWeek={date}&rankRange=0-5000",
                    date=date
                ))


def scrape_ratings(
        links
):
    scraper = cloudscraper.create_scraper()
    mdb = MongoDBDatabase()
    for link in tqdm(links):
        url = link.url
        response = scraper.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find("table", class_=["mobile-table", "mega-table", "non-live"])
            trs = table.find_all("tr", class_="lower-row")

            for tr in trs:
                rank = tr.find("td", class_=["rank", "bold", "heavy", "tiny-cell"]).text.strip()
                player = tr.find("li", class_= "name").find("a").get("href")
                points = tr.find("td", class_=["points"]).text.strip()

                if "T" in rank:
                    rank = rank.split("T")[0]

                mdb.add_entry(
                    Rank(
                        id=str(uuid.uuid4()),
                        player="https://www.atptour.com"+player,
                        rank=rank,
                        points=points,
                        date = link.date
                    )
                )


def remove_ranks(save: bool = False, delete: bool = False):
    mdb = MongoDBDatabase()
    ranks = mdb.get_entries_dict("Rank")
    rank_players = {rank["player"] for rank in ranks}

    matches =  mdb.get_entries_dict("MatchData1")
    players = set()
    [players.add(match["player_link_1"]) for match in matches]
    [players.add(match["player_link_2"]) for match in matches]

    players1 = set()
    for pl in players:
        players1.add(pl.replace(" ", "-"))

    li = {player for player in players1 if player not in rank_players}

    count = 0
    for match in matches:
        pl1 = match["player_link_1"]
        pl2 = match["player_link_2"]

        if pl1 in li or pl2 in li:
            count+=1
            if delete:
                mdb.delete_entity_dict(match, collection_name="MatchData1")

    print(count)
    if save:
        mdb.add_entry(
            Statistic(explanation=f"Matches that do not have ranked players in them: {count}\n"
                                  f"Total number of matches: {len(matches)}\n"
                                  f"Percentage: {count/len(matches)*100:.2f}%",),
        )
        print("Saved")

def update_links_in_matches():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchData1")

    count = 0
    for match in tqdm(matches):
        pl1 = match["player_link_1"]
        pl2 = match["player_link_2"]

        if " " in pl1:
            pl1 = pl1.replace(" ", "-")

        if " " in pl2:
            pl2 = pl2.replace(" ", "-")
        match["player_link_1"] = pl1
        match["player_link_2"] = pl2

        mdb.update_entity_dict(match, collection_name="MatchData1")

    print(count)

def check_player_in_ranks(delete: bool = False):
    mdb = MongoDBDatabase()
    ranks = mdb.get_entries_dict("Rank")
    rank_players = {rank["player"] for rank in ranks}

    matches = mdb.get_entries_dict("MatchData1")
    players = set()
    [players.add(match["player_link_1"]) for match in matches]
    [players.add(match["player_link_2"]) for match in matches]

    s = set()
    count = 0
    for rank in tqdm(ranks):
        pl = rank["player"]
        if pl not in players:
            count +=1
            if delete:
                mdb.delete_entity_dict(rank, collection_name="Rank")
        else:
            s.add(pl)

    print(count)

def convert_column_types(save: bool = False):
    mdb = MongoDBDatabase()
    ranks = mdb.get_entries_dict("Rank")

    change= False
    for rank in tqdm(ranks):
        if isinstance(rank["points"], str):
            if rank["points"] == "" or rank["points"].strip() == "-":
                rank["points"] = 0
            else:
                rank["points"] = int(rank["points"].replace(",", "").replace("-", ""))
            change = True
        if isinstance(rank["date"], str):
            rank["date"] =  datetime.strptime(rank["date"], "%Y.%m.%d")
            change = True
        if isinstance(rank["rank"], str):
            rank["rank"] = int(rank["rank"].replace("-", ""))
            change = True

        if save and change:
            mdb.update_entity_dict(rank, collection_name="Rank")


convert_column_types(save=True)