import uuid
from copy import deepcopy
from datetime import datetime
from itertools import count

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.models.models import PlainText
from app.scraper.analyze_data import get_remaining_stats
from tqdm import tqdm
from pydantic import BaseModel

from app.scraper.scrape_utils import get_driver, get_page


class OldMatch(BaseModel):
    id: str
    pass

def check_soup(soup, link, mdb:MongoDBDatabase):
    good_check = True
    stat_groups = soup.find_all("div", class_="stats-group")

    if "Loading Match Stats..." in soup.prettify() or "Verify you are human by completing the action below." in soup.prettify():
        print("Gotchu")
        return True

    if len(stat_groups) == 0:
        stat_groups = soup.find_all("div", class_="stat-section")
        if(len(stat_groups) == 0):
            good_check = False

    if not good_check:
        mdb.add_entry(PlainText(text=link), metadata={"desc": "bad_link"})
    return True

def scrape_match_stats():
    driver = get_driver()
    mdb = MongoDBDatabase()
    links = get_remaining_stats()
    print(len(links))

    bad_links = mdb.get_entries(PlainText, {"desc": "bad_link"})
    bad_links = [elem.text for elem in bad_links]
    links = [link for link in links if link not in bad_links]

    for parent_link in tqdm(links):
        try:
            url = parent_link

            soup = get_page(driver, url, 20)
            check_soup(soup, url, mdb)
            match_div = soup.find("div", class_="match")
            li = soup.find_all("div", class_="stats-item")
            di = {"parent_url": parent_link}

            for i, player in enumerate(li):
                divs = player.find_all("div")
                a_link = divs[0].find("a")
                di[f"player_name_{i+1}"] = divs[0].text
                di[f"player_link_{i + 1}"] = "https://www.atptour.com"+a_link.get("href")

                score_items = player.find_all("div", class_="score-item")
                for j, score_item in enumerate(score_items):
                    di[f"player_{i+1}_set_{j+1}"] = score_item.text

                winner = player.find_all("div", class_="winner")
                if len(winner) > 0:
                    di[f"player_{i+1}_winner"] = True
                else:
                    di[f"player_{i + 1}_winner"] = False

            stat_groups = soup.find_all("div", class_="stats-group")


            if len(stat_groups) == 0:
                stat_groups = soup.find_all("div", class_="stat-section")
                for stat_group in stat_groups:
                    li = stat_group.find_all("div",class_="statTileWrapper")
                    for elem in li:
                        elem = elem.find("div", class_=["desktopView", "top-stat"])
                        divs = elem.find_all("div", recursive=False)
                        category = divs[1].text.strip()
                        if category == "First serve":
                            category = "1st Serve"
                        elif category == "1st serve points won":
                            category = "1st Serve Points Won"
                        elif category == "2nd serve points won":
                            category = "2nd Serve Points Won"
                        di[category + "_1"] = divs[0].text.strip()
                        di[category + "_2"] = divs[2].text.strip()
            else:
                for stat_group in stat_groups:
                    li = stat_group.find_all("li")
                    for elem in li:
                        divs = elem.find_all("div", recursive=False)
                        di[divs[1].text.strip()+"_1"] = divs[0].text.strip()
                        di[divs[1].text.strip()+"_2"] = divs[2].text.strip()

            mdb.add_entry(OldMatch(id=str(uuid.uuid4())), metadata=di)

        except Exception as e:
            print(e)
            continue

def add_sets(save: bool = False):
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchData1")

    s = set()

    for match in tqdm(matches):
        pl1 = match["player_link_1"]
        pl2 = match["player_link_2"]

        match_link = match["parent_url"]
        dict = {}
        for key, value in match.items():
            if "set" in key:
                player = set_value = key.split("set")[0]
                value = value[0]
                set_number = int(key[-1])
                if set_number not in dict:
                    dict[set_number] = {}
                if "1" in player:
                    dict[set_number]["1"] = value
                else:
                    dict[set_number]["2"] = value


        for key, values in dict.items():
            if "1" in values:
                set1 = values["1"]
            else:
                set1 = "0"

            if "2" in values:
                set2 = values["2"]
            else:
                set2 = "0"

            if len(set1) > 1 and len(set2) > 1 and set1[0] == "7" and set2[0] == "6":
                set1 = "7"
                set2 = "6"

            if save:
                mdb.add_entry_dict({
                    "set_number": key,
                    "player": pl1,
                    "value": int(set1),
                    "match": match_link
                }, collection_name="Sets")

                mdb.add_entry_dict({
                    "set_number": key,
                    "player": pl2,
                    "value": int(set2),
                    "match": match_link
                }, collection_name="Sets")


def fix_sets():
    mdb = MongoDBDatabase()
    sets = mdb.get_entries_dict("Sets")

    count = 0
    s = set()
    for set_dict in sets:
        value = set_dict["value"]
        match = set_dict["match"]



    print(count , len(sets))


def create_match_stats(save: bool = False):
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchData1")

    s = set()

    for match in tqdm(matches):
        pl1 = match["player_link_1"]
        pl2 = match["player_link_2"]

        match_link = match["parent_url"]
        dict = {"1":{"player": pl1, "match":match_link}, "2":{"player":pl2, "match": match_link}}
        for key, value in match.items():
            if "set" not in key and ("_1" in key or "_2" in key):
                if "_1" in key:
                    key = key.replace("_1", "")
                    dict["1"][key] = value
                else:
                    key = key.replace("_2", "")
                    dict["2"][key] = value

        for key, stat_dict in dict.items():
            if save:
                mdb.add_entry_dict(stat_dict, "MatchStats")


def convert_col_to_int(save: bool = False):
    mdb = MongoDBDatabase()
    match_stats = mdb.get_entries_dict("MatchStats")

    for stat in tqdm(match_stats):
        stat["Serve Rating"] = int(stat["Serve Rating"])
        stat["Double Faults"] = int(stat["Double Faults"])
        stat["Service Games Played"] = int(stat["Service Games Played"])
        stat["Return Rating"] = int(stat["Return Rating"])
        stat["Return Games Played"] = int(stat["Return Games Played"])
        stat["Aces"] = int(stat["Aces"])

        if save:
            mdb.update_entity_dict(stat, "MatchStats")


def create_matches(save: bool = False):
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchData1")

    s = set()

    for match in tqdm(matches):
        pl1 = match["player_link_1"]
        pl2 = match["player_link_2"]

        dict = {}
        for key, value in match.items():
            if "winner" in key or "set" in key or "_1" in key or "_2" in key:
                pass
            else:
                dict[key] = value

        if save:
            mdb.add_entry_dict(dict, "Matches")

def convert_time_obj(save: bool = False):
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("Matches")

    for match in tqdm(matches):
        time = match["time"]
        li = time.split(":")

        if len(li) == 3:
            time_obj = datetime.strptime(time, "%H:%M:%S").time()
        else:
            time_obj = datetime.strptime(time, "%H:%M").time()

        match["duration_minutes"] = time_obj.hour * 60 + time_obj.minute

        if save:
            mdb.update_entity_dict(match, "Matches")
