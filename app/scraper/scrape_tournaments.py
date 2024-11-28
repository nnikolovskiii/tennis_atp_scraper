import uuid
import time
import re
import cloudscraper
from bs4 import BeautifulSoup
from nltk.app.wordnet_app import explanation

from app.databases.mongo_database.mongo_database import MongoDBDatabase, MongoEntry
from app.models.rdl import Statistic
from app.scraper.scrape_utils import get_driver, get_page
from app.utils.subject_scraper import Link
from tqdm import tqdm
from pydantic import BaseModel

class Tournament(MongoEntry):
    name: str
    venue: str
    date: str
    url: str


def retrieve_tournament_info(driver, link, mdb: MongoDBDatabase, tournament):
    soup = get_page(driver, link, 0.1)
    atp_logo = soup.find("img", {"class": "atp_logo"})

    try:
        t_details = soup.find("div", {"class": "tourn_details"})
        uls = t_details.find_all("ul")

        left = uls[0].find_all("li", recursive=False)
        right = uls[1].find_all("li", recursive=False)

        surface = left[2].find_all("span", recursive=False)[1].text.strip()
        location = right[0].find_all("span", recursive=False)[1].text.strip()
        prize = left[3].find_all("span", recursive=False)[1].text.strip()
        total_fin_commitment = right[3].find_all("span", recursive=False)[1].text.strip()

        mdb.add_entry_dict(
            entity={
                "surface": surface,
                "location": location,
                "prize": prize,
                "total_fin_commitment": total_fin_commitment,
                "tournament": tournament,
                "atp_logo": atp_logo.get("alt"),
                "link": link
            },
            collection_name="TournamentDetails1"
        )
    except Exception as e:
        mdb.add_entry_dict(
            {"surface": "", "location": "", "prize": "", "total_fin_commitment": "", "atp_logo": "",
             "tournament": tournament, "link":link},
            collection_name="TournamentDetails1"
        )
        #mdb.add_entry_dict(entity={"url": link}, collection_name="BadTournament")


def scrape_tournaments():
    scraper = cloudscraper.create_scraper()
    driver = get_driver()

    mdb = MongoDBDatabase()
    links = mdb.get_entries(Link, doc_filter={"link_type": "tennis_years"})
    existing_links = set([t.url for t in mdb.get_entries(Tournament)])

    for link in tqdm(links):
        url = link.url
        response = scraper.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            tournaments = soup.find_all("ul", class_="events")
            tournament_links = soup.find_all("a", class_="tournament__profile")

            for tournament, t_link in tqdm(zip(tournaments, tournament_links), total=len(tournaments)):
                description = tournament.find("div", class_="tournament-info")
                top = description.find("div", class_="top")
                bottom = description.find("div", class_="bottom")
                venue = description.find("span", class_="venue")
                date = description.find("span", class_="Date")

                name_v = top.text.strip()
                venue_v = venue.text.strip()
                date_v = date.text.strip()

                link = tournament.find('a', text=lambda text: text and "Results" in text)
                if link:
                    url = "https://www.atptour.com" + link.get('href')
                    if url in existing_links:
                        retrieve_tournament_info(driver, "https://www.atptour.com"+t_link.get('href'), mdb, url)




                #
                # if link:
                #     mdb.add_entry(Tournament(
                #         id=str(uuid.uuid4()),
                #         url="https://www.atptour.com" + link.get('href'),
                #         name=name_v,
                #         venue=venue_v,
                #         date=date_v,
                #     ))
                #

def get_tournament_details():
    mdb = MongoDBDatabase()
    t_details = mdb.get_entries_dict("TournamentDetails1")
    t_links = {elem["tournament"] for elem in t_details}
    tournaments =  mdb.get_entries_dict("Tournament")
    t_links1 = {elem["url"] for elem in tournaments}
    li = [0 for elem in t_links1 if elem not in t_links]
    print(len(li))

def fix_tournament_details():
    mdb = MongoDBDatabase()
    tournaments = mdb.get_entries_dict("TournamentDetails1")
    dict = {}
    for tournament in tournaments:
        key = tournament["link"]
        dict[key] = tournament

    for key, value in dict.items():
        new_dict = {}
        new_dict["url"] = value["link"]
        new_dict["surface"] = value["surface"]
        li = value["location"].split(",")
        if len(li) == 2:
            new_dict["city"] = li[0].strip()
            new_dict["country"] = li[1].strip()
        else:
            new_dict["city"] = value["location"].strip()
            new_dict["country"] = value["location"].strip()
        new_dict["prize_money"] = value["prize"]
        new_dict["points"] = value["atp_logo"]
        dict[key] = new_dict

    bad_tournaments = mdb.get_entries_dict("BadTournament1")
    missing_data = {}
    for tournament in bad_tournaments:
        key = tournament["url"]
        tournament.pop("total_financial_commitment")
        missing_data[key] = tournament

        for key, value in dict.items():
            if key not in missing_data:
                pass
            else:
                value= missing_data[key]
        mdb.add_entry_dict(
            value,
            collection_name="AllTournaments"
        )


def fix_columns():
    mdb = MongoDBDatabase()
    tournaments = mdb.get_entries_dict("AllTournaments")
    dict = {}
    for tournament in tournaments:
        surface = tournament["country"]
        if surface in ["", "Challenger tour"]:
            print(tournament["url"])
            print(surface)
        if surface not in dict:
            dict[surface] = 0
        dict[surface] += 1

    print(len(dict))
    print(dict)

    # for key, value in dict.items():
    #     new_dict = {}
    #     new_dict["prev"] = key
    #     new_dict["next"] = ""
    #     mdb.add_entry_dict(new_dict, "TournamentPoints")

def fix_prize_column():
    mdb = MongoDBDatabase()
    tournaments = mdb.get_entries_dict("AllTournaments")

    for tournament in tournaments:
        prize = tournament["prize_money"]
        if isinstance(prize, str):
            integers = re.findall(r'\d+', prize)
            s = ""
            for elem in integers:
                s+=elem
            if s != "":
                tournament["prize_money"] = int(s)
            else:
                tournament["prize_money"] = 0
            mdb.update_entity_dict(tournament, "AllTournaments")

def fix_c():
    mdb = MongoDBDatabase()
    tournaments = mdb.get_entries_dict("AllTournaments")
    fixed_values = mdb.get_entries_dict("TournamentPoints")

    dict = {}
    for elem in fixed_values:
        dict[elem["prev"]] = elem["next"]

    for elem in tournaments:
        surface = elem["points"]
        new_surface = dict[surface]
        elem["points"] = new_surface

        mdb.update_entity_dict(elem, "AllTournaments")

def check():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchData1")

    s = {match["tournament"] for match in matches}
    tournaments = mdb.get_entries_dict("TournamentDetails1")
    s1 = {elem["tournament"] for elem in tournaments}

    li = [1 for elem in s if elem not in s1]
    li1 = [1 for elem in s1 if elem not in s]

    print(len(li), len(li1))

def check1():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("AllTournaments")

    s = {match["url"] for match in matches}
    tournaments = mdb.get_entries_dict("TournamentDetails1")
    s1 = {elem["link"] for elem in tournaments}

    li = [1 for elem in s if elem not in s1]
    li1 = [1 for elem in s1 if elem not in s]

    print(len(li), len(li1))

def delete_matches():
    mdb = MongoDBDatabase()
    t_details = mdb.get_entries_dict("AllTournaments")
    tournaments = mdb.get_entries_dict("TournamentDetails1")
    dict ={}
    for elem in tournaments:
        link = elem["link"]
        if link not in dict:
            dict[link] = []
        dict[link].append(elem["tournament"])

    links = [elem["url"] for elem in t_details if elem["points"] ==0]
    li = []
    for link in links:
        if link in dict:
            li.extend(dict[link])

    count = 0
    matches = mdb.get_entries_dict("MatchData1")
    s = set(li)
    for match in matches:
        tournament = match["tournament"]
        if tournament in s:
            count += 1

    # mdb.add_entry(
    #     Statistic(explanation=f"Number of matches that are part of tournaments which are not standard tournaments {count}\n"
    #                           f"Total num of matches: {len(matches)}\n"
    #                           f"Percentage: {count/len(matches)*100:.2f}%\n")
    # )
    print(count)
    print(len(li))

def fix_t():
    mdb = MongoDBDatabase()
    t1 = {d["link"]: d["tournament"] for d in mdb.get_entries_dict("TournamentDetails1")}
    t2 = mdb.get_entries_dict("AllTournaments")

    for t in t2:
        link = t["tournament"]
        if link in t1:
            t["tournament"] = t1[link]
            mdb.update_entity_dict(t, "AllTournaments")

