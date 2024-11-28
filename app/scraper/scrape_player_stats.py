import uuid
from itertools import count

import cloudscraper
from bs4 import BeautifulSoup
from nltk.app.wordnet_app import explanation

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.models.rdl import Statistic
from app.scraper.scrape_utils import get_page, get_driver
from tqdm import tqdm

def analyze_player_info():
    mdb = MongoDBDatabase()
    ranks = mdb.get_entries_dict("Rank")

    players = set()
    dict = {}

    for rank in ranks:
        pl = rank["player"]
        rank_pos = rank["rank"]

        if pl not in dict:
            dict[pl] = []

        dict[pl].append(rank_pos)

    dict1 = {}
    for key, value in dict.items():
        value = sorted(value)
        dict1[key] = value[0]

    lowest_rank = None
    lowest_rank_pl = None
    for key, value in dict1.items():
        if lowest_rank is None or value > lowest_rank:
            lowest_rank = value
            lowest_rank_pl = key



    print(len(players))
    print(lowest_rank, lowest_rank_pl)

def scrape_player_info():
    mdb = MongoDBDatabase()
    ranks = mdb.get_entries_dict("Rank")
    existing_players = mdb.get_entries_dict("PlayerHtmlDetails")

    existing_players_links = set()
    for player in existing_players:
        pl = player['player']
        existing_players_links.add(pl)

    player_links = set()

    for rank in ranks:
        pl = rank["player"]
        player_links.add(pl)

    players = [pl for pl in player_links if pl not in existing_players_links]

    driver = get_driver()
    for player_link in tqdm(players):
        try:
            soup = get_page(driver, player_link)
            personal_details = soup.find("div", class_= "personal_details")

            text = personal_details.text

            if "Personal details" in text and "Weight" in text and "Height" in text:
                mdb.add_entry_dict(
                    {"player": player_link, "details": personal_details.prettify()},
                    "PlayerHtmlDetails"
                )
            else:
                print(player_link)
        except Exception as e:
            continue


def scrape_info_from_html():
    mdb = MongoDBDatabase()

    players = mdb.get_entries_dict("PlayerHtmlDetails")

    count = 0
    s = set()
    height_set = set()
    weight_set = set()
    s3 = set()
    s4 = set()
    for player in tqdm(players):
        details = player["details"]
        player_link = player["player"]

        soup = BeautifulSoup(details, "html.parser")
        lis = soup.find_all("li")

        weight = lis[1].find_all("span")[1].text.strip().split("(")[1].split(")")[0].replace("kg", "").strip()
        height = lis[2].find_all("span")[1].text.strip().split("(")[1].split(")")[0].replace("cm", "").strip()
        birth_place = ""
        plays = ""
        for i, li in enumerate(lis):
            if "Birthplace" in li.text:
                birth_place = li.find_all("span")[1].text.strip()
            if "Plays" in li.text:
                plays = li.find_all("span")[1].text.strip()

        good_stat = True

        if height == "":
            s.add(player_link)
            good_stat = False
        else:
            height = int(height)
            height_set.add(height)
            if height < 163:
                s.add(player_link)
                good_stat = False



        if weight == "":
            s.add(player_link)
            good_stat = False
        else:
            weight = int(weight)
            weight_set.add(weight)
            if weight < 59:
                s.add(weight)
                good_stat = False

        if good_stat:
            mdb.add_entry_dict({
                "player": player_link,
                "height": height,
                "weight": weight,
                "birthplace": birth_place,
                "style": plays
            }, collection_name="PlayerStats")

    print(s3)
    print(s4)
    count = 0
    matches = mdb.get_entries_dict("MatchData1")
    for match in matches:
        pl1 = match["player_link_1"]
        pl2 = match["player_link_2"]

        if pl1 in s or pl2 in s:
            count+=1

    print(count/len(matches)*100)
    print(height_set)
    print(weight_set)
    print(s3)

    #
    # mdb.add_entry(
    #     Statistic(explanation=f"These are all the values for height and weight in that order:\n{height_set}\n{weight_set}\n"
    #                           f"Players who do not have wieght or height, or their height is below 163 and weight below 59 are dropped.\n"
    #                           f"Their percentage is: {count/len(matches)*100:.2f}%",),
    # )

def analyze_player_stats():
    mdb = MongoDBDatabase()
    player_stats = mdb.get_entries_dict("PlayerStats")
    match_impact = {"hand":{}, "backend":{}}

    for stat in player_stats:
        player = stat["player"]
        style = stat["style"]
        li = style.split(",")
        hand = li[0].strip()
        backend_style = li[1].strip()

        if hand not in match_impact["hand"]:
            match_impact["hand"][hand] = set()

        if backend_style not in match_impact["backend"]:
            match_impact["backend"][backend_style] = set()

        match_impact["hand"][hand].add(player)
        match_impact["backend"][backend_style].add(player)
        stat["hand"] = hand
        mdb.update_entity_dict(stat, collection_name="PlayerStats")

    matches = mdb.get_entries_dict("MatchData1")
    for key in ["hand", "backend"]:
        dict = match_impact[key]
        for key1, value1 in dict.items():
            count = 0
            for match in matches:
                pl1 = match["player_link_1"]
                pl2 = match["player_link_2"]

                if pl1 in value1 or pl2 in value1:
                    count += 1

            print(key, key1, count / len(matches) * 100)

    #mdb.add_entry(Statistic(explanation=f"There is not enough information on backend style: {backend_styles}"))

def delete_match_stats():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchStats")
    players_with_stats = [stat["player"] for stat in mdb.get_entries_dict("PlayerStats")]
    for match in matches:
        pl = match["player"]
        if pl not in players_with_stats:
            mdb.delete_entity_dict(match, collection_name="MatchStats")

def delete_ranks():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("Rank")
    players_with_stats = [stat["player"] for stat in mdb.get_entries_dict("PlayerStats")]
    count = 0
    for match in matches:
        pl = match["player"]
        if pl not in players_with_stats:
            count+=1
            mdb.delete_entity_dict(match, collection_name="Rank")

    print(count/len(matches) * 100)

def see_tournaments():
    mdb = MongoDBDatabase()
    lol = {stat["tournament"] for stat in mdb.get_entries_dict("Matches")}

    matches = mdb.get_entries_dict("AllTournaments")
    li =[1 for t in matches if t["tournament"] not in lol]
    print(len(li)/len(matches)*100)
    # for match in matches:
    #     if match["match"] not in match_stats:
    #         mdb.delete_entity_dict(match, collection_name="Matches")


see_tournaments()
# soup = BeautifulSoup("""<div class="personal_details"><h3 class="pd_header">Personal details</h3><div class="pd_content"><ul class="pd_left"><li><span>DOB</span><span>1988/11/14</span></li><li><span>Weight</span><span>178 lbs (81kg)</span></li><li><span>Height</span><span>6'2" (188cm)</span></li><li><span>Turned pro</span><span></span></li><li><span>Follow player</span><span><div class="social"><ul></ul></div></span></li></ul><ul class="pd_right"><li><span>Country</span><span class="flag">Australia <svg class="atp-flag"><use href="/assets/atptour/assets/flags.svg#flag-aus"></use></svg></span></li><li><span>Birthplace</span><span>Hobart, Australia</span></li><li><span>Plays</span><span>Right-Handed, Two-Handed Backhand</span></li><li><span>Coach</span><span></span></li></ul></div></div>
# """,  'html.parser')
#
# lis = soup.find_all("li")
# print(lis)