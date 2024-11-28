from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.models.rdl import Player
from tqdm import tqdm
import re
from datetime import datetime,timedelta

from app.scraper.analyze_data import get_remaining_stats
from app.scraper.scrape_matches import OldMatch
from app.scraper.scrape_tournaments import Tournament
from app.scraper.scrape_stats import Match


replacements = {
    'Host City Finals': 'Final',
    'Finals': 'Final',
    'Semi': 'Semifinals',
    'Quarter':'Quarterfinals',
    'Olympic Bronze':'3rd/4th Place Match',
    'Round Robin Day 4':'Quarterfinals',
    'Round Robin':'Quarterfinals',
    'Round Robin Day 2':'Quarterfinals',
    'Round Robin Day 5':'Quarterfinals',
    'Round Robin Day 3':'Quarterfinals',
    'Round Robin Day 6':'Quarterfinals'
}

priority = ['Final', 'Semifinals', 'Quarterfinals', 'Round of 16', 'Round of 32', 'Round of 64',
            'Round of 128', '1st Round Qualifying', '2nd Round Qualifying', '3rd Round Qualifying']

def _transform_into_datetime(
        day:str,
        month: str,
        year: str
):
    date_str = f"{day} {month} {year}"
    return datetime.strptime(date_str, "%d %B %Y")

def _get_date(date):
    li = date.split(",")
    year = li[1].strip()
    day_month = li[0].strip().split(" ")
    day = day_month[0].strip()
    month = day_month[1].strip()
    return day, month, year

def _get_intermediate_dates(start_date, end_date, n):
    delta = (end_date - start_date) / (n + 1)
    return [start_date + i * delta for i in range(1, n + 1)]

def _split_day_into_partitions(date, n):
    day_seconds = 24 * 60 * 60
    partition_interval = day_seconds // n
    return [date + timedelta(seconds=i * partition_interval) for i in range(n)]

def create_player_collection():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries(collection_name="OldMatch")
    player_dict = {}
    for match in matches:
        if "player_name_1" not in match or "player_name_2" not in match:
            print(match)
        else:
            player1_name = match["player_name_1"]
            player1_url = match["player_link_1"]
            player2_name = match["player_name_2"]
            player2_url = match["player_link_2"]

            if player1_url not in player_dict:
                player_dict[player1_url] = player1_name

            if player2_url not in player_dict:
                player_dict[player2_url] = player2_name

    print(len(player_dict))

    for player_url, player_name in tqdm(player_dict.items(), total=len(player_dict)):
        mdb.add_entry(Player(name=player_name, url=player_url))


def get_tournament_dates():
    mdb = MongoDBDatabase()
    tournaments = mdb.get_entries(collection_name="Tournament")
    tournament_dict = {}

    for tournament in tournaments:
        date = tournament["date"]
        pattern = r"(\d{1,2} [A-Za-z]+, \d{4}) - (\d{1,2} [A-Za-z]+, \d{4})"
        matches = re.findall(pattern, date)
        match = re.search(r'\b\d+\s*-\s*\d+\b', date)
        from_date = None
        to_date = None

        if matches:
            start_date, end_date = matches[0]
            day1, month1, year1 = _get_date(start_date)
            day2, month2, year2 = _get_date(end_date)

            from_date = _transform_into_datetime(day1, month1, year1)
            to_date = _transform_into_datetime(day2, month2, year2)
        elif match:
            days = match.group().strip()
            li = days.split("-")
            day1 = li[0].strip()
            day2 = li[1].strip()

            li = date.split(days)[1].strip()
            li = li.split(",")
            month = li[0].strip()
            year = li[1].strip()

            from_date = _transform_into_datetime(day1, month, year)
            to_date = _transform_into_datetime(day2, month, year)


        else:
            li = date.split(",")
            year = li[1].strip()
            li = li[0].split("-")
            date1 = li[0].strip().split(" ")
            date2 = li[1].strip().split(" ")
            day1 = date1[0].strip()
            month1 = date1[1].strip()
            day2 = date2[0].strip()
            month2 = date2[1].strip()

            from_date = _transform_into_datetime(day1, month1, year)
            to_date = _transform_into_datetime(day2, month2, year)

        tournament_dict[tournament["url"]] = (from_date, to_date)

    return tournament_dict

def update_match_levels():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries(class_type=Match)

    s = set()

    for match in tqdm(matches):
        level = match.level.split("-")[0].strip()
        s.add(level)
        if level in replacements:
            level = replacements[level]
        if level not in priority:
            mdb.delete_entity(match)
            continue

        new_level = priority.index(level) + 1
        match.level = new_level
        mdb.update_entity(match)

def add_match_dates():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries(class_type=Match)

    s = set()
    count = 0
    for match in matches:
        if match.url in s:
            count+=1
            mdb.delete_entity(match)
        else:
            s.add(match.url)
    print(count)

    matches = mdb.get_entries(class_type=Match)
    tournament_di = {}
    for match in matches:
        if match.tournament not in tournament_di:
            tournament_di[match.tournament] = {}
        if match.level not in tournament_di[match.tournament]:
            tournament_di[match.tournament][match.level] = []
        tournament_di[match.tournament][match.level].append(match)

    dates_di = get_tournament_dates()
    for tournament, matches_di in tqdm(tournament_di.items(), total=len(tournament_di)):
        from_date, to_date = dates_di[tournament]
        dates = _get_intermediate_dates(from_date, to_date, len(matches_di))
        sorted_matches = sorted(matches_di.items(), key=lambda item: item[0])
        list_matches = [elem for _, elem in sorted_matches]
        for date, matches in zip(dates, list_matches):
            times = _split_day_into_partitions(date, len(matches))
            for time, match in zip(times, matches):
                match.date = time
                mdb.add_entry(match, collection_name="MatchDate")


def update_venues():
    mdb = MongoDBDatabase()
    tournaments = mdb.get_entries(Tournament)
    s = set()
    s1 = set()

    for tournament in tournaments:
        li = tournament.venue.split("|")[0].strip().split(",")
        city = li[0].strip()
        country = li[1].strip()
        s.add(city)
        s1.add(country)

    print(s)
    print(s1)

def remove_bad_matches():
    mdb = MongoDBDatabase()
    matches_dict = mdb.get_entries_dict("OldMatch")
    s = set()
    count = 0
    remaining_urls = set(get_remaining_stats())
    for match in tqdm(matches_dict, total=len(matches_dict)):
        url = match["parent_url"]
        if url in remaining_urls:
            count += 1
            mdb.delete_entity_dict(match, "OldMatch")

    print(count)

def check_match_duplicates():
    mdb = MongoDBDatabase()
    matches_dict = mdb.get_entries_dict("OldMatch")
    s = set()
    for match in matches_dict:
        s.add(match["parent_url"])

    print(len(s), len(matches_dict))

def check_sets():
    mdb = MongoDBDatabase()
    matches_dict = mdb.get_entries_dict("OldMatch")

    count = 0
    for match in matches_dict:
        if "player_1_winner" not in match:
            count +=1

    print(count)
