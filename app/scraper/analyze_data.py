from nltk.app.wordnet_app import explanation

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.models.rdl import Statistic
from app.utils.subject_scraper import Link
from tqdm import tqdm

def get_remaining_stats(
        delete: bool = False
):
    mdb = MongoDBDatabase()
    all_matches = mdb.get_entries_dict(collection_name="OldMatch")

    must_haves = ['parent_url', 'player_name_1', 'player_link_1', 'player_1_winner', 'player_name_2', 'player_link_2',
                  'player_2_winner', 'Serve Rating_1', 'Serve Rating_2', 'Aces_1', 'Aces_2', 'Double Faults_1',
                  'Double Faults_2', '1st Serve_1', '1st Serve_2', '1st Serve Points Won_1', '1st Serve Points Won_2',
                  '2nd Serve Points Won_1', '2nd Serve Points Won_2', 'Break Points Saved_1', 'Break Points Saved_2',
                  'Service Games Played_1', 'Service Games Played_2', 'Return Rating_1', 'Return Rating_2',
                  '1st Serve Return Points Won_1', '1st Serve Return Points Won_2', '2nd Serve Return Points Won_1',
                  '2nd Serve Return Points Won_2', 'Break Points Converted_1', 'Break Points Converted_2',
                  'Return Games Played_1', 'Return Games Played_2', 'Service Points Won_1', 'Service Points Won_2',
                  'Return Points Won_1', 'Return Points Won_2', 'Total Points Won_1', 'Total Points Won_2']

    must_haves1 = ['Serve Rating_1', 'Serve Rating_2', 'Aces_1', 'Aces_2', 'Double Faults_1',
                   'Double Faults_2', 'First serve_1', 'First serve_2', '1st serve points won_1',
                   '1st serve points won_2', '2nd serve points won_1', '2nd serve points won_2',
                   'Break Points Saved_1', 'Break Points Saved_2', 'Service Games Played_1',
                   'Service Games Played_2', 'Return Rating_1', 'Return Rating_2', '1st Serve Return Points Won_1',
                   '1st Serve Return Points Won_2', '2nd Serve Return Points Won_1', '2nd Serve Return Points Won_2',
                   'Break Points Converted_1', 'Break Points Converted_2', 'Return Games Played_1', 'Return Games Played_2',
                   'Service Points Won_1', 'Service Points Won_2', 'Return Points Won_1', 'Return Points Won_2', 'Total Points Won_1',
                   'Total Points Won_2']

    s1 = set()
    count = 0
    for match in all_matches:
        add = True
        if "parent_url" in match:
            match_keys = [str(key) for key in match.keys()]
            for key in must_haves:
                if key not in match_keys:
                    add = False

            if add:
                s1.add(match["parent_url"])
            else:
                count += 1
                if delete:
                    mdb.delete_entity_dict(match, "OldMatch")
    print(count)

    links = mdb.get_entries(class_type=Link, doc_filter={"link_type": "tennis_stats"})
    s = set()
    [s.add(link.url) for link in links]

    remaining = [elem for elem in s if elem not in s1]

    return remaining

def get_num_players():
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("OldMatch")
    s = set()
    for match in matches:
        pl1 = match["player_link_1"]
        pl2 = match["player_link_2"]
        s.add(pl1)
        s.add(pl2)

    print(len(s))

def analyze_match_dates(save:bool=False):
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchDate")
    s = set()
    count = 0
    for match in matches:
        time = match["time"]
        if time is None:
            mdb.delete_entity_dict(match, "MatchDate")
            count += 1

    print(count)

    if save:
        stat = Statistic(
            explanation=f"Number of matches that do not have time: {count}\n,"
                        f"Number of overall mathces: {len(matches)}\n"
                        f"Percentage of those that have no time: {(count/len(matches))*100}",
        )
        mdb.add_entry(stat)

def merge_matches(save:bool=False):
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("OldMatch")
    match_dates = mdb.get_entries_dict("MatchDate")
    dict = {}
    for match_date in match_dates:
        dict[match_date["url"]] = match_date

    count = 0
    for match in tqdm(matches):
        url = match["parent_url"]
        if url not in dict:
            mdb.delete_entity_dict(match, "OldMatch")
        else:
            more_info = dict[match["parent_url"]]
            match["level"] = more_info["level"]
            match["date"] = more_info["date"]
            match["time"] = more_info["time"]
            match["tournament"] = more_info["tournament"]

            mdb.add_entry_dict(match, "MatchData")
    if save:
        stat = Statistic(
            explanation=f"Number of matches that have level, time and date: {count}\n,"
                        f"Number of overall matches: {len(matches)}\n"
                        f"Percentage: {(count/len(matches))*100}",
        )
        mdb.add_entry(stat)

def remove_tournaments(save:bool=False):
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchData")
    m_tournaments = set([match["tournament"] for match in matches])
    tournaments = mdb.get_entries_dict("Tournament")
    tournament_links = set([tournament["url"] for tournament in tournaments])

    d_tournaments = [t for t in tournament_links if t not in m_tournaments]
    count = 0
    for tournament in tournaments:
        if tournament["url"] in d_tournaments:
            count += 1
            mdb.delete_entity_dict(tournament, "Tournament")
    print(count)
    if save:
        stat = Statistic(
            explanation=f"Number of tournaments not processed: {len(d_tournaments)}\n,"
                        f"Number of overall tournaments: {len(tournaments)}\n"
                        f"Percentage: {(len(d_tournaments) / len(tournaments)) * 100}",
        )
        mdb.add_entry(stat, metadata={"tournaments": d_tournaments})


def update_stats(save:bool=False):
    mdb = MongoDBDatabase()
    matches = mdb.get_entries_dict("MatchData")
    s = {
        "1st Serve_1",
        '1st Serve_2',
        '1st Serve Points Won_1'
        "1st Serve Points Won_1",
        "1st Serve Points Won_2",
        '1st Serve Points Won_2',
        '2nd Serve Points Won_1',
        '2nd Serve Points Won_2',
        "1st Serve Points Won_1",
        'Break Points Saved_1',
        'Break Points Saved_2',
        '1st Serve Return Points Won_1',
        '1st Serve Return Points Won_2',
        '2nd Serve Return Points Won_1',
        '2nd Serve Return Points Won_2',
        'Break Points Converted_1',
        'Break Points Converted_2',
        'Service Points Won_1',
        'Service Points Won_2',
        'Return Points Won_1',
        'Return Points Won_2',
        'Total Points Won_1',
        'Total Points Won_2',
    }
    s1 = set()
    for match in tqdm(matches):
        di = {}
        drop_col = []
        for key, value in match.items():
            if key in s:
                drop_col.append(key)
                percent = amount = total = None
                try:
                    #
                    li = value.split("%")
                    percent = li[0].strip()
                    li = li[1].strip().split("/")
                    amount = li[0].strip().split("(")[1].strip()
                    total = li[1].strip().split(")")[0].strip()
                except:
                    # 44% (37/84)
                    #'21/33 (64%)'
                    li = value.split("/")
                    amount = li[0].strip()
                    li = li[1].strip().split(" ")
                    total = li[0].strip()
                    percent = li[1].strip()[1:-2]

                amount = int(amount)
                total = int(total)
                percent = int(percent)
                #98% (94/96)
                if amount > total:
                    s1.add(match["parent_url"])
                    tmp = amount
                    amount = total
                    total = tmp
                    percent = int(amount / total * 100)

                di[key+"_amount"] = amount
                di[key+"_total"] = total
                di[key+"_percent"] = percent


        for col in drop_col:
            match.pop(col)
        match.update(di)
        mdb.add_entry_dict(match, "MatchData1")

    if save:
        mdb.add_entry(
            Statistic(
                explanation=f"All of these the amount is bigger than the total, which is a statistical error from ATP.",
            ),
            metadata={"links" : list(s1)}
        )
