import pandas as pd

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from tqdm import tqdm

def move_tournaments(
        collection_name,
        new_name,
        columns=[],
):
    old_database = MongoDBDatabase()
    new_database = MongoDBDatabase("rel_deep")

    for column in columns:
        old_database.delete_column(collection_name, column)
    entities = old_database.get_entries_dict(collection_name)

    for dict in tqdm(entities):
        new_database.add_entry_dict(dict, collection_name=new_name)

def remove_sets():
    mdb = MongoDBDatabase()
    sets = mdb.get_entries_dict("Sets")
    existing_matches = {elem["match"] for elem in mdb.get_entries_dict("Matches")}
    count = 0
    for set_elem in sets:
        match = set_elem["match"]
        if match not in existing_matches:
            mdb.delete_entity_dict(set_elem, "Sets")

    print(count/len(sets)*100)

def add_timestamp():
    mdb = MongoDBDatabase("rel_deep")
    match_dict = {elem["match"]: elem["date"] for elem in mdb.get_entries_dict("Matches")}

    match_stats = mdb.get_entries_dict("Sets")
    for stat in tqdm(match_stats):
        date = match_dict[stat["match"]]
        stat["date"] = date
        mdb.update_entity_dict(stat, "Sets")

mdb = MongoDBDatabase("rel_deep")
mdb.delete_column("MatchStats", "id")