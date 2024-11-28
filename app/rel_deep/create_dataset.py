import pandas as pd
from relbench.base import Database, Dataset, Table
from app.databases.mongo_database.mongo_database import MongoDBDatabase

class TennisATPDataset(Dataset):
    val_timestamp = pd.Timestamp("2005-01-01")
    test_timestamp = pd.Timestamp("2010-01-01")

    def make_db(self) -> Database:
        mdb = MongoDBDatabase("rel_deep")
        players_data = mdb.get_entries_dict("Players")
        players = pd.DataFrame(players_data)

        tournaments_data = mdb.get_entries_dict("Tournaments")
        tournaments = pd.DataFrame(tournaments_data)

        rankings_data = mdb.get_entries_dict("Rank")
        rankings = pd.DataFrame(rankings_data)

        matches_data = mdb.get_entries_dict("Matches")
        matches = pd.DataFrame(matches_data)

        match_stats_data = mdb.get_entries_dict("MatchStats")
        match_stats = pd.DataFrame(match_stats_data)

        sets_data = mdb.get_entries_dict("Sets")
        sets = pd.DataFrame(sets_data)

        print("Players DataFrame Data Types:")
        print(players.dtypes)
        print("\nTournaments DataFrame Data Types:")
        print(tournaments.dtypes)
        print("\nRankings DataFrame Data Types:")
        print(rankings.dtypes)
        print("\nMatches DataFrame Data Types:")
        print(matches.dtypes)
        print("\nMatch Stats DataFrame Data Types:")
        print(match_stats.dtypes)
        print("\nSets DataFrame Data Types:")
        print(sets.dtypes)

        players.drop(
            columns=["id"],
            inplace=True,
        )

        tournaments.drop(
            columns=["id"],
            inplace=True,
        )

        rankings.drop(
            columns=["id"],
            inplace=True,
        )

        matches.drop(
            columns=["id"],
            inplace=True,
        )

        match_stats.drop(
            columns=["id"],
            inplace=True,
        )

        sets.drop(
            columns=["id"],
            inplace=True,
        )

        tables = {}

        tables["tournaments"] = Table(
            df=pd.DataFrame(tournaments),
            fkey_col_to_pkey_table={},
            pkey_col="tournament",
        )

        tables["players"] = Table(
            df=pd.DataFrame(players),
            fkey_col_to_pkey_table={},
            pkey_col="player",
        )

        tables["rankings"] = Table(
            df=pd.DataFrame(rankings),
            fkey_col_to_pkey_table={
                "player": "players",
            },
            time_col="date",
        )

        tables["matches"] = Table(
            df=pd.DataFrame(matches),
            fkey_col_to_pkey_table={
                "tournament": "tournaments",
            },
            pkey_col="match",
            time_col="date",
        )

        tables["match_stats"] = Table(
            df=pd.DataFrame(match_stats),
            fkey_col_to_pkey_table={
                "player": "players",
                "match": "matches",
            },
            time_col="date",
        )

        tables["sets"] = Table(
            df=pd.DataFrame(sets),
            fkey_col_to_pkey_table={
                "match": "matches",
                "player": "players",
            },
            time_col="date",
        )

        return Database(tables)


tennis_dataset = TennisATPDataset(cache_dir="./cache/at1")
print(tennis_dataset.make_db())
