import pandas as pd

from app.databases.mongo_database.mongo_database import MongoDBDatabase
from tqdm import tqdm

def save_collection_to_csv(
        collection_name:str
):
    mdb = MongoDBDatabase("rel_deep")
    data = mdb.get_entries_dict(collection_name)
    df = pd.DataFrame(data)

    df.drop(
        columns=["id"],
        inplace=True,
    )

    nan_counts = df.isna().sum()

    print(nan_counts)
    df.to_csv("/home/nnikolovskii/dev/dev/kg_llm_fusion/data/"+collection_name.lower()+".csv", index=False)

for collection in tqdm(["MatchStats", "Matches", "Players", "Rank", "Sets", "Tournaments"]):
    save_collection_to_csv(collection)

# mdb = MongoDBDatabase("rel_deep")
# for col in ["Net points won", "Winners", "Unforced Errors", "Max Speed", "1st Serve Average Speed", "2nd Serve Average Speed"]:
#     mdb.delete_column("MatchStats", col)