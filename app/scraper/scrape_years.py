from app.databases.mongo_database.mongo_database import MongoDBDatabase
from app.utils.subject_scraper import Link


def get_years():
    mdb = MongoDBDatabase()
    for i in range(2000, 2025):
        mdb.add_entry(Link(
            url=f"https://www.atptour.com/en/scores/results-archive?year={i}",
            link_type="tennis_years"
        ))
    print("Finish with years")

get_years()

