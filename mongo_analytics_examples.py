
"""
Example PyMongo analytics pipelines.
Run with your MONGO env set; prints sample results.
"""
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
dbn = os.getenv("MONGO_DB", "cricket_iq")
client = MongoClient(uri)
db = client[dbn]
D = db.deliveries
M = db.matches

def runs_by_batter(limit=10):
    pipe = [
        {"$group": {"_id": "$batter",
                    "runs": {"$sum": "$runs_batter"},
                    "balls": {"$sum": 1},
                    "boundaries": {"$sum": "$is_boundary"}}},
        {"$addFields": {"strikeRate": {"$multiply": [{"$divide": ["$runs", "$balls"]}, 100]},
                        "boundaryPct": {"$multiply": [{"$divide": ["$boundaries", "$balls"]}, 100]}}},
        {"$sort": {"runs": -1}},
        {"$limit": limit}
    ]
    return list(D.aggregate(pipe))

def wickets_by_bowler(limit=10):
    pipe = [
        {"$project": {"bowler": 1, "wkts": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$wickets", []]}}, 0]}, 1, 0]}}},
        {"$group": {"_id": "$bowler", "wickets": {"$sum": "$wkts"}}},
        {"$sort": {"wickets": -1}},
        {"$limit": limit}
    ]
    return list(D.aggregate(pipe))

def kohli_vs_southee():
    pipe = [
        {"$match": {"batter": "V Kohli", "bowler": "TG Southee"}},
        {"$group": {"_id": None,
                    "balls": {"$sum": 1},
                    "runs": {"$sum": "$runs_total"},
                    "outs": {"$sum": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$wickets", []]}}, 0]}, 1, 0]}}}}
    ]
    return list(D.aggregate(pipe))

if __name__ == "__main__":
    print("Top batters:", runs_by_batter(5))
    print("Top bowlers:", wickets_by_bowler(5))
    print("Kohli vs Southee:", kohli_vs_southee())
