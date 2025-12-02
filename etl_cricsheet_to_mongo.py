
"""
ETL: Cricsheet JSON -> MongoDB
- Creates two collections:
  * matches: one doc per match (metadata + compacted outcome info)
  * deliveries: one doc per ball (flattened for analytics)

Run:
  python etl_cricsheet_to_mongo.py --data_dir /path/to/cricsheet/matches
Env:
  - MONGODB_URI
  - MONGO_DB
"""
import os
import json
import argparse
from tqdm import tqdm
from datetime import datetime
from pymongo import MongoClient, InsertOne
from dotenv import load_dotenv

def get_env():
    load_dotenv()
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    dbname = os.getenv("MONGO_DB", "cricket_iq")
    return uri, dbname

def normalize_match(doc):
    """Extract top-level match info; support v1.0.0 and v1.1.0 variations."""
    info = doc.get("info", {})
    match_id = info.get("match_id") or info.get("registry", {}).get("match", None)
    dates = info.get("dates") or []
    date_str = dates[0] if dates else None
    try:
        date = datetime.fromisoformat(date_str) if date_str else None
    except Exception:
        date = None

    outcome = info.get("outcome", {})
    winner = outcome.get("winner")
    result = None
    if "by" in outcome:
        by = outcome["by"]
        if "runs" in by:
            result = f"{winner} won by {by['runs']} runs"
        elif "wickets" in by:
            result = f"{winner} won by {by['wickets']} wickets"
    elif outcome.get("result"):
        result = outcome.get("result")

    return {
        "_id": match_id or info.get("event", {}).get("match_number") or info.get("city", "") + "-" + (date_str or ""),
        "info": {
            "dates": dates,
            "team_type": info.get("team_type"),
            "match_type": info.get("match_type"),
            "gender": info.get("gender"),
            "teams": info.get("teams", []),
            "venue": info.get("venue"),
            "city": info.get("city"),
            "officials": info.get("officials", {}),
        },
        "outcome": {
            "winner": winner,
            "result": result
        },
        "source_version": doc.get("meta", {}).get("data_version")
    }

def iter_deliveries(doc, match_id):
    """Yield flattened deliveries from nested innings/overs/deliveries."""
    innings = doc.get("innings", [])
    for inn in innings:
        inn_no = inn.get("innings") or inn.get("number") or inn.get("team")
        batting_team = inn.get("team")
        for over_obj in inn.get("overs", []):
            over_no = over_obj.get("over")
            for d in over_obj.get("deliveries", []):
                # Support v1.0.0 vs v1.1.0 naming
                batter = d.get("batter") or d.get("striker")
                non_striker = d.get("non_striker") or d.get("nonStriker") or d.get("non_striker")
                bowler = d.get("bowler")
                runs = d.get("runs", {})
                extras = runs.get("extras", 0) if isinstance(runs, dict) else 0
                batter_runs = runs.get("batter", 0) if isinstance(runs, dict) else d.get("runs", 0)
                total_runs = runs.get("total", batter_runs + extras) if isinstance(runs, dict) else batter_runs + extras
                wickets = d.get("wickets") or d.get("wicket") or []
                if isinstance(wickets, dict):
                    wickets = [wickets]
                yield {
                    "matchId": match_id,
                    "innings": inn_no,
                    "battingTeam": batting_team,
                    "over": over_no,
                    "ball": d.get("ball"),
                    "batter": batter,
                    "nonStriker": non_striker,
                    "bowler": bowler,
                    "runs_batter": batter_runs,
                    "runs_extras": extras,
                    "runs_total": total_runs,
                    "wickets": wickets,
                    # Optional handy flags
                    "is_boundary": 1 if batter_runs in (4,6) else 0,
                    "is_dot": 1 if total_runs == 0 else 0
                }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", required=True, help="Path to folder containing Cricsheet JSON match files")
    parser.add_argument("--batch", type=int, default=2000, help="Insert batch size")
    args = parser.parse_args()

    uri, dbname = get_env()
    client = MongoClient(uri)
    db = client[dbname]
    matches = db.matches
    deliveries = db.deliveries

    # Indexes (idempotent)
    matches.create_index([("info.dates", 1)])
    matches.create_index([("info.match_type", 1), ("info.team_type", 1)])
    deliveries.create_index([("matchId", 1), ("innings", 1), ("over", 1)])
    deliveries.create_index([("batter", 1)])
    deliveries.create_index([("bowler", 1)])
    deliveries.create_index([("battingTeam", 1)])

    files = [os.path.join(args.data_dir, f) for f in os.listdir(args.data_dir) if f.endswith(".json")]
    files.sort()

    for fp in tqdm(files, desc="Processing matches"):
        try:
            with open(fp, "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            m = normalize_match(doc)
            matches.replace_one({"_id": m["_id"]}, m, upsert=True)

            batch = []
            for row in iter_deliveries(doc, m["_id"]):
                batch.append(InsertOne(row))
                if len(batch) >= args.batch:
                    deliveries.bulk_write(batch, ordered=False)
                    batch.clear()
            if batch:
                deliveries.bulk_write(batch, ordered=False)
        except Exception as e:
            print(f"Error on {fp}: {e}")

if __name__ == "__main__":
    main()
