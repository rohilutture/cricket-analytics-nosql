"""
Neo4j loader (Python): Creates Player, Team, Match nodes and FACED relationships from Mongo deliveries.
Usage:
  python neo4j_loader.py --limit 200000   # optional limit to test
Requires env: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, MONGODB_URI, MONGO_DB
"""

import os
import argparse
from neo4j import GraphDatabase
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_clients():
    m_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    m_db = os.getenv("MONGO_DB", "cricket_iq")
    n_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    n_user = os.getenv("NEO4J_USER", "neo4j")
    n_pass = os.getenv("NEO4J_PASSWORD", "password")
    mclient = MongoClient(m_uri)[m_db]
    ndriver = GraphDatabase.driver(n_uri, auth=(n_user, n_pass))
    return mclient, ndriver

def ensure_schema(session):
    # Idempotent constraints
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Player) REQUIRE p.name IS UNIQUE;")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Team) REQUIRE t.name IS UNIQUE;")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Match) REQUIRE m.id IS UNIQUE;")

def seed_matches(session, matches):
    if not matches:
        return
    cy = """
    UNWIND $rows AS row
    MERGE (m:Match {id: row.id})
      ON CREATE SET m.date = row.date, m.type = row.type, m.gender = row.gender, m.winner = row.winner
      ON MATCH  SET m.date = coalesce(m.date, row.date),
                  m.type = coalesce(m.type, row.type),
                  m.gender = coalesce(m.gender, row.gender),
                  m.winner = coalesce(m.winner, row.winner)
    WITH m, row
    UNWIND row.teams AS t
    MERGE (tm:Team {name: t})
    MERGE (tm)-[:PLAYED {matchId: m.id}]->(m);
    """
    session.run(cy, rows=matches)

def load_facings(session, batch):
    if not batch:
        return
    cy = """
    UNWIND $rows AS r
    WITH r
    WHERE r.batter IS NOT NULL AND r.bowler IS NOT NULL
      AND r.matchId IS NOT NULL AND r.over IS NOT NULL
    MERGE (bat:Player {name: r.batter})
    MERGE (bow:Player {name: r.bowler})
    MERGE (bat)-[f:FACED {
        matchId: r.matchId,
        innings: r.innings,
        over: r.over,
        ball: coalesce(r.ball, -1)    // ensure non-null for MERGE key
    }]->(bow)
    SET f.runs = coalesce(r.runs_total, 0),
        f.isWicket = coalesce(r.is_wicket, 0),
        f.team = r.battingTeam;
    """
    session.run(cy, rows=batch)

def main(limit=None, batch_size=5000):
    mdb, driver = get_clients()
    deliveries = mdb.deliveries
    matches = mdb.matches

    with driver.session() as session:
        ensure_schema(session)

        # Seed matches/teams in chunks
        rows = []
        for m in matches.find({}, {"_id": 1, "info.dates": 1, "info.match_type": 1, "info.gender": 1, "info.teams":1, "outcome.winner":1}):
            rows.append({
                "id": m["_id"],
                "date": (m.get("info", {}).get("dates") or [None])[0],
                "type": m.get("info", {}).get("match_type"),
                "gender": m.get("info", {}).get("gender"),
                "teams": m.get("info", {}).get("teams", []),
                "winner": (m.get("outcome") or {}).get("winner")
            })
            if len(rows) >= 2000:
                seed_matches(session, rows); rows = []
        if rows: seed_matches(session, rows)

        # FACED relationships from deliveries (validate & normalize)
        cursor = deliveries.find({}, {
            "matchId":1,"innings":1,"battingTeam":1,"over":1,"ball":1,
            "batter":1,"bowler":1,"runs_total":1,"wickets":1
        })

        count = 0
        batch = []
        for d in cursor:
            # hard validation
            batter = d.get("batter")
            bowler = d.get("bowler")
            matchId = d.get("matchId")
            over = d.get("over")
            if not batter or not bowler or matchId is None or over is None:
                continue

            # normalize ball & wicket flag
            ball = d.get("ball")
            if ball is None:
                ball = -1
            is_wicket = 1 if d.get("wickets") else 0

            batch.append({
                "matchId": matchId,
                "innings": d.get("innings"),
                "battingTeam": d.get("battingTeam"),
                "over": over,
                "ball": ball,
                "batter": batter,
                "bowler": bowler,
                "runs_total": d.get("runs_total", 0),
                "is_wicket": is_wicket
            })

            if len(batch) >= batch_size:
                load_facings(session, batch); batch = []

            count += 1
            if limit and count >= limit:
                break

        if batch:
            load_facings(session, batch)

    driver.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="Max deliveries to load into Neo4j")
    args = ap.parse_args()
    main(limit=args.limit)
