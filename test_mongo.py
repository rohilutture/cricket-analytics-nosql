import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
if not uri:
    print("❌ MONGODB_URI not set")
    exit(1)

try:
    client = MongoClient(uri)
    # The ismaster command is cheap and does not require auth.
    client.admin.command('ismaster')
    print("✅ Connected!")
except Exception as e:
    print(f"❌ Failed: {e}")
