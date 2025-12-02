from pymongo import MongoClient

uri = "mongodb+srv://dw_user:dw_user2025%24@cluster1.svtrdu2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1"
client = MongoClient(uri)
print("✅ Connected!" if client.admin.command("ping") else "❌ Failed")
