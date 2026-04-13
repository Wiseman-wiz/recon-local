import pymongo


client = pymongo.MongoClient("localhost:27017")
db = client["BDC_bank_recon"]


col = db["bank_statement"]

main_data = col.find()
distinct_accounts = col.distinct("account_number")

print(distinct_accounts)
"""
for account in distinct_accounts:
     data = col.find({"account_number":account},{"_id":0})
     for d in data:  
          target_col = db[f"bank_statement_{account}"]
          target_col.delete_many({})
"""


          
data = db.list_collection_names()

data2 = list(x for x in data if "_pb_" in x)
data2 = ["bank_statement"]
print(data2)

main_accounts = []
for collection in data2:
    distinct_accounts = db[collection].distinct("account_number")
    for account in distinct_accounts:
        data = col.find({"account_number":account},{"_id":0})
        for d in data:  
            target_col = db[f"bank_statement_{account}"]
            target_col.insert(d)
        print(account)


'''
col = db["1001059_bdc_pb_janjuly"]
account = "7-58963897-7"
target_col = db[f"bank_statement_{account}"]

target_col.delete_many({})
data = col.find({"account_number":account},{"_id":0})
for d in data:  
    target_col = db[f"bank_statement_{account}"]
    target_col.insert(d)
print(account)
'''