import pymongo
from bson import ObjectId


class BankAccount:
    def __init__(self, db_name:str="BDC_bank_recon", collection:str="bank_accounts"):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self.conn = self.db[collection]

    

    def convert_object_id_to_string(self,data):
        if isinstance(data, dict):
            for key, value in data.items():
                data[key] = self.convert_object_id_to_string(value)
        elif isinstance(data, list):
            for i in range(len(data)):
                data[i] = self.convert_object_id_to_string(data[i])
        elif isinstance(data, ObjectId):
            return str(data)
        return data
    
    def get_all(self,filters=None):
        accounts = []
        accounts_cur = self.conn.find({},{ftr:1 for ftr in filters}) if filters else self.conn.find({})
        for account in accounts_cur:
            accounts.append(self.convert_object_id_to_string(account))
            account["id"]=account["_id"]
            del(account["_id"])
        return accounts

    def get_one(self,account_id):
        """
        _id:646f15b083ffb022145ec0cc
        subaccount:1001-1082
        account_number:44774475135162
        account_holder:BDC
        bank_name:MBTC
        bank_branch:STA MONICA
        account_type_1:Collection
        account_type_2:Main
        beginning_balance:0.0
        """
        account = self.conn.find_one({"_id":ObjectId(account_id)})
        account = self.convert_object_id_to_string(account)
        return account
    

if __name__ == '__main__':
    test_accounts = BankAccount('BDC_bank_recon', 'bank_accounts').get_all()
    print(test_accounts)