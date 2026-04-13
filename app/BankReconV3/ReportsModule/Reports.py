import pymongo
from bson import ObjectId
from datetime import datetime

class Report:
    def __init__(self, db_name:str="BDC_bank_recon_v3", collection:str="report_settings"):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self.conn = self.db[collection]
        self.default_conn = self.db["default_report_settings"]
        self.conn_gl = self.db["general_ledger"]
        self.conn_bs = self.db["bank_statement"]

    
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

    def set_strict_matches(self, strict_matches):
        {
            "bank_account":"",
            "from":"",
            "to":"",
            "criteria":""
        }
    
    def get_gl_fields(self,account):
        conn = self.db["general_ledger"]
        record = dict(conn.find_one({"subacct":account}))
        record = conn.find_one()
        return list(record.keys())
    
    def get_bs_fields(self,account):
        conn = self.db["bank_statement"]
        record = dict(conn.find_one({"account_number":account}))
        record = conn.find_one()
        return list(record.keys())
    
    def get_sample_gl(self,account):
        conn = self.db["general_ledger"]
        record = dict(conn.find_one({"subacct":account}))
        record = self.convert_object_id_to_string(record)
        return record

    def get_sample_bs(self,account):
        conn = self.db["bank_statement"]
        record = dict(conn.find_one({"account_number":account}))
        record = self.convert_object_id_to_string(record)
        return record
    
    def get_daterange_gl(self,account):
        query = [
            {
                "$match": {
                "subacct": account 
                }
            },{
                "$group": {
                "_id": None,
                "minDate": { "$min": "$trndate" }
                }
            }
        ]
        min_date = list(self.conn_gl.aggregate(query))[0]["minDate"]
        query = [
            {
                "$match": {
                "subacct": account
                }
            },{
                "$group": {
                "_id": None,
                "maxDate": { "$max": "$trndate" }
                }
            }
        ]
        max_date = list(self.conn_gl.aggregate(query))[0]["maxDate"]
        return {
            "min_date":str(min_date),
            "max_date":str(max_date)
        }

        
    def get_daterange_bs(self,account):
        query = [
            {
                "$match": {
                "account_number": account 
                }
            },{
                "$group": {
                "_id": None,
                "minDate": { "$min": "$transaction_date" }
                }
            }
        ]
        min_date = list(self.conn_bs.aggregate(query))[0]["minDate"]
        query = [
            {
                "$match": {
                "account_number": account
                }
            },{
                "$group": {
                "_id": None,
                "maxDate": { "$max": "$transaction_date" }
                }
            }
        ]
        max_date = list(self.conn_bs.aggregate(query))[0]["maxDate"]
        return {
            "min_date":str(min_date),
            "max_date":str(max_date)
        }
    
    def create_report(self,report_name,accounts,start_date,end_date,account_number,subaccount):
        conn = self.db["test_reports2"]
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        # Convert the datetime object to a formatted string in ISO 8601 format
        start_date = start_date.isoformat()
        end_date = end_date.isoformat()
        record = {
            "report_name":report_name,
            "start_date":start_date,
            "end_date":end_date,
            "account_number":account_number,
            "subaccount":subaccount,
            "accounts":accounts,
            "filters":[],
            "strict_matches":[]
        }
        if conn.find_one({"report_name":report_name}):
            return (False,"Report name already exists")
        conn.insert_one(record)
        return(True,"Report created successfully")

    def get_reports_list(self,account):
        conn = self.db["test_reports2"]
        records = list(conn.find({"account_number":account}))
        final_record = []
        final_headers= []
        if records:
            for record in records:
                record = self.convert_object_id_to_string(record)
                record["id"] = record["_id"]
                del(record["_id"])
                final_record.append(record)
            final_headers = list(final_record[0].keys())
            return (True,[{"data":final_record,"headers":final_headers}])
        else:
            return (False,"No records found")
