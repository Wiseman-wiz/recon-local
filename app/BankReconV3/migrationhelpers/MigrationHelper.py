import pymongo
from bson import ObjectId
from datetime import datetime


class MigrationHelper:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["bank_recon_v3"]
        
        pass

    def get_gl_schema(self) -> dict:
        return {
            "_id": ObjectId,
            "approved": bool,
            "cr_amt": float,
            "date_modified": datetime,
            "dr_amt": float,
            "is_matched": bool,
            "other_01":str,
            "other_03":str,
            "ref_1":str,
            "subacct":str,
            "trndate":datetime,
            "trnno":str,
            "from":str,
            "ref_2":str,
            "to":str,
            "net":str
        }

    def get_bs_schema(self) -> dict:
        return {
            "_id":ObjectId,
            "account_number":str,
            "approved":bool,
            "credit_amount":float,
            "date_modified": datetime,
            "debit_amount":float,
            "is_matched":bool,
            "transaction_date":datetime,
            "transaction_description":str,
            "transaction_reference":str,
            "to":str,
            "net":str,
            "check_number":str,
            "from":str
        }
    
    def validate_keys(self,database_name:str,collection_name:str,schema:dict,schema_name:str) -> bool:
        self.db = self.client[database_name]
        self.conn = self.db[collection_name]

        no_schema = 0
        diff_keys = []
        for record in self.conn.find():
            record_keys = record.keys()
            for key in record_keys:
                schema_keys = schema.keys()
                if key not in schema_keys:
                    no_schema += 1
                    diff_keys.append(key)
        diff_keys = set(diff_keys)
        return(no_schema,schema_name,diff_keys)

    def validate_types(self,database_name:str,collection_name:str,schema:dict):
        self.db = self.client[database_name]
        self.conn = self.db[collection_name]
        print("document_count",self.conn.count_documents({}))

        issue_keys = []
        for record in self.conn.find({}):
            for k,v in record.items():
                if k == "_id":
                    continue
                if v == "":
                    issue_keys.append(k)
                if isinstance(v, schema[k]):
                    continue
                else:
                    issue_keys.append(k)
        return(set(issue_keys))        

    def test_migrate(self,database_name:str,collection_name:str,corrections:dict):
        self.db = self.client[database_name]
        self.conn = self.db[collection_name]
        print("document_count",self.conn.count_documents({}))
        for record in self.conn.find({},{k:1 for k in corrections.keys()}):
            for k,v in record.items():
                if k == "_id":
                    continue
                if v == "":
                    record[k] = corrections[k]["default"]
                if isinstance(v, corrections[k]["type"]):
                    continue
                else:
                    record[k] = corrections[k]["type"](v)
        return True
    
    def migrate(self,database_name:str,migration_database:str,collection_name:str,corrections:dict):
        self.db = self.client[database_name]
        self.conn = self.db[collection_name]
        
        self.db_migrate = self.client[migration_database]
        self.conn_migrate = self.db_migrate[collection_name]
        
        print("document_count",self.conn.count_documents({}))
        for record in self.conn.find({},{"_id":0}):
            for k,v in record.items():
                if k in corrections.keys():
                    if v == "":
                        record[k] = corrections[k]["default"]
                    if isinstance(v, corrections[k]["type"]):
                        continue
                    else:
                        record[k] = corrections[k]["type"](v)
            self.conn_migrate.insert_one(record)
        return self.conn_migrate.count_documents({})
    
    def deduplicate_records(self,database_name:str,collection_name:str,duplicate_criteria:str):
        self.db = self.client[database_name]
        self.conn = self.db[collection_name]

        for record in self.conn.find({}):
            record_keys = record.keys()

if __name__ == "__main__":
    BRR = MigrationHelper()
    gl_schema = BRR.get_gl_schema()
    bs_schema = BRR.get_bs_schema()

    validate_gl = BRR.validate_keys("BDC_bank_recon","general_ledger",gl_schema,"gl_schema")
    print("validate_gl",validate_gl)
    
    validate_bs = BRR.validate_keys("BDC_bank_recon","bank_statement",bs_schema,"bs_schema")
    print("validate_bs",validate_bs)

    validate_types_gl = BRR.validate_types("BDC_bank_recon","general_ledger",gl_schema,)
    print("migrate_gl",validate_types_gl)
    validate_types_bs = BRR.validate_types("BDC_bank_recon","bank_statement",bs_schema,)
    print("migrate_bs",validate_types_bs)

    gl_corrections = {
        "approved":{
            "type":bool,
            "default":False
        },
        "other_03":{
            "type":str,
            "default":""
        },
        "other_01":{
            "type":str,
            "default":""
        },
        "ref_1":{
            "type":str,
            "default":""
        },
        "is_matched":{
            "type":bool,
            "default":False
        }
    }

    bs_corrections = {
        "approved":{
            "type":bool,
            "default":False
        },
        "transaction_reference":{
            "type":str,
            "default":""
        },
        "transaction_description":{
            "type":str,
            "default":""
        },
        "check_number":{
            "type":str,
            "default":""
        },
        "is_matched":{
            "type":bool,
            "default":False
        }
    }
    
    test_migrate_gl = BRR.test_migrate("BDC_bank_recon","general_ledger",gl_corrections)
    print("test_migrate_gl",test_migrate_gl)

    test_migrate_bs = BRR.test_migrate("BDC_bank_recon","bank_statement",bs_corrections)
    print("test_migrate_bs",test_migrate_bs)

    migrate_gl = BRR.migrate("BDC_bank_recon","BDC_bank_recon_v3","general_ledger",gl_corrections)
    print("test_migrate_gl",migrate_gl)

    migrate_bs = BRR.migrate("BDC_bank_recon","BDC_bank_recon_v3","bank_statement",bs_corrections)
    print("test_migrate_bs",migrate_bs)


    

