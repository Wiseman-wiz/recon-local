import pymongo

class DeletePending:
    def __init__(self, start_date, end_date):
        client = pymongo.MongoClient("localhost:27017")
        self.db = client["BDC_bank_reco"]
        self.gl_col = self.db["general_ledger"]
        self.bs_col = self.db["bank_statement"]
        self.start_date = start_date
        self.end_date = end_date

    def delete_single_pending_bs(self, _id):
        self.bs_col.delete_one({"_id" : _id})
        return {
            "msg" : "successfully deleted"
        }

    def delete_single_pending_gl(self, _id):
        self.gl_col.delete_one({"_id" : _id})
        return {
            "msg" : "successfully deleted"
        }
    
    def delete_many_pending_bs(self):
        self.bs_col.delete_many({"approved" : "pending", "trndate": {"$gte": self.start_date, "$lt": self.end_date}})
        return {
            "msg" : "successfully deleted"
        }
    
    def delete_many_pending_gl(self):
        self.gl_col.delete_many({"approved" : "pending", "trndate": {"$gte": self.start_date, "$lt": self.end_date}})
         return {
            "msg" : "successfully deleted"
        }