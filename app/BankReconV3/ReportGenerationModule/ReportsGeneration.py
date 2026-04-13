import pymongo
from bson import ObjectId
from datetime import datetime

class ReportGeneration:
    def __init__(self,report_id) -> None:
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["BDC_bank_recon_v3"]
        self.db2 = self.client["BDC_bank_recon"]
        self.conn = self.db["test_reports2"]
        self.report_id = report_id
        self.pb_singular = []
        self.gl_singular = []
        pass
    
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
    
    def get_initial_report_data(self):
        data = self.conn.find_one({"_id":ObjectId(self.report_id)})
        data = self.convert_object_id_to_string(data)
        data["id"] = data["_id"]
        del(data["_id"])

        return {
            "headers":list(data.keys()),
            "data":data
        }
    
    def filter_by_date(self,data, start_date, end_date,key):
        filtered_data = []
        for item in data:
            trndate = item[key]
            if start_date <= trndate <= end_date:
                filtered_data.append(item)
        return filtered_data
    

    def get_unique_duplicate(self,key,list_dict):
        duplicate_keys = []
        duplicates = []
        uniques = []
        for record in list_dict:
            record_key = record[key]
            if record_key in duplicate_keys:
                duplicate_keys.append(record_key)

        for record in list_dict:
            record_key = record[key]
            if record_key in duplicate_keys:
                duplicates.append(record)
            else:
                uniques.append(record)
        return(uniques,duplicates)

        

    def one_to_one_matches(self):
        # Get Initial Report Data
        report_data = self.get_initial_report_data().get("data")
        subacct = report_data["subaccount"]
        account_number = report_data["account_number"]

        startdate = report_data["start_date"]
        startdate = datetime.fromisoformat(startdate)

        enddate = report_data["end_date"]
        enddate = datetime.fromisoformat(enddate)


        # Get Bank Records
        gl_records = self.db.general_ledger.find({
            "subacct":subacct,
        })
        pb_records =self.db.bank_statement.find({
            "account_number":account_number,
        })

        # Clean Bank Records
        cleaned_gl = []
        cleaned_pb = []
        for d in gl_records:
            if isinstance(d["trndate"], str):
                d["trndate"] = datetime.fromisoformat(d["trndate"])
            d["id"] = str(d["_id"])
            del(d["_id"])
            cleaned_gl.append(d)
        
        for d in pb_records:
            if isinstance(d["transaction_date"], str):
                d["transaction_date"] = datetime.fromisoformat(d["transaction_date"])
            d["id"] = str(d["_id"])
            del(d["_id"])
            cleaned_pb.append(d)


        # Get date filters
        gl_filtered = self.filter_by_date(cleaned_gl, startdate, enddate,"trndate")
        bs_filtered = self.filter_by_date(cleaned_pb, startdate, enddate,"transaction_date")
        
        gl_filtered = self.convert_object_id_to_string(gl_filtered)
        bs_filtered = self.convert_object_id_to_string(bs_filtered)


        # Get Unique and Duplicate Records
        data = self.get_unique_duplicate("other_03",gl_filtered)
        unique_gl_filtered = data[0]
        duplicate_gl_filtered = data[1]
        
        data = self.get_unique_duplicate("transaction_reference",bs_filtered)
        unique_bs_filtered = data[0]
        duplicate_bs_filtered = data[1]

        matches_one_to_one = []
        matched_ids = [] # to avoid matching the same record twice

        for gl_record in unique_gl_filtered:
            for bs_record in unique_bs_filtered:
                if gl_record["other_03"] == bs_record["transaction_reference"] and gl_record["cr_amt"] == bs_record["debit_amount"] and gl_record["dr_amt"] == bs_record["credit_amount"]:
                    matches_one_to_one.append({
                        "gl_record":gl_record,
                        "bs_record":bs_record})
                    matched_ids.append(gl_record["id"])                
                    matched_ids.append(bs_record["id"])    
        # end of story for one to one
        # remaining is one to many 
        # remaining is many to many

        # Get unmatched records FROM UNIQUE GL RECORDS
        single_unmatched_gl = []
        single_unmatched_pb = []
        for bs_record in unique_gl_filtered:
            if bs_record["id"] not in matched_ids:
                single_unmatched_gl.append(bs_record)

        for gl_record in unique_bs_filtered:
            if gl_record["id"] not in matched_ids:
                single_unmatched_pb.append(gl_record)



        # Merge data_gl
        null_gl_other_3 = []
        merged_data_gl = {}
        for record in duplicate_gl_filtered:
            other_03 = record['other_03']
            if other_03:
                merged_data_gl[other_03]['dr_amt'] += record['dr_amt']
                merged_data_gl[other_03]['cr_amt'] += record['cr_amt']
                merged_data_gl[other_03]["records"] = []
            else:
                null_gl_other_3.append(record)
                pass

        #bs
        null_bs_transaction_reference = []
        merged_data_bs = {}
        for record in duplicate_gl_filtered:
            if record["other_03"] in merged_data_gl.keys():
                merged_data_gl["records"].append(record)

        # merged_data_bs, merged_data_gl, null_gl_other_3, null_bs_transaction_reference,single_unmatched_gl,single_unmatched_pb


        single_matched_gl = []
        merged_matched_bs = []
        matched_one_to_many = []
        for single_gl in single_unmatched_gl:
            for merged in merged_data_bs:
                if single_gl["other_03"] == merged["transaction_reference"] and single_gl["cr_amt"] == merged["debit_amount"] and single_gl["dr_amt"] == merged["credit_amount"]:
                    matched_one_to_many.append({
                        "gl_record":single_gl,
                        "bs_record":merged})
                    single_matched_gl.append(single_gl["id"])
                    merged_matched_bs.append(merged["id"])

        # do the same on above but invert the gl and bs

        for single_gl in single_unmatched_gl:
            if single_gl["id"] not in single_matched_gl:
                single_unmatched_gl.append(single_gl)
        
        for merged in merged_data_bs:
            if merged["id"] not in merged_matched_bs:
                merged_matched_bs.append(merged)

        # do the same on above but invert the gl and bs
        
        # matched_one_to_many DONE
        # 
        # Many to Many




        # Merge Data
        #unmatched_pb single 
        #unmatched_gl single
        {"record":"record"}        
        

        print("matches",len(matches_one_to_one))
        print("gl_filtered",len(gl_filtered))
        print("bs_filtered",len(bs_filtered))
        print("unmatched_gl",len(single_unmatched_gl))
        print("unmatched_bs",len(single_unmatched_pb))
        return {
            "matches_one_to_one":matches_one_to_one,
            "matches_one_to_many":matches_one_to_one,
            "matches_many_to_many":matches_one_to_one,
            "unmatched_gl":single_unmatched_gl,
            "unmatched_pb":single_unmatched_pb
        }
        
        """isolate records based on the data provided"""

    def set_strict_matches(self):
        """set strict matches"""

    def set_filers(self):
        """sets filters"""
