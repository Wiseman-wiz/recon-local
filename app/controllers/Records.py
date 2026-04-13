import re
from bson.objectid import ObjectId
from django.contrib import messages
from pprint import pprint
from datetime import datetime
from . import (
    DbOps,
    BankRecon as br
)
db_mo = DbOps.MainOps()

# client = DbOps.MainOps()
# db = client.ref_db("bank_recon")


def create_record(request, db):
    try:
        list_collections = list(br.get_collections(db))
            
        record_name = re.sub('[^A-Za-z0-9_ ]+', '', request.POST.get("record_name"))
        clean_record_name = " ".join(record_name.lower().split()).replace(' ','_')
            
        if clean_record_name in list_collections:
            messages.error(request, 'Record is already existing.')
        else:
            result = db.create_collection(clean_record_name)
            if (result):
                upload_list =  {
                    "record_name": clean_record_name,
                    "date_modified": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                }
                db["upload_list"].replace_one({"record_name":f"{clean_record_name}"}, upload_list, upsert=True)
                messages.success(request, 'New Record was successfully added.')
                
                trail = db_mo.connect("trail","trail_recon")
                trail.insert_one({
                    "user":str(request.user),
                    "action":"create_record",
                    "action_type":"create",
                    "company":str(request.session["company_code"]),
                    "time":datetime.now(),
                    "actions":[record_name]
                })
    except Exception as e:
        messages.error(request, 'Creating New Record Failed')
        pprint(repr(e))
        return {"error":"Creating New Record Failed " + repr(e)}


def del_record(request, db):
    record_name = request.POST.get("record").lower().replace(' ', '_')
    
    if db["upload_list"].find({"record_name":record_name}).count() > 0:
        pprint(record_name)
        upload_list = db["upload_list"].find({"record_name":record_name})
        for field in upload_list:
            if "reports" in field:
                for report in field["reports"]:
                    try:
                        db[f"summary_reports_{report['report_name']}"].drop()
                        db[f"reports_{report['report_name']}"].drop()
                        db["reports"].delete_one({"report_name":report["report_name"]})
                            
                        db["upload_list"].update(
                            {"record_name":report["report_matched"]},
                            {"$pull": {
                                    "reports": { 
                                        "report_name":report["report_name"], 
                                        "report_matched": record_name
                                    }
                                }
                            }
                        )
                            
                        db["upload_list"].update(
                            {"record_name":record_name},
                            {"$pull": {
                                    "reports": { 
                                        "report_name":report["report_name"], 
                                        "report_matched": report["report_matched"]
                                    }
                                }
                            }
                        )
                        trail = db_mo.connect("trail","trail_recon")
                        trail.insert_one({
                            "user":str(request.user),
                            "action":"del_record",
                            "action_type":"delete",
                            "company":str(request.session["company_code"]),
                            "time":datetime.now(),
                            "actions":[record_name]
                        })
                    except Exception as e:
                        messages.error(request, f"Deleting report failed. {e}")
                        return False
            
        try:
            
            result = db["upload_list"].delete_one({"record_name":record_name})
            target_collection = db[f"{record_name}"]
            target_collection.drop()
        except Exception as e:
            messages.error(request, f"Deleting report failed. {e}")
            return False

        return True
    else: 
        return False

def delete_records(request, db, collection):
    try:
        str_ids = request.POST.getlist("report_id")
        if not str_ids:
            messages.error(request, 'Please select which to delete first and try again.')
            return False
        collection = collection.replace(' ','_')
        obj_ids = [ObjectId(str_id) for str_id in str_ids]
        docs_to_delete = list(db[collection].find({"_id": {"$in": obj_ids}}))
        if docs_to_delete:
            deleted_docs = {
                "src_collection": collection,
                "deleted_documents": docs_to_delete,
                "remarks": request.POST.get("remarks"),
                "user": str(request.user),
                "deleted_at": datetime.now()
            }
            db["deleted_documents"].insert_one(deleted_docs)
            db[collection].delete_many({"_id": { "$in": obj_ids } })
        trail = db_mo.connect("trail","trail_recon")
        trail.insert_one({
            "user":str(request.user),
            "action":"delete_per_record",
            "action_type":"delete",
            "company":str(request.session["company_code"]),
            "time":datetime.now(),
            "actions":[obj_ids]
        })
        return True
    except Exception as e:
        messages.error(request, 'Failed deleting reports.')
        pprint(repr(e))
        return False