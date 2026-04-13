import re
from bson.objectid import ObjectId
from django.contrib import messages
from pprint import pprint
from . import DbOps, BankRecon as br

# client = DbOps.MainOps()
# db = client.ref_db("bank_recon")


#  dont change
def set_approval(request, collection, db):
    try:
        ids = request.POST.getlist("report_id")
        print(ids)
        if not ids:
            messages.error(request, 'Please select which to approve first and try again.')
            return False

        if "pending" in request.POST.get("btn_approval"):
            for id in ids:
                db[collection.replace('-','_')].update({"_id":ObjectId(id)}, {"$set": {"approved":"pending"}})
        elif "approve" in request.POST.get("btn_approval"):
            for id in ids:        
                db[collection.replace('-','_')].update({"_id":ObjectId(id)}, {"$set": {"approved":True}})
        elif "reject" in request.POST.get("btn_approval"):
            for id in ids:
                db[collection.replace('-','_')].update({"_id":ObjectId(id)}, {"$set": {"approved":False}})
                
        return True
    except Exception as e:
        messages.error(request, 'Failed updating report status.')
        pprint(repr(e))
        return False
