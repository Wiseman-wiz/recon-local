import re
from bson.objectid import ObjectId
from django.contrib import messages
from pprint import pprint
# from . import DbOps

# client = DbOps.MainOps()
# db = client.ref_db("companies")

def change_company(request, db):
    try:
        db["company"].update({},{"$set": {"current_company": request.POST.get("companies")}}, upsert=True)
    except Exception as e:
        pprint(e)
        return False
    return True

def get_current_company(db):
    data = db["company"].find_one()
    return data