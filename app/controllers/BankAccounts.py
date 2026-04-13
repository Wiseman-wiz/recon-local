from bson.objectid import ObjectId
from django.contrib import messages
from pprint import pprint
from app.controllers import DbOps
from datetime import datetime

db_mo = DbOps.MainOps()


def get_bank_accounts(company_code):
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    accounts = list(db["bank_accounts"].find({}))
    for account in accounts:
        account["_id"] = str(account["_id"])
        account["id"] = account.pop("_id")
    return accounts


def create_bank_account(request):
    company_code = request.session["company_code"]
    
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    request_data = {
        "subaccount": request.POST.get("form_1-subaccount"),
        "account_number": request.POST.get("form_1-account_number"),
        "account_holder": request.POST.get("form_1-account_holder"),
        "bank_name": request.POST.get("form_1-bank_name"),
        "bank_branch": request.POST.get("form_1-bank_branch"),
        "account_type_1": request.POST.get("form_1-account_type_1"),
        "account_type_2": request.POST.get("form_1-account_type_2"),
        "beginning_balance": float(request.POST.get("form_1-beginning_balance")),
        "current_balance": float(request.POST.get("form_1-beginning_balance")),
        "last_updates": ""
    }

    try:
        db["bank_accounts"].insert_one(request_data)
        trail = db_mo.connect("trail","trail_recon")
        trail.insert_one({
            "user":str(request.user),
            "action":"create_bank_account",
            "action_type":"create",
            "company":str(company_code),
            "time":datetime.now(),
            "actions":[request_data]
        })
        return True

    except Exception as e:
        messages.error(request, "Creating New Bank Account Failed. " + repr(e))
        return False


def update_bank_account(request):
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    request_data = {
        "subaccount": request.POST.get("subaccount"),
        "account_number": request.POST.get("account_number"),
        "account_holder": request.POST.get("account_holder"),
        "bank_name": request.POST.get("bank_name"),
        "bank_branch": request.POST.get("bank_branch"),
        "account_type_1": request.POST.get("account_type_1"),
        "account_type_2": request.POST.get("account_type_2"),
        "beginning_balance": float(request.POST.get("beginning_balance")),
    }

    try:
        db["bank_accounts"].update_one(
            {"_id": ObjectId(request.POST.get("id"))}, {"$set": request_data}
        )
        
        trail = db_mo.connect("trail","trail_recon")
        now = datetime.now()
        trail.insert_one({
            "user":str(request.user),
            "action":"update_bank_account",
            "action_type":"update",
            "company":str(company_code),
            "time":now,
            "actions":[{"_id": ObjectId(request.POST.get("id"))}, {"$set": request_data}]
        })
        return True

    except Exception as e:
        messages.error(request, "Updating Bank Account Failed. " + repr(e))
        return False


def delete_bank_account(request):
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    bank_acct_obj_id = ObjectId(request.POST.get("id"))
    user = str(request.user)
    try:
        data =dict(db["bank_accounts"].find_one({"_id": bank_acct_obj_id}))
        if data:
            deleted_docs = {
                "src_collection": "bank_accounts",
                "deleted_documents": [data],
                "remarks": request.POST.get("remarks"),
                "user": user,
                "deleted_at": datetime.now()
            }
            db["deleted_documents"].insert_one(deleted_docs)
            db["bank_accounts"].delete_one({"_id": bank_acct_obj_id})
        trail = db_mo.connect("trail","trail_recon")
        trail.insert_one({
            "user":user,
            "action":"delete_bank_account",
            "action_type":"delete",
            "company":str(company_code),
            "time":datetime.now(),
            "actions":[{"_id": bank_acct_obj_id,"data":data}]
        })
        return True

    except Exception as e:
        messages.error(request, "Deleting Bank Account Failed. " + repr(e))
        return False
