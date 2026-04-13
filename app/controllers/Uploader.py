import os
import time
import csv
import re
import subprocess
import math
from io import open
from django.contrib import messages
from pprint import pprint

from django.shortcuts import redirect
from . import DbOps, BankRecon as br
from datetime import date, datetime, timedelta
from uuid import uuid4
from django.core.files.storage import default_storage
from app.controllers.helpers import Company
from app.controllers import DbOps
# from helpers.Deduplicator import compare_passbook_to_gl as cptg


db_mo = DbOps.MainOps()


def to_two_dec(f):
    return float(str(f)[0 : str(f).index(".") + 3])


def upload_message(request, msg_level, msg):
    if msg_level == "error":
        return messages.error(request, msg)
    elif msg_level == "success":
        return messages.success(request, msg)


def get_structured_csv(csv_headers, csv_values):
    csv_list_dict = []
    for csv_row in csv_values:
        csv_list_dict.append(dict(zip(csv_headers, csv_row)))
    return csv_list_dict

# def get_list_of_bank_accounts(db):
#     bank_account_col = db["bank_accounts"]
#     return [x["account_number"] for x in bank_account_col.find()]


def insert_csv_data(csv_file_name, csv_list_dict, request):
    try:
        company_code = request.session["company_code"]
    except:
        return redirect("logout")
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    bank_account_col = db["bank_accounts"]
    general_ledger_col = db["general_ledger"]
    csv_file_name = f"{csv_file_name.replace(' ', '_')}"
    # glbs = determine_gl_bs(csv_file_name)
    glbs = request.POST.get("file_operation")
    if glbs == "gl":
        target_upload = db["general_ledger"]
    elif glbs == "bs":
        target_upload = db["bank_statement"]

    list_of_bank_accounts = [x["account_number"] for x in bank_account_col.find()]
    
    for data in csv_list_dict:
        if glbs == "bs":
            acc_no = data.get("account_number", "None")
            if acc_no not in list_of_bank_accounts:
                messages.error(
                    request,
                    "Account number "+acc_no+" does not exist in our list of bank accounts. please add it in our database.",
                )
        query = {}
        for k,v in data.items:
            query[k]=v

        if general_ledger_col.find(query):
            messages.error(
                    request,
                    "Record already exists in our database",
                )
            

        data["date_modified"] = datetime.utcnow() + timedelta(hours=8)
        data["approved"] = "pending"

    target_upload.insert_many(csv_list_dict)

    upload_list = {
        "$set": {
            "record_name": csv_file_name.replace(" ", "_"),
            "date_modified": datetime.utcnow() + timedelta(hours=8),
        }
    }
    return db["upload_list"].update_one(
        {"record_name": f"{csv_file_name.replace(' ', '_')}"}, upload_list, upsert=True
    )


#  to test
def create_backup(request, collection):
    try:
        company_code = request.session["company_code"]
    except:
        return redirect("logout")
    backup_dir = (
        "/home/admin/apps/bank_recon/development/recon/app/forms/data/"
        + time.strftime("%Y%m%d_%H%M%S")
    )
    cmd = f"mongodump --db={company_code}_bank_recon --collection={collection} --out={backup_dir}"
    try:
        os.system(cmd)
    except Exception as e:
        return upload_message(
            request, "error", "Backup operation failed. Please try again."
        )


def upload_record_data(request, collection):
    """
    Cannot handle mismatched headers for now
    """
    try:
        company_code = request.session["company_code"]
    except:
        return redirect("logout")
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    try:
        csv_file = request.FILES["file_upload"]
        target_upload = collection.replace("-", "_")
        file_ops = request.POST.get("file_operation")

        if not csv_file.name.endswith(".csv"):
            messages.error(
                request,
                "Uploading file failed: Upload file using CSV format and try again.",
            )

        try:
            eventid = f"{datetime.now().strftime('%Y%m-%d%H-%M%S-')}{str(uuid4())}.csv"
            path = (
                "/home/admin/apps/bank_recon/development/recon/uploadfiles/csv_files/"
            )
            default_storage.save(path + eventid, csv_file)
            csv_data = csv_to_listdict(eventid)

            if file_ops != "new":
                keys = br.get_keys(target_upload, db)
                keys.remove("_id")
                if "date_modified" in keys:
                    keys.remove("date_modified")
                if "approved" in keys:
                    keys.remove("approved")
        except Exception as e:
            import traceback
            import sys
            return messages.error(
                request,
                "Failed reading CSV file. Kindly check the format and try again. "
                + f"{e}{traceback.format_exc()}",
            )

        if file_ops == "append":
            if sorted(keys) == sorted(csv_data[0].keys()):
                if insert_csv_data(target_upload, csv_data, request):
                    upload_message(
                        request, "success", "Uploaded Record was successfully added."
                    )
            else:
                return upload_message(
                    request,
                    "error",
                    "Uploaded Record headers didn't match with existing headers.",
                )
        elif file_ops == "overwrite":
            create_backup(request, target_upload)
            db[target_upload].drop()
            if insert_csv_data(target_upload, csv_data, request):
                upload_message(
                    request, "success", "Uploaded Record was successfully added."
                )
        elif file_ops == "new":
            if insert_csv_data(target_upload, csv_data, request):
                upload_message(
                    request, "success", "Uploaded Record was successfully added."
                )
        else:
            return upload_message(
                request, "error", "Select either Append or Overwrite."
            )

    except Exception as e:
        messages.error(request, "Uploading file failed. " + repr(e))
        pprint(repr(e))
        return {"error": "Uploading failed: " + repr(e)}


def csv_to_listdict(file_name: str, codecs: str = "utf-8-sig") -> list:
    paths = "/home/admin/apps/bank_recon/development/recon/uploadfiles/csv_files/"
    try:
        with open(f"{paths}{file_name}", "r", encoding=codecs) as f:
            list_dict = []
            for row in csv.DictReader(f, skipinitialspace=True):
                dict_struct = {}
                for k, v in row.items():
                    if "amt" in k.lower() or "amount" in k.lower():
                        try:
                            v = to_two_dec(v)
                        except:
                            v = float(v.strip().replace(",", "")) if v else 0.00
                            v = to_two_dec(v)
                    elif "date" in k.lower():
                        v = datetime.strptime(v, "%m/%d/%Y") if v else ""
                    else:
                        v = v.strip()
                    dict_struct[
                        re.sub(
                            r"\W+", "", " ".join(k.lower().split()).replace(" ", "_")
                        )
                    ] = v
                list_dict.append(dict_struct)
            return list_dict
    except:
        codecs = "latin-1"
        with open(f"{paths}{file_name}", "r", encoding=codecs) as f:
            list_dict = []
            for row in csv.DictReader(f, skipinitialspace=True):
                dict_struct = {}
                for k, v in row.items():
                    if "amt" in k.lower() or "amount" in k.lower():
                        try:
                            v = to_two_dec(v)
                        except:
                            v = float(v.strip().replace(",", "")) if v else 0.00
                            v = to_two_dec(v)
                    elif "date" in k.lower():
                        v = datetime.strptime(v, "%m/%d/%Y") if v else ""
                    else:
                        v = v.strip()
                    dict_struct[
                        re.sub(
                            r"\W+", "", " ".join(k.lower().split()).replace(" ", "_")
                        )
                    ] = v
                list_dict.append(dict_struct)
            return list_dict

def determine_gl_bs(filename):
    fname = filename.split(".")[0]
    gl_or_bs = fname.split("_")[-1]
    return gl_or_bs


def upload_file(request):
    try:
        company_code = request.session["company_code"]
    except:
        return redirect("logout")
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    gl_col = db["gl_collection"]
    bs_col = db["bs_collection"]
    try:
        csv_file = request.FILES["file_upload"]
        csv_file_name = request.FILES["file_upload"].name.split(".csv")[0]
        
        if not csv_file.name.endswith(".csv"):
            messages.error(
                request,
                "Uploading file failed: Upload file using CSV format and try again.",
            )

        try:
            eventid = f"{datetime.now().strftime('%Y%m-%d%H-%M%S-')}{str(uuid4())}.csv"
            path = (
                "/home/admin/apps/bank_recon/development/recon/uploadfiles/csv_files/"
            )
            default_storage.save(path + eventid, csv_file)
            csv_data = csv_to_listdict(eventid)

        except Exception as e:
            return messages.error(
                request,
                "Failed reading CSV file. Kindly check the format and try again. "
                + f"{e}",
            )
        print(csv_data[1])

        list_collections = br.get_collections(db)

        record_name = re.sub("[^A-Za-z0-9_ ]+", "", csv_file_name)
        clean_record_name = " ".join(record_name.lower().split()).replace(" ", "_")

        if clean_record_name in list_collections:
            keys = br.get_keys(clean_record_name, db)
            keys.remove("_id")
            if "date_modified" in keys:
                keys.remove("date_modified")

            if sorted(csv_data[0].keys()) == sorted(keys):
                if insert_csv_data(clean_record_name, csv_data, request):
                    upload_message(
                        request, "success", "Successfully uploaded new record/s."
                    )
            else:
                if insert_csv_data(f"{clean_record_name}_new", csv_data, request):
                    upload_message(
                        request,
                        "success",
                        "Successfully uploaded existing format with new fields.",
                    )
        else:
            if insert_csv_data(clean_record_name, csv_data, request):
                upload_message(
                    request, "success", "Successfully uploaded a new format."
                )

        return True

    except Exception as e:
        pprint(e)
        messages.error(request, "Uploading file failed. " + f"{e}")
        return {"error": "Uploading failed: " + repr(e)}


def upload_access_file(request):
    try:
        company_code = request.session["company_code"]
    except:
        return redirect("logout")
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    file = request.FILES["file_upload_access"]
    file_name = request.FILES["file_upload_access"].name.split(".")[0]
    file_type = request.FILES["file_upload_access"].name.split(".")[1]
    try:
        access_file_name = f"{datetime.now().strftime('%Y%m-%d%H-%M%S-')}{str(uuid4())}-{file_name}.{file_type}"
        path = r"/home/admin/apps/bank_recon/development/recon/uploadfiles/"
        default_storage.save(path + access_file_name, file)

        table_names = subprocess.check_output(
            ["mdb-tables", path + access_file_name]
        ).decode("utf-8")
        tables = str(table_names).split(" ")

        for table in tables:
            if table != "" and "TRM_" in table or "TRN_" in table:
                clean_table_name = re.sub("[^A-Za-z0-9_ ]+", "", table)
                record_name = " ".join(clean_table_name.lower().split()).replace(
                    " ", "_"
                )
                csv_filename = (
                    f"{datetime.now().strftime('%Y%m-%d%H-%M%S-')}{str(uuid4())}.csv"
                )
                print("Exporting " + table)
                with open(path + r"csv_files/" + csv_filename, "wb") as f:
                    subprocess.check_call(
                        ["sudo", "mdb-export", path + access_file_name, table], stdout=f
                    )

                csv_data = csv_to_listdict(csv_filename)  # test

                if csv_data:
                    list_collections = br.get_collections(db)
                    if record_name in list_collections:
                        keys = br.get_keys(record_name, db)
                        keys.remove("_id")
                        if "date_modified" in keys:
                            keys.remove("date_modified")
                        if sorted(csv_data[0].keys()) == sorted(keys):
                            insert_csv_data(record_name, csv_data, request)
                        else:
                            # option to handle not matched keys and updating all entries
                            pass
                    else:
                        insert_csv_data(record_name, csv_data, request)

        upload_message(request, "success", "Uploaded Record was successfully added.")
        return True
    except Exception as e:
        return messages.error(
            request,
            "Failed reading access file. " + f"{e}",
        )
