from pprint import pprint
from datetime import datetime as dt
from django.contrib import messages
from . import DbOps
from bson.objectid import ObjectId
from .helpers.merge_report import (
    Merger
)
import pymongo
import math
import pandas as pd


client = DbOps.MainOps()
db_bank_recon = "bank_recon"
col_accounts = "bank_accounts"
col_bs1 = "bank_statement_format_1"
col_bs2 = "bank_statement_format_2"
col_gl = "general_ledger"


src_bank_accounts = client.connect(
    database=db_bank_recon, collection=col_accounts)


def to_two_dec(f):
    return math.floor(f * 10 ** 2) / 10 ** 2


def get_one_record(collection: str, db) -> dict:
    return dict(db[collection].find_one())


def get_specific_report(report_name, db):
    target_collection = db["match_reports"]
    target_return = target_collection.find_one({"_id": ObjectId(report_name)})

    filter_qry_src = {}
    for key, value in target_return['filter_source'].items():
        if value != 0:
            filter_qry_src[key] = int(value)

    filter_qry_trg = {}
    for key, value in target_return['filter_target'].items():
        if value != 0:
            filter_qry_trg[key] = int(value)

    if filter_qry_src or filter_qry_trg:
        report_source = list(db[target_return["source"]].find({},filter_qry_src))
        report_target = list(db[target_return["target"]].find({},filter_qry_trg))
    else:
        report_source = list(db[target_return["source"]].find({}))
        report_target = list(db[target_return["target"]].find({}))

    matched_records = []
    src_keys = []
    trgt_keys = []

    selected_match_fields = {}
    for key in target_return["match"]:
        if target_return["match"][key] != '':
            selected_match_fields[key] = target_return["match"][key]
            src_keys.append(key)
            trgt_keys.append(target_return["match"][key])

    source_df = pd.DataFrame(report_source)
    target_df = pd.DataFrame(report_target)


    merged_data = Merger(source_df,
                        target_df,
                        src_keys,
                        trgt_keys)
                
    final_report = []
    for data in matched_records:
        doc = {}
        for key, value in data.items():
            if not isinstance(value, ObjectId):
                doc[key] = value
            if doc:
                doc["report_name"] = target_return['report_name']
                final_report.append(doc)

    db[f"match_reports_{target_return['report_name']}"].remove({})

    for data in final_report:
        try:
            db[f"reports_{target_return['report_name']}"].insert_one(data)
        except pymongo.errors.DuplicateKeyError:
            # skip document because it already exists in new collection
            continue
    if matched_records:
        summary_report = {k: v for k, v in matched_records[0].items()}
        for data in matched_records[1:]:
            for key, value in data.items():
                try:
                    summary_report[key] += float(value)
                except:
                    summary_report[key] = ""
    else:
        summary_report = {"none": ""}
    db[f"summary_match_reports_{target_return['report_name']}"].remove({})
    try:
        db[f"summary_match_reports_{target_return['report_name']}"].insert_one(
            summary_report
        )
    except pymongo.errors.DuplicateKeyError:
        pass

    final_data = {
        "report_name": target_return["report_name"],
        "source": target_return["source"],
        "target": target_return["target"],
        "summary_report": [summary_report],
        "matched_records": merged_data.matched_list,
        "matched_fields": selected_match_fields,
        "unmatched_records_source": merged_data.src_umatched_list,
        "unmatched_records_target": merged_data.trgt_umatched_list,
        "filter_qry_src": target_return['filter_source'],
        "filter_qry_trg": target_return['filter_target'],
    }

    db["match_reports_database"].update_one(
        {"report_name": target_return["report_name"]}, {"$set": final_data}, upsert=True
    )

    return dict(
        db["match_reports_database"].find_one({"report_name": target_return['report_name']})
    )


def get_all_reports(db):
    target_collection = db["match_reports"]
    qry_data = list(
        target_collection.find({}, {"_id": 1, "report_name": 1, "approved": 1}).sort(
            [("_id", -1), ("report_name", 1)]
        )
    )
    ret_data = []
    for data in qry_data:
        if data["approved"] is True:
            ret_data.append(
                {"approved": {
                    "report_name": data["report_name"], "report_id": str(data["_id"])}}
            )
        elif data["approved"] == "pending":
            ret_data.append(
                {"pending": {
                    "report_name": data["report_name"], "report_id": str(data["_id"])}}
            )
        elif data["approved"] is False:
            ret_data.append(
                {"not_approved": {
                    "report_name": data["report_name"], "report_id": str(data["_id"])}}
            )

    return ret_data


def push_data(collection: str, qry: dict, db):
    target_collection = db[collection]
    if target_collection.find_one({"report_name": qry['report_name']}):
        target_collection.replace_one({"report_name": qry['report_name']}, qry)
    else:
        target_collection.insert_one(qry)

    return target_collection.find_one({"report_name": qry['report_name']})


def push_report_to_upload_list(report, db):
    target_collection = db["upload_list"]

    try:
        target_collection.update_one(
            {"record_name": report["source"]},
            {
                "$addToSet": {
                    "reports": {
                        "report_name": report["report_name"],
                        "report_matched": report["target"]
                    }
                }
            }
        )
        target_collection.update_one(
            {"record_name": report["target"]},
            {
                "$addToSet": {
                    "reports": {
                        "report_name": report["report_name"],
                        "report_matched": report["source"]
                    }
                }
            }
        )
    except Exception as e:
        return pprint(f"{e}")


def del_report(request, db):
    try:
        report_ids = request.POST.getlist("report_id")

        if not report_ids:
            messages.error(
                request, "Please select report/s to delete and try again.")
            return False

        if request.POST.get("collection") == "match_reports":
            report_name_list = [db["match_reports"].find_one({"_id": ObjectId(report_id)}, {"report_name": 1})
                                for report_id in report_ids]

            for r_name in report_name_list:
                db[f'summary_match_reports_{r_name["report_name"]}'].drop()
                db[f'match_reports_{r_name["report_name"]}'].drop()
                db["match_reports"].delete_one(
                    {"report_name": r_name["report_name"]})

                db["upload_list"].update_many(
                    {
                        "reports": {
                            "$elemMatch": {
                                "report_name": r_name["report_name"]
                            }
                        }
                    },
                    {
                        "$pull": {
                            "reports": {
                                "report_name": r_name["report_name"]
                            }
                        }
                    }
                )

        return True
    except Exception as e:
        messages.error(request, f"Deleting report failed. {e}")
        return False


def check_field_names(match_fields: dict) -> None:
    db_conn = client.ref_db("bank_recon_field_keywords")

    for key, val in match_fields.items():
        if val != '':
            db_conn["field_keywords"].update_one(
                {"field": key, "keywords": {"$ne": val}},
                {"$addToSet": { "keywords": val }}
            ) 
