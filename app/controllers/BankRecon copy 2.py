import timeit
import time
from time import process_time
from pprint import pprint
from django.contrib import messages
from . import DbOps
from bson.objectid import ObjectId
from .helpers.aje_export_module import (
    populate_entries as pe,
    random_string_generator as rsg,
)
from .helpers.bank_account_module import get_monthly_adj_cash_bal
from .helpers.BankReconReport import (
    BankReconReport,
    get_book_error,
    get_outstanding_checks,
)
from .helpers.match_unmatch_segregate import Segregator
from .helpers.calculate_bank_recon_report import ReportCalculator
import pymongo
import pandas as pd
import datetime as lo
import math
from datetime import datetime
from dateutil.relativedelta import relativedelta
from uuid import uuid4
import json
from collections import Counter


db_mo = DbOps.MainOps()
col_accounts = "bank_accounts"
col_bs1 = "bank_statement_format_1"
col_bs2 = "bank_statement_format_2"
col_gl = "general_ledger"
bank_account_collection = "bank_account"

from typing import List
def add_record_matched_id(matched_record, db):
    """
        Docstring 
        Accepts matched_record data and so on oiqhdqiowdoqiwhdoihd
    """
    final_matched = []
    for idx, value in enumerate(matched_record):
        match_id = datetime.now().strftime("%Y%m-%d%H-%M%S-") + str(uuid4())

        ref = value["transaction_reference"]
        bs = value["entry_id_bs"]
        gl = value["entry_id_gl"]

        gl_id = db["general_ledger"].find_one({"_id":ObjectId(gl)})
        bs_id = db["bank_statement"].find_one({"_id":ObjectId(bs)})

        if not gl_id["record_matched_id"]:
            db["general_ledger"].update(
                {"other_03": value["transaction_reference"]},
                {"$set": {"record_matched_id": match_id, "is_matched": True}},
            )

        if not bs_id["record_matched_id"]:
            db["bank_statement"].update(
                {"transaction_reference": value["transaction_reference"]},
                {"$set": {"record_matched_id": match_id, "is_matched": True}},
            )
        value["record_matched_id_gl"] = gl_id["record_matched_id"]
        value["record_matched_id_bs"] = bs_id["record_matched_id"]

        final_matched.append(value)

    matched_with_idx = final_matched

    return matched_with_idx

def to_two_dec(f):
    return math.floor(f * 10 ** 2) / 10 ** 2

# JMR Start
ORIGINAL_GL_DATE_FIELDS = [
    "check_date",
    "trndate"
]
UNMATCHED_GL_REPORT_DATE_FIELDS = [
    "trndate",
    "trndate_gl",
    "transaction_date",
    "transaction_date_gl",
    "date_modified_gl",
    "check_date",
]
UNMATCHED_BS_REPORT_DATE_FIELDS = [
    "date_modified_bs",
    "transaction_date",
    "transaction_date_bs",
    "trndate",
    "trndate_bs",
    "check_date",
]

MATCHED_REPORT_DATE_FIELDS = [
    "date_modified_bs",
    "date_modified_gl",
    "transaction_date_bs",
    "transaction_date_gl",
    "check_date",    
]


def _safe_iso(value: object) -> str:
    """
    Convert a pandas/NumPy/py‑datetime value to ISO‑8601.
    If the value is NaT, NaN, None, or cannot be parsed, return "".
    """
    if pd.isnull(value):                 # catches NaT, NaN, None
        return ""

    # unwrap Timestamp/DatetimeIndex elements to Python datetime
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()

    # anything that still has .isoformat()
    try:
        return value.isoformat()
    except Exception:
        # fall back to plain str(); last‑ditch guarantee
        return str(value)

# JMR End

# unused for review
def add_record_matched_id_filtered(matched_record, db):
    final_matched = []
    for idx, value in enumerate(matched_record):
        match_id = datetime.now().strftime("%Y%m-%d%H-%M%S-") + str(uuid4())
        try:
            match_id = value["record_matched_id"]
        except:
            match_id = datetime.now().strftime("%Y%m-%d%H-%M%S-") + str(uuid4())

        bs = value["entry_id_bs"]
        gl = value["entry_id_gl"]

        gl_id = db["general_ledger"].find_one({"_id":ObjectId(gl)})
        bs_id = db["bank_statement"].find_one({"_id":ObjectId(bs)})

        if not gl_id["record_matched_id"]:
            db["general_ledger"].update(
                {"_id": ObjectId(gl)},
                {"$set": {"record_matched_id": match_id, "is_matched": True}},
            )

        if not bs_id["record_matched_id"]:
            db["bank_statement"].update(
                {"_id": ObjectId(bs)},
                {"$set": {"record_matched_id": match_id, "is_matched": True}},
            )
        value["record_matched_id_gl"] = gl_id["record_matched_id"]
        value["record_matched_id_bs"] = bs_id["record_matched_id"]

        final_matched.append(value)

    matched_with_idx = final_matched

    return matched_with_idx


def is_starting_report(subaccount, date, db):
    conn_report = db["reports_database"]
    data = conn_report.find(
        {"subaccount": subaccount}, {"subaccount": 1, "date_period_from": 1}
    )

    for d in data:
        record_date = datetime.strptime(d["date_period_from"], "%m/%d/%Y")
        record_date = record_date.isoformat()
        if record_date < date:
            return False
        elif record_date == date:
            return True
        else:
            return True


def get_final_balance(
    gl_data, pb_data, subaccount, report_period, report_state, report_name, db
):
    conn = db["bank_accounts"]

    (total_gl_movement, total_gl_aje) = gl_data
    (total_bs_movement, deposit_in_transit) = pb_data
    prev_bal = 0

    cur = dict(conn.find_one({"subaccount": subaccount}))    
    if cur.get("last_updates"):
        data = (cur.get("last_updates") + lo.timedelta(days=1)).isoformat()
        report_period = report_period.isoformat()
        if cur.get("current_balance", None) and data == report_period:
            prev_bal = cur.get("current_balance")
        else:
            prev_bal = cur.get("beginning_balance", 0)

    else:
        prev_bal = cur.get("beginning_balance", 0)

    # log the parameter values        
    # Prepare data for debugging
    # data_to_dump = {
    #     "gl_data": gl_data,
    #     "pb_data": pb_data,
    #     "subaccount": subaccount,
    #     "report_period": report_period,
    #     "report_state": report_state,
    #     "report_name": report_name,
    #     "db_keys": list(db.keys()) if isinstance(db, dict) else str(db),
    #     "cur": cur,
    #     "data": data,
    #     "prev_bal": prev_bal
    # }

    # Save to JSON file
    # with open("/home/admin/apps/bank_recon/NewBankReconLog.json", "w+") as f:
            # json.dump(data_to_dump, f, indent=2, default=str)  # default=str handles datetime serialization

    try:
        data = db["reports_database"].find_one({"report_name": report_name})
        if data["approved"] == "approved":
            prev_bal = data["adjusted_cash_bal"]
            # JMR - update the current balance when the report was approved
    except:
        pass
    try:
        if is_starting_report(subaccount, report_period, db):
            prev_bal = cur.get("beginning_balance", 0)
    except:
        pass

    try:
        manual_reversal_of_aje_gl = data["manual_reversal_of_aje_gl"] if data.get("manual_reversal_of_aje_gl") else 0
        manual_reversal_of_aje_bs = data["manual_reversal_of_aje_bs"] if data.get("manual_reversal_of_aje_bs") else 0
    except:
        manual_reversal_of_aje_gl = 0
        manual_reversal_of_aje_bs = 0
    beginning_cash_bal_gl = prev_bal + manual_reversal_of_aje_gl
    beginning_cash_bal_bs = prev_bal + manual_reversal_of_aje_bs
    ending_cash_bal_gl = beginning_cash_bal_gl + total_gl_movement
    ending_cash_bal_bs = beginning_cash_bal_bs + total_bs_movement
    
    final_gl_balance = ending_cash_bal_gl + total_gl_aje
    final_pb_balance = ending_cash_bal_bs + deposit_in_transit
    return (
        final_gl_balance,
        final_pb_balance,
        prev_bal,
        manual_reversal_of_aje_gl,
        manual_reversal_of_aje_bs,
        beginning_cash_bal_gl,
        beginning_cash_bal_bs,
        ending_cash_bal_gl,
        ending_cash_bal_bs
    )

# Added by JMR
def normalize_dates(list_of_dicts, fields):
    """
    Mutates each dict so *fields* become ISO strings ("" if null/NaT).
    """
    for row in list_of_dicts:
        for field in fields:
            row[field] = _safe_iso(row.get(field))
    return list_of_dicts

# def pydater_1(list_data):
#     unmatched_gl_report = []
#     for d in list_data:
#         try:
#             d["date_modified_gl"] = d["date_modified_gl"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["date_modified_gl"] = d["date_modified_gl"].isoformat()
#         except:
#             pass
#         try:
#             d["transaction_date"] = d["transaction_date"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["transaction_date"] = d["transaction_date"].isoformat()
#         except:
#             pass
#         try:
#             d["transaction_date_gl"] = d["transaction_date_gl"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["transaction_date_gl"] = d["transaction_date_gl"].isoformat()
#         except:
#             pass
#         try:
#             d["trndate"] = d["trndate"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["trndate"] = d["trndate"].isoformat()
#         except:
#             pass
#         try:
#             d["trndate_gl"] = d["trndate_gl"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["trndate_gl"] = d["trndate_gl"].isoformat()
#         except:
#             pass

#         unmatched_gl_report.append(d)
#     return unmatched_gl_report

# def pydater_2(list_data):
#     unmatched_bs_report = []
#     for d in list_data:
#         try:
#             d["date_modified_bs"] = d["date_modified_bs"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["date_modified_bs"] = d["date_modified_bs"].isoformat()
#         except:
#             pass
#         try:
#             d["transaction_date"] = d["transaction_date"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["transaction_date"] = d["transaction_date"].isoformat()
#         except:
#             pass
#         try:
#             d["transaction_date_bs"] = d["transaction_date_bs"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["transaction_date_bs"] = d["transaction_date_bs"].isoformat()
#         except:
#             pass
#         try:
#             d["trndate"] = d["trndate"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["trndate"] = d["trndate"].isoformat()
#         except:
#             pass
#         try:
#             d["trndate_bs"] = d["trndate_bs"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["trndate_bs"] = d["trndate_bs"].isoformat()
#         except:
#             pass
#         unmatched_bs_report.append(d)
#     return unmatched_bs_report

# def pydater_3(list_data):
#     matched_report = []
#     for d in list_data:
#         try:
#             d["date_modified_bs"] = d["date_modified_bs"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["date_modified_bs"] = d["date_modified_bs"].isoformat()
#         except:
#             pass
#         try:
#             d["date_modified_gl"] = d["date_modified_gl"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["date_modified_gl"] = d["date_modified_gl"].isoformat()
#         except:
#             pass
#         try:
#             d["transaction_date_bs"] = d["transaction_date_bs"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["transaction_date_bs"] = d["transaction_date_bs"].isoformat()
#         except:
#             pass
#         try:
#             d["transaction_date_gl"] = d["transaction_date_gl"].to_pydatetime()
#         except:
#             pass
#         try:
#             d["transaction_date_gl"] = d["transaction_date_gl"].isoformat()
#         except:
#             pass
            
#         matched_report.append(d)
#     return matched_report

# JMR Start
def clean_date_fields(records, date_keys):
    cleaned = []
    for rec in records:
        for key in date_keys:
            value = rec.get(key)
            if pd.notnull(value):
                try:
                    if hasattr(value, "to_pydatetime"):
                        value = value.to_pydatetime()
                    rec[key] = value.isoformat()
                except Exception:
                    rec[key] = str(value)
            else:
                rec[key] = ""  # or use None, depending on your needs
        cleaned.append(rec)
    return cleaned
# JMR End


def get_one_record(collection: str, db) -> dict:
    return dict(
        db[collection].find_one({}, {"_id": 0, "date_modified": 0, "approved": 0})
    )


# Function called when opening reconcile report.
# This does all the fetching data, calculations, segregating matches and unmatches, 
# and presents all the information on the reconciled report
def get_specific_report(
    collection_id: str, db, request, date_start=None, date_end=None
    
) -> dict:
    target_collection = db["reports"]
    # Retrieve the report document by its _id from the "reports" collection.
    target_return = target_collection.find_one({"_id": ObjectId(collection_id)})
    
    # data_to_dump = {
    #     "date_start": date_start,
    #     "date_end": date_end
    # }
    
    # with open("/home/admin/apps/bank_recon/DebuggingBankRecon.json", "w+") as f:
    #         json.dump(data_to_dump, f, indent=2, default=str)  # default=str handles datetime serialization
    
    # Will only read information from reports_database if report is already approved
    # This will prevent any recalculation on final recon report.
    if target_return["approved"] == "pending":
        pprint("============= NOT APPROVED ==============")
        
        # Check if the report has an "is_report_viewed" flag; if error occurs, set it to False.
        try:
            if target_return.get("is_report_viewed"):
                pass
        except Exception as e:
            db["reports"].update_one(
                {"_id": ObjectId(collection_id)},
                {"$set": {"is_report_viewed": False }}
            )
        
        # Retrieve filter data for the report, if any.
        report_filter_data = get_report_filter(collection_id, db)

        # If the report was filtered or has been viewed previously, process the filtered report flow.
        # This is to prevent recomputing all raw data which might cause discrepancies due to the nature of their data.
        if report_filter_data or target_return.get("is_report_viewed"):
            pprint("============= WITH FILTER ==============")
            # Retrieve the corresponding report details from the reports_database collection.
            report_db = dict(
                db["reports_database"].find_one(
                    {"report_name": target_return["report_name"]}
                )
            )
            approval = None
            # Attempt to get the approval status from report_db; if missing, default to "pending".
            try:
                approval = report_db["approved"]
            except:
                approval = "pending"
            if not approval:
                raise "Shit" # ask cali why
            
            # Initialize variables for unmatched records.
            unmatched_records_source_df = ""
            unmatched_records_target_df = ""
            # Convert filtered GL matched records, book error list, and matched records into DataFrames.
            filtered_gl_matched_df = pd.DataFrame(report_db["filtered_gl_matched"])
            book_error_list_df = pd.DataFrame(report_db["book_error_list"])
            matched_records_df = pd.DataFrame(report_db["matched_records"])

            # Update approval status for each filtered BS matched record.
            for data in report_db["filtered_bs_matched"]:
                db["bank_statement"].update_one(
                    {"_id": ObjectId(data["entry_id_bs"])},
                    {"$set": {"approved": approval}},
                )
            # Update approval status for each filtered GL matched record.
            for data in report_db["filtered_gl_matched"]:
                db["general_ledger"].update_one(
                    {"_id": ObjectId(data["entry_id_gl"])},
                    {"$set": {"approved": approval}},
                )

            # Update bank statement records in matched_records based on transaction_reference.
            approved_bs_ids = [i for i in report_db.get("matched_records")]
            for _id in approved_bs_ids:
                db["bank_statement"].update_many(
                    {"transaction_reference": _id["transaction_reference"]},
                    {"$set": {"approved": approval, "is_matched": True}},
                )
                if _id["record_matched_id_bs"]:
                    db["bank_statement"].update_many(
                        {"transaction_reference": _id["transaction_reference"]},
                        {"$set": {
                            "approved": approval,
                            "record_matched_id": _id["record_matched_id_bs"],
                        }},
                    )

            # Re-assign matched_records for further processing for GL updates.
            approved_bs_ids = [i for i in report_db.get("matched_records")]

            # Update general ledger records in matched_records using the "other_03" field.
            for _id in approved_bs_ids:
                db["general_ledger"].update_many(
                    {"other_03": _id["transaction_reference"]},
                    {"$set": {"approved": approval, "is_matched": True}},
                )
                if _id["record_matched_id_gl"]:
                    db["general_ledger"].update_many(
                        {"other_03": _id["transaction_reference"]},
                        {"$set": {
                            "approved": approval,
                            "record_matched_id": _id["record_matched_id_gl"],
                        }},
                    )

            # Extract approved GL and BS entry IDs from matched records.
            approved_gl_ids = [
                i["entry_id_gl"] for i in report_db.get("matched_records")
            ]
            approved_bs_ids = [
                i["entry_id_bs"] for i in report_db.get("matched_records")
            ]

            # Update individual GL records by _id.
            for _id in approved_gl_ids:
                _id = str(_id)
                db["general_ledger"].update_one(
                    {"_id": ObjectId(_id)},
                    {"$set": {"approved": approval, "is_matched": True}},
                )

            # Update individual BS records by _id.
            for _id in approved_bs_ids:
                _id = str(_id)
                db["bank_statement"].update_one(
                    {"_id": ObjectId(_id)},
                    {"$set": {"approved": approval, "is_matched": True}},
                )

            # Extract approved GL and BS IDs from unmatched records.
            approved_gl_ids = [
                i["entry_id_gl"] for i in report_db.get("unmatched_records_source")
            ]
            approved_bs_ids = [
                i["entry_id_bs"] for i in report_db.get("unmatched_records_target")
            ]

            # Redundant re-assignment of approved IDs for unmatched records.
            approved_gl_ids = [i["entry_id_gl"] for i in report_db.get("unmatched_records_source")]
            approved_bs_ids = [i["entry_id_bs"] for i in report_db.get("unmatched_records_target")]

            # For original BS records not in approved_bs_ids, update their approval and match status.
            if report_db.get("original_bs"):
                original_bs = [str(i["entry_id"]) for i in report_db.get("original_bs")]
                for _id1 in original_bs:
                    if _id1 not in list(str(x) for x in approved_bs_ids):
                        db["bank_statement"].update_one(
                            {"_id": ObjectId(_id1)},
                            {"$set": {"approved": approval, "is_matched": True}},
                        )
            # For original GL records not in approved_gl_ids, update their approval and match status.
            if report_db.get("original_gl"):
                original_gl = [str(i["entry_id"]) for i in report_db.get("original_gl")]
                for _id1 in original_gl:
                    if _id1 not in list(str(x) for x in approved_gl_ids):
                        db["general_ledger"].update_one(
                            {"_id": ObjectId(_id1)},
                            {"$set": {"approved": approval, "is_matched": True}},
                        )

            # Duplicate block for original BS records update.
            if report_db.get("original_bs"):
                original_bs = [str(i["entry_id"]) for i in report_db.get("original_bs")]
                for _id1 in original_bs:
                    if _id1 not in list(str(x) for x in approved_bs_ids):
                        db["bank_statement"].update_one(
                            {"_id": ObjectId(_id1)},
                            {"$set": {"approved": approval, "is_matched": True}},
                        )
            # Duplicate block for original GL records update.
            if report_db.get("original_gl"):
                original_gl = [str(i["entry_id"]) for i in report_db.get("original_gl")]
                for _id1 in original_gl:
                    if _id1 not in list(str(x) for x in approved_gl_ids):
                        db["general_ledger"].update_one(
                            {"_id": ObjectId(_id1)},
                            {"$set": {"approved": approval, "is_matched": True}},
                        )
            # Update GL records to mark as not matched.
            for _id in approved_gl_ids:
                _id = str(_id)
                db["general_ledger"].update_one(
                    {"_id": ObjectId(_id)},
                    {"$set": {"approved": approval, "is_matched": False}},
                )

            # Update BS records to mark as not matched.
            for _id in approved_bs_ids:
                _id = str(_id)
                db["bank_statement"].update_one(
                    {"_id": ObjectId(_id)},
                    {"$set": {"approved": approval, "is_matched": False}},
                )
            
            # Create DataFrame for unmatched source records if available, else an empty DataFrame with matched_records columns.
            if report_db["unmatched_records_source"]:
                unmatched_records_source_df = pd.DataFrame(report_db["unmatched_records_source"])
            else:
                unmatched_records_source_df = pd.DataFrame(columns=list(matched_records_df.columns))
                
            # Create DataFrame for unmatched target records if available, else an empty DataFrame with matched_records columns.
            if report_db["unmatched_records_target"]:
                unmatched_records_target_df = pd.DataFrame(report_db["unmatched_records_target"])
            else:
                unmatched_records_target_df = pd.DataFrame(columns=list(matched_records_df.columns))
            
            # Instantiate the ReportCalculator to perform various reconciliation calculations.
            rep_calc = ReportCalculator(
                general_ledger_df=unmatched_records_source_df,
                bank_statement_df=unmatched_records_target_df,
                matched_df=matched_records_df,
                filtered_df=filtered_gl_matched_df,  # only GL records are needed for calculation
                book_errors_df=book_error_list_df,
                to_match=report_db["matched_fields"],
            )

            # Retrieve various reconciliation amounts from the ReportCalculator and report_db.
            adjusted_cash_bal = report_db["adjusted_cash_bal"]
            credit_memo = rep_calc.credit_memo_amount
            debit_memo = rep_calc.debit_memo_amount
            reversal_of_aje = rep_calc.reversal_of_aje_amount
            stale_checks = rep_calc.stale_checks_amount
            outstanding_checks = rep_calc.outstanding_checks_amount
            deposit_in_transit = rep_calc.deposit_on_transit_amount
            book_errors = rep_calc.book_errors_amt
            total_gl_dr_amt = report_db["total_gl_dr_amt"]
            total_gl_cr_amt = report_db["total_gl_cr_amt"]
            total_bs_dr_amt = report_db["total_bs_dr_amt"]
            total_bs_cr_amt = report_db["total_bs_cr_amt"]
            total_gl_movement = report_db["total_gl_movement"]
            total_bs_movement = report_db["total_bs_movement"]

            # Calculate total GL adjustments by summing individual adjustment amounts.
            total_gl_aje = (
                reversal_of_aje
                + stale_checks
                + outstanding_checks
                + debit_memo
                + credit_memo
                + book_errors
            )

            approved = False

            # Retrieve bank account details for the given subaccount.
            conn = db["bank_accounts"]
            bank_acct_details = conn.find_one(
                {"subaccount": target_return["subaccount"]}
            )
            prev_bal = 0

            approved = False
            # Prepare data tuples for GL and PB (bank statement) movements.
            gl_data = (total_gl_movement, total_gl_aje)
            pb_data = (total_bs_movement, deposit_in_transit)
            # Call get_final_balance to compute final balances and related values.
            (
                total_gl_fin_bal, 
                total_pb_fin_bal,
                adjusted_cash_bal,
                manual_reversal_of_aje_gl,
                manual_reversal_of_aje_bs,
                beginning_cash_bal_gl,
                beginning_cash_bal_bs,
                ending_cash_bal_gl,
                ending_cash_bal_bs
            ) = get_final_balance(
                gl_data,
                pb_data,
                target_return["subaccount"],
                target_return["date_period_from"],
                approved,
                target_return["report_name"],
                db
            )
            
            # Prepare a dictionary with the updated calculated reconciliation values.
            updated_report_calc = {
                "adjusted_cash_bal": adjusted_cash_bal,
                "credit_memo": rep_calc.credit_memo_amount,
                "debit_memo": rep_calc.debit_memo_amount,
                "reversal_of_aje": reversal_of_aje,
                "stale_checks": rep_calc.stale_checks_amount,
                "outstanding_checks": rep_calc.outstanding_checks_amount,
                "deposit_in_transit": deposit_in_transit,
                "book_errors_amt": rep_calc.book_errors_amt,
                "total_gl_aje": total_gl_aje,
                "manual_reversal_of_aje_gl": manual_reversal_of_aje_gl,
                "manual_reversal_of_aje_bs": manual_reversal_of_aje_bs,
                "beginning_cash_bal_gl": beginning_cash_bal_gl,
                "beginning_cash_bal_bs": beginning_cash_bal_bs,
                "ending_cash_bal_gl": ending_cash_bal_gl,
                "ending_cash_bal_bs": ending_cash_bal_bs,
                "total_gl_fin_bal": total_gl_fin_bal,
                "total_pb_fin_bal": total_pb_fin_bal,
            }

            # Update the report document in "reports_database" with the calculated values.
            db["reports_database"].update_one(
                {"report_name": target_return["report_name"]},
                {"$set": updated_report_calc},
                upsert=True,
            )

            # Return the updated report document.
            return dict(
                db["reports_database"].find_one(
                    {"report_name": target_return["report_name"]}
                )
            )
              
        pprint("============= INITIAL RECON ==============") 
        # Build filter queries for source (GL) and target (BS) using the report's filter parameters.
        filter_qry_src = {}
        filter_qry_src["approved"] = 1
        for key, value in target_return["filter_source"].items():
            if value != 0 and key != "":
                filter_qry_src[key] = int(value)

        filter_qry_trg = {}
        filter_qry_trg["approved"] = 1
        for key, value in target_return["filter_target"].items():
            if value != 0 and key != "":
                filter_qry_trg[key] = int(value)

        # Retrieve source records from general_ledger and target records from bank_statement based on subaccount/account number.
        report_source = list(
            db["general_ledger"].find({"subacct": target_return["subaccount"]})
        )
        report_target = list(
            db["bank_statement"].find(
                {"account_number": target_return["account_number"]}
            )
        )

        unmatched_records_source = []
        unmatched_records_target = []
        selected_match_fields = {}

        # Extract matching fields from the report configuration.
        for key in target_return["match"]:
            if target_return["match"][key] != "":
                selected_match_fields[key] = target_return["match"][key]

        # Rename the "_id" field to "entry_id" in both source and target records.
        for obj in report_source:
            obj["entry_id"] = obj.pop("_id")
        for obj in report_target:
            obj["entry_id"] = obj.pop("_id")

        report_filter = []

        # Convert the source, target, and filter lists into pandas DataFrames.
        source_df = pd.DataFrame(report_source)
        target_df = pd.DataFrame(report_target)
        filtered_df = pd.DataFrame(report_filter)

        # Instantiate the Segregator to perform matching between GL and BS records.
        seg = Segregator(
            source_df,
            target_df,
            filtered_df,
            selected_match_fields,
            target_return["date_period_from"],
            target_return["date_period_to"],
        )
        
        # refactored - JMR
        date_keys_matched_report = ["trndate", "check_date", "date_modified_gl", "transaction_date", "date_modified_bs"]
        # date_keys_matched_report = ["trndate", "check_date", "date_modified_gl", "transaction_date", "date_modified_bs"]
        date_keys_gl = ["trndate", "check_date", "date_modified"]
        date_keys_bs = ["trndate", "transaction_date", "date_modified_bs", "check_date"]
        date_keys_unmatched_gl = ["trndate", "check_date", "date_modified_gl"]
        
        # Prepare the dictionary to insert matched records and related data into a dedicated report collection.
        to_insert_reports = {
            "report_name": target_return["report_name"],
            "matched_report": seg.match,
            "original_gl": seg.original_gl,
            "original_bs": seg.original_bs,
            "unmatched_gl_report": seg.in_gl_no_bs,
            "unmatched_bs_report": seg.in_bs_no_gl,
            "recon_items": "",
            "book_errors": seg.book_errors,
        }
        
        # refactored - JMR
        # to_insert_reports = {
        #     "report_name": target_return["report_name"],
        #     "matched_report": seg.match,
        #     "original_gl": clean_date_fields(seg.original_gl, date_keys_gl),
        #     "original_bs": clean_date_fields(seg.original_bs, date_keys_bs),
        #     "unmatched_gl_report": clean_date_fields(seg.in_gl_no_bs, date_keys_bs),
        #     "unmatched_bs_report": clean_date_fields(seg.in_bs_no_gl, date_keys_unmatched_gl),
        #     "recon_items": "",
        #     "book_errors": seg.book_errors,
        # }
        
        # JMR
        # data_to_dump = {
        #     "report_source": report_source,
        #     "report_target": report_target,
        #     "report_filter": report_filter,
        #     "selected_match_fields": selected_match_fields,
        #     "date_period_from": target_return["date_period_from"],
        #     "date_period_to": target_return["date_period_to"],
        #     "to_insert_reports": to_insert_reports,
        # }
    
        # JMR
        # with open("/home/admin/apps/bank_recon/BankReconLogs.json", "w+") as f:
        #         json.dump(data_to_dump, f, indent=2, default=str)  # default=str handles datetime serialization

        # Remove any existing records from the report-specific collection.
        db[f"reports_{target_return['report_name']}"].remove({})

        try:
            # Convert Timestamp fields in book errors to ISO formatted datetime strings.
            data = to_insert_reports
            book_errors = []
            for d in to_insert_reports["book_errors"]:
                d["transaction_date"] = d["transaction_date"].to_pydatetime()
                d["trndate"] = d["trndate"].to_pydatetime()
                d["transaction_date"] = d["transaction_date"].isoformat()
                d["trndate"] = d["trndate"].isoformat()
                book_errors.append(d)
            to_insert_reports["book_errors"] = book_errors
            
            # Normalize the date of original_gl using normalize_date function.
            to_insert_reports["original_gl"] = normalize_dates(to_insert_reports["original_gl"], ORIGINAL_GL_DATE_FIELDS)
            
            # Process matched and unmatched reports through pydater functions.
            # Process matched and unmatched reports through normalize_dates function.
            matched_report = normalize_dates(to_insert_reports["matched_report"], MATCHED_REPORT_DATE_FIELDS)
            to_insert_reports["matched_report"] = matched_report

            unmatched_bs_report = normalize_dates(to_insert_reports["unmatched_bs_report"], UNMATCHED_BS_REPORT_DATE_FIELDS)
            to_insert_reports["unmatched_bs_report"] = unmatched_bs_report
            
            unmatched_gl_report = normalize_dates(to_insert_reports["unmatched_gl_report"], UNMATCHED_GL_REPORT_DATE_FIELDS)
            to_insert_reports["unmatched_gl_report"] = unmatched_gl_report
            
            
            # Create a dictionary to hold the data you want to dump
            # data_to_dump = {
                # "data": data,
                # "to_insert_reports": to_insert_reports,
                # "database": db,
                # "request_details": request, # You might want to refine this key based on what 'request' contains
                # "start_date": date_start,
                # "end_date": date_end,
                # "source": "except"
            # }
            # with open("/home/admin/apps/bank_recon/DebuggingBankRecon2.json", "w+") as f:
                # json.dump(data_to_dump, f, indent=2, default=str)  # default=str handles datetime serialization


            # Insert the processed report into the report-specific collection.
            db[f"reports_{target_return['report_name']}"].insert_one(to_insert_reports)

            # Log the report generation action in the trail for auditing.
            trail = db_mo.connect("trail", "trail_recon")
            trail.insert_one(
                {
                    "user": str(request.user),
                    "action": "get_specific_report",
                    "action_type": "create",
                    "company": str(request.session["company_code"]),
                    "time": datetime.now(),
                    "actions": [to_insert_reports],
                }
            )
        except pymongo.errors.DuplicateKeyError:
            pass

        # Retrieve company code from session and instantiate main DB operations.
        company_code = request.session["company_code"]
        db_conn = DbOps.MainOps()

        try:
            # Calculate the monthly adjusted cash balance using the target report records.
            adjusted_cash_bal = get_monthly_adj_cash_bal(report_target, company_code)
        except:
            raise Exception(
                "Unregistered Bank Account Number found. Please add the \
                account number to the system and try accessing the report again."
            )
        # Initialize manual reversals and compute beginning balances.
        manual_reversal_of_aje_gl = 0
        manual_reversal_of_aje_bs = 0
        beginning_cash_bal_gl = adjusted_cash_bal + manual_reversal_of_aje_gl
        beginning_cash_bal_bs = adjusted_cash_bal + manual_reversal_of_aje_bs
        ending_cash_bal_gl = 0
        ending_cash_bal_bs = 0
        credit_memo = seg.credit_memo_amount
        debit_memo = seg.debit_memo_amount
        reversal_of_aje = seg.reversal_of_aje_amount
        stale_checks = seg.stale_checks_amount
        outstanding_checks = seg.outstanding_checks_amount
        deposit_in_transit = seg.deposit_on_transit_amount
        book_errors = seg.book_errors_amt
        total_gl_dr_amt = seg.total_dr_cr_amts["gl_debit_total_amt"]
        total_gl_cr_amt = seg.total_dr_cr_amts["gl_credit_total_amt"]
        total_bs_dr_amt = seg.total_dr_cr_amts["bs_debit_total_amt"]
        total_bs_cr_amt = seg.total_dr_cr_amts["bs_credit_total_amt"]
        total_gl_movement = seg.total_dr_cr_amts["gl_total_cash_movement"]
        total_bs_movement = seg.total_dr_cr_amts["bs_total_cash_movement"]
        credit_memo = seg.credit_memo_amount

        # Calculate total GL adjustments (AJE) by summing individual adjustment components.
        total_gl_aje = (
            reversal_of_aje
            + stale_checks
            + outstanding_checks
            + debit_memo
            + credit_memo
            + book_errors
        )

        approved = False
        gl_data = (total_gl_movement, total_gl_aje)
        pb_data = (total_bs_movement, deposit_in_transit)
        # Call get_final_balance again to recalculate final balances and cash positions.
        (
            total_gl_fin_bal, 
            total_pb_fin_bal,
            adjusted_cash_bal,
            manual_reversal_of_aje_gl,
            manual_reversal_of_aje_bs,
            beginning_cash_bal_gl,
            beginning_cash_bal_bs,
            ending_cash_bal_gl,
            ending_cash_bal_bs
        ) = get_final_balance(
            gl_data,
            pb_data,
            target_return["subaccount"],
            target_return["date_period_from"],
            approved,
            target_return["report_name"],
            db
        )

        # Depending on the report source/target, generate a sheet for unmatched records.
        if "GL" in target_return["source"].upper() or (
            "GENERAL_LEDGER" in target_return["source"].upper()
        ):
            data = unmatched_records_source
            type_of_list = "GL"
            filename = rsg.RandomString_Generator.get_random_filename(type_of_list)
            pe.create_sheet(filename, data, type_of_list, company_code)
        elif "PB" in target_return["target"].upper() or (
            "BANK_STATEMENT" in target_return["target"].upper()
        ):
            data = unmatched_records_target
            type_of_list = "PB"
            filename = rsg.RandomString_Generator.get_random_filename(type_of_list)
            pe.create_sheet(filename, data, type_of_list, company_code)
        else:
            messages.error(request, "Some error happen with type")
            raise Exception

        # Connect to the AJE records collection for the company.
        conn3 = db_conn.ref_db(f"{company_code}_AJE_records")
        # Fetch uploaded AJE draft records.
        recon_aje_list = list(conn3["gl_aje_draft"].find({"recon": 1}))
        for aje in recon_aje_list:
            aje["_id"] = str(aje["_id"])
            aje["original_id"] = str(aje["original_id"])
        
        # Calculate net amounts for matched, unmatched GL, and unmatched BS records.
        matched_recon_list = []
        mdf = pd.DataFrame(seg.match)
        if not mdf.empty:
            mdf["net_gl"] = mdf["credit_amount_gl"] - mdf["debit_amount_gl"]
            mdf["net_bs"] = mdf["debit_amount_bs"] - mdf["credit_amount_bs"]
            matched_recon_list = mdf.to_dict("records")
                
            print("========================")
            print(matched_recon_list)
            
        # has issue
        unmatched_gl_list = []
        umgldf = pd.DataFrame(seg.in_gl_no_bs)
        if not umgldf.empty:
            # change this line of code from this
            # umgldf["net_gl"] = umgldf["debit_amount_gl"] - umgldf["credit_amount_gl"]
            #  to this to handle round off (JMR)
            umgldf["net_gl"] = (umgldf["debit_amount_gl"] - umgldf["credit_amount_gl"]).round(2)
            umgldf["net_gl"] = umgldf["net_gl"].apply(lambda x: 0.0 if abs(x) == 0 else x)

            unmatched_gl_list = umgldf.to_dict("records")
        
            # added line - JMR
            # with open("/home/admin/apps/bank_recon/BankReconLogsNew.json", "w+") as f:
            #         json.dump(unmatched_gl_list, f, indent=2, default=str)  # default=str handles datetime serialization

            print("========================")
            print(unmatched_gl_list)
        
        unmatched_bs_list = []
        umbsdf = pd.DataFrame(seg.in_bs_no_gl)
        if not umbsdf.empty:
            umbsdf["net_bs"] = umbsdf["debit_amount_bs"] - umbsdf["credit_amount_bs"]
            unmatched_bs_list = umbsdf.to_dict("records")
            print("========================")
            print(unmatched_bs_list)
        
        # Build the final data dictionary to update the report details.
        final_data = {
            "report_name": target_return["report_name"],
            "report_id": collection_id,
            "source": target_return["source"],
            "target": target_return["target"],
            "subaccount": target_return["subaccount"],
            "dateperiod": target_return["date_period_to"],
            "date_period_from": target_return["date_period_from"].strftime("%m/%d/%Y"),
            "date_period_to": target_return["date_period_to"].strftime("%m/%d/%Y"),
            "matched_records": matched_recon_list,
            "matched_fields": selected_match_fields,
            "original_bs": seg.original_bs,
            "original_gl": seg.original_gl,
            "unmatched_records_source": unmatched_gl_list,
            "unmatched_records_target": unmatched_bs_list,
            "filter_qry_src": target_return["filter_source"],
            "filter_qry_trg": target_return["filter_target"],
            "adjusted_cash_bal": adjusted_cash_bal,
            "manual_reversal_of_aje_gl": manual_reversal_of_aje_gl,
            "manual_reversal_of_aje_bs": manual_reversal_of_aje_bs,
            "beginning_cash_bal_gl": beginning_cash_bal_gl,
            "beginning_cash_bal_bs": beginning_cash_bal_bs,
            "total_gl_dr_amt": total_gl_dr_amt,
            "total_gl_cr_amt": -total_gl_cr_amt,
            "total_bs_dr_amt": -total_bs_dr_amt,
            "total_bs_cr_amt": total_bs_cr_amt,
            "total_gl_movement": total_gl_movement,
            "total_bs_movement": total_bs_movement,
            "ending_cash_bal_gl": ending_cash_bal_gl,
            "ending_cash_bal_bs": ending_cash_bal_bs,
            "credit_memo": seg.credit_memo_amount,
            "debit_memo": seg.debit_memo_amount,
            "reversal_of_aje": reversal_of_aje,
            "stale_checks": seg.stale_checks_amount,
            "outstanding_checks": seg.outstanding_checks_amount,
            "deposit_in_transit": deposit_in_transit,
            "book_errors": book_errors,
            "total_gl_aje": total_gl_aje,
            "total_gl_fin_bal": total_gl_fin_bal,
            "total_pb_fin_bal": total_pb_fin_bal,
            "filename": filename,
            "recon_aje_list": recon_aje_list,
            "book_error_list": seg.book_errors,
            "unmatched_bs_cols": seg.unmatched_bs_cols,
            "unmatched_gl_cols": seg.unmatched_gl_cols,
            "filtered_bs_matched": [],
            "filtered_gl_matched": [],
        }

        # Update the main and backup report databases with the final data.
        db["reports_database"].update_one(
            {"report_name": target_return["report_name"]}, {"$set": final_data}, upsert=True
        )
        db["reports_database_backup"].update_one(
            {"report_name": target_return["report_name"]}, {"$set": final_data}, upsert=True
        )
        
        # Mark the report as viewed.
        db["reports"].update_one(
            {"_id": ObjectId(collection_id)},
            {"$set": {"is_report_viewed": True }}
        )

    # Return the final report data as a dictionary.
    return dict(
        db["reports_database"].find_one({"report_name": target_return["report_name"]})
    )


def get_reports_database(reports_id: str, db):
    reports_db_data = dict(
        db["reports_database"].find_one({"_id": ObjectId(reports_id)})
    )

    report_data = dict(
        db["reports"].find_one({"report_name": reports_db_data["report_name"]})
    )

    # unmatched_src = reports_db_data["unmatched_records_source"]
    # unmatched_trgt = reports_db_data["unmatched_records_target"]

    return reports_db_data, report_data


def create_report_filter(
    reports_id: str,
    filters: dict,
    unmatched_records_source: dict,
    unmatched_records_target: dict,
    db,
    request=None,
):
    trail = db_mo.connect("trail", "trail_recon")
    trail.insert_one(
        {
            "user": str(request.user) if request else "None",
            "action": "get_specific_report",
            "action_type": "create",
            "company": str(request.session["company_code"]) if request else "None",
            "time": datetime.now(),
            "actions": [
                {
                    "report_id": reports_id,
                    "filter_fields": filters,
                    "unmatched_records_source": unmatched_records_source,
                    "unmatched_records_target": unmatched_records_target,
                    "matched_records_filter": [],
                    "unmatched_records_source_filter": [],
                    "unmatched_records_target_filter": [],
                    "is_submitted": False,
                }
            ],
        }
    )
    return db["reports_filter"].insert_one(
        {
            "report_id": reports_id,
            "filter_fields": filters,
            "unmatched_records_source": unmatched_records_source,
            "unmatched_records_target": unmatched_records_target,
            "matched_records_filter": [],
            "unmatched_records_source_filter": [],
            "unmatched_records_target_filter": [],
            "is_submitted": False,
        }
    )

def get_filter_data(sub_arr:str,filter_id:str,field_name:str,elem_id:int, db):
    record_data = list(
        db["reports_filter"].find(
            {"_id": ObjectId(filter_id)},
            {
                sub_arr: {
                    "$elemMatch": {field_name: elem_id}
                }
            },
        )
    )
    return record_data

def fetch_and_push_to_matched(
    entry_bs_id: list, entry_gl_id: list, report_filter_id: str, db
):

    # convert list of temp filter_ids to int
    entry_bs_id = list(map(int, entry_bs_id))
    entry_gl_id = list(map(int, entry_gl_id))

    # find all fields by ids of each entries of gl and bs
    match_id = datetime.now().strftime("%Y%m-%d%H-%M%S-") + str(uuid4())

    if entry_bs_id and entry_gl_id:
        for bs_oid in entry_bs_id:
            bs_record = get_filter_data("unmatched_records_target",report_filter_id,"filter_bs_id",bs_oid, db)
            unmatched_bs_rec = bs_record[0]["unmatched_records_target"][0]
            
            db["bank_statement"].update_one(
                {"_id": unmatched_bs_rec["entry_id_bs"]},
                {
                    "$set": {
                        "record_matched_id": match_id,
                    }
                },
            )
            unmatched_bs_rec["record_matched_id"] = match_id
            db["reports_filter"].update_one(
                {"_id": ObjectId(report_filter_id)},
                {"$addToSet": {"matched_bs_records_filter": unmatched_bs_rec}},
            )

        for gl_oid in entry_gl_id:
            gl_record = get_filter_data("unmatched_records_source",report_filter_id,"filter_gl_id",gl_oid, db)
            unmatched_gl_rec = gl_record[0]["unmatched_records_source"][0]
            
            db["general_ledger"].update_one(
                {"_id": unmatched_gl_rec["entry_id_gl"]},
                {
                    "$set": {
                        "record_matched_id": match_id,
                    }
                },
            )
            unmatched_gl_rec["record_matched_id"] = match_id
            db["reports_filter"].update_one(
                {"_id": ObjectId(report_filter_id)},
                {"$addToSet": {"matched_gl_records_filter": unmatched_gl_rec}},
            )

        # pprint(records_bs_filter_cols)
        # pprint(records_gl_filter_cols)
    else:
        return {
            "status": "failed",
            "msg": "select 1 entry of GL and BS then try again.",
        }

    try:
        # remove the selected unmatched records filter 
        for bs_oid in entry_bs_id:
            db["reports_filter"].update(
                {"_id": ObjectId(report_filter_id)},
                {"$pull": {"unmatched_records_target": {"filter_bs_id": bs_oid}}},
            )

        for gl_oid in entry_gl_id:
            db["reports_filter"].update(
                {"_id": ObjectId(report_filter_id)},
                {"$pull": {"unmatched_records_source": {"filter_gl_id": gl_oid}}},
            )
    except Exception as e:
        # apply error msg here
        return {"status": "failed", "msg": "error in pulling entries"}

    return {"status": "success", "msg": "successfully update to match"}


def get_report_filter(report_id: str, db):
    crsr = list(
        db["reports_filter"]
        .find({"report_id": ObjectId(report_id)})
        .sort("_id", -1)
        .limit(1)
    )

    return crsr[0] if crsr else []


def set_unmatch_filter(gl_matched_ids: list, bs_matched_ids: list, report_filter_id: str, db):
    def update_report_filter(matched_ids: list, is_gl: bool):
        matched_name = "matched_gl_records_filter" if is_gl else "matched_bs_records_filter"
        unmatched_name = "unmatched_records_source" if is_gl else "unmatched_records_target"
        filter_id_name = "filter_gl_id" if is_gl else "filter_bs_id"
        entry_id_name = "entry_id_gl" if is_gl else "entry_id_bs"
        matched_ids = gl_matched_ids if is_gl else bs_matched_ids
        gl_or_bs = "general_ledger" if is_gl else "bank_statement"
        
        try:
            for m_id in matched_ids:
                rec = get_filter_data(matched_name, report_filter_id, filter_id_name, int(m_id), db)
                matched_rec = rec[0][matched_name][0]
                
                # reset record_match_id in gl or bs to None
                db[gl_or_bs].update_one(
                    {"_id": ObjectId(matched_rec[entry_id_name])}, # getting entry_id of gl or bs
                    {
                        "$set": {
                            "record_matched_id": None,
                        }
                    },
                )
                
                # remove record_matched_id
                matched_rec.pop("record_matched_id")
                # add the matched dict to 
                db["reports_filter"].update_one(
                    {"_id": ObjectId(report_filter_id)},
                    {"$push": { unmatched_name: matched_rec }},
                )

                # remove the selected records to matched filter list
                db["reports_filter"].update_one(
                    { "_id": ObjectId(report_filter_id) },
                    { "$pull": { matched_name: { filter_id_name: int(m_id) } } },
                )

        except:
            return {"status": "failed", "msg": "error in unmatching filters."}
        
    if gl_matched_ids and bs_matched_ids:
        update_report_filter(gl_matched_ids, True)
        update_report_filter(bs_matched_ids, False)
    else:
        return {
            "status": "failed",
            "msg": "select 1 entry of GL and BS to unmatch then try again.",
        }
    
    return {"status": "success", "msg": "successfully updated the lists"}


def merge_filter_reports(report_coll_id: str, report_id: str, filter_id: str, db):
    crsr = get_report_filter(report_id, db)
    
    if crsr.get("matched_gl_records_filter") and crsr.get("matched_bs_records_filter"):
        for item in crsr.get("matched_gl_records_filter"):
            item["is_filter_saved"] = True
            # adding to filtered_match list each entry
            try:
                db["reports_database"].update(
                    {"_id": ObjectId(report_coll_id)},
                    {"$addToSet": {"filtered_gl_matched": item}},
                )
            except:
                return {"status": "failed", "msg": "error in adding filtered matches"}

            # removes filtered matched entries to unmatched lists
            try:
                db["reports_database"].update(
                    {"_id": ObjectId(report_coll_id)},
                    {
                        "$pull": {
                            "unmatched_records_source": {
                                "entry_id_gl": ObjectId(item.get("entry_id_gl"))
                            }
                        }
                    },
                    multi=True,
                )
            except Exception as e:
                return {
                    "status": "failed",
                    "msg": "error in deleting filtered matches in unmatched list",
                    "error_msg": repr(e),
                }
                
        for item in crsr.get("matched_bs_records_filter"):
            item["is_filter_saved"] = True
            # adding to filtered_match list each entry
            try:
                db["reports_database"].update(
                    {"_id": ObjectId(report_coll_id)},
                    {"$addToSet": {"filtered_bs_matched": item}},
                )
            except:
                return {"status": "failed", "msg": "error in adding filtered matches"}

            # removes filtered matched entries to unmatched lists
            try:
                db["reports_database"].update(
                    {"_id": ObjectId(report_coll_id)},
                    {
                        "$pull": {
                            "unmatched_records_target": {
                                "entry_id_bs": ObjectId(item.get("entry_id_bs"))
                            }
                        }
                    },
                    multi=True,
                )
            except Exception as e:
                return {
                    "status": "failed",
                    "msg": "error in deleting filtered matches in unmatched list",
                    "error_msg": repr(e),
                }
                            
        # updates is_submitted field for indication of report having filters
        db["reports_filter"].update(
            {"_id": ObjectId(filter_id)}, {"$set": {"is_submitted": True}}, upsert=True
        )
        db["reports_filter"].update_many(
            {"_id": ObjectId(filter_id)},
            { 
                "$set": { 
                    "matched_bs_records_filter.$[].is_filter_saved": True,
                    "matched_gl_records_filter.$[].is_filter_saved": True,
                }
            }
        )

        # pprint(res)
        # pprint("dahek?")
        return {"status": "success", "msg": "successfully merged to filtered matches"}

    return {"status": "failed", "msg": "No filtered match to merge."}


# def clean_columns_suffix(df, suffix):
#     new_columns = []
#     for col in df.columns:
#         if col.endswith(suffix):
#             new_columns.append(col)
#         else:
#             new_columns.append(col + suffix)
#     return new_columns

# JMR Start
def clean_columns_suffix(df, suffix):
    raw = [str(col).strip() for col in df.columns]
    new_cols = [col if col.endswith(suffix) else col + suffix for col in raw]

    counts = Counter()
    unique_cols = []
    for col in new_cols:
        counts[col] += 1
        if counts[col] == 1:
            unique_cols.append(col)
        else:
            unique_cols.append(f"{col}_{counts[col] - 1}")
    return unique_cols
# JMR End

def clean_filter_suffix(filters, suffix):
    new_columns = []
    for i in filters:
        if i.endswith(suffix):
            new_columns.append(i)
        else:
            new_columns.append(i + suffix)
    return new_columns

#cowboy
#def merge_unmatched_reports(
#    unmatched_bs: dict, unmatched_gl: dict, filter_fields: dict 
#):

def merge_unmatched_reports(
    unmatched_bs: dict, unmatched_gl: dict, filter_fields: dict = None
):
    if filter_fields:
        bs_df = pd.DataFrame(unmatched_bs)
        gl_df = pd.DataFrame(unmatched_gl)
        
        # to clean all cols before adding suffix
        bs_df.columns = clean_columns_suffix(bs_df, "_bs")
        gl_df.columns = clean_columns_suffix(gl_df, "_gl")
        
        # JMR start
        # log the value
        log_payload = {
            "bs_df": {
                "bs_df_shape": bs_df.shape,
                "bs_df_columns": bs_df.columns.tolist(),
                "bs_df_sample": bs_df.head(5).to_dict(orient="records")
            },
            "gl_df": {
                "gl_df_shape": gl_df.shape,
                "gl_df_columns": gl_df.columns.tolist(),
                "gl_df_sample": gl_df.head(5).to_dict(orient="records")
            },
            "filter_fields": filter_fields 
        }
    
        # with open("/home/admin/apps/bank_recon/BankReconLogs3.json", "w+") as f:
        #         json.dump(log_payload, f, indent=2, default=str)  # default=str handles datetime serialization
        # JMR end
        
        bs_df["transaction_date_bs"] = pd.to_datetime(bs_df["transaction_date_bs"])
        gl_df["trndate_gl"] = pd.to_datetime(gl_df["trndate_gl"])
        
        bs_df_cols = bs_df.columns.tolist()
        gl_df_cols = gl_df.columns.tolist()
        
        bs_filter = clean_filter_suffix(list(filter_fields.values()), "_bs")
        gl_filter = clean_filter_suffix(list(filter_fields.keys()), "_gl")
        # may work. this plan is to get all distinct val in each cols then use
        # loc with isin func to separate vals.
        # for key, val in filter_fields.items():
        #     y = bs_df[val].dtypes
        #     pprint(y)
        #     x = bs_df[val].unique().tolist()
        #     meh = gl_df[gl_df[key].isin(x)]
        #     pprint(meh)
        #     pprint('-'*10)
        merged_df = pd.merge(
            bs_df, gl_df, left_on=bs_filter, right_on=gl_filter, how="outer", indicator=True
        )

        match_both_df = merged_df[(merged_df["_merge"] == "both")]
        match_both_cols = match_both_df.columns.tolist()

        # try:
        bs_match_df = match_both_df[bs_df_cols]
        # except:
        #     # to handle cols with same name that have suffix now.
        #     bs_with_suffix_cols = [i for i in match_both_cols if "_bs" in i]
        #     i = 0
        #     for j in bs_df_cols:
        #         if f"{j}_bs" in bs_with_suffix_cols:
        #             bs_df_cols[i] = f"{j}_bs"
        #         i += 1
        #     # pprint(bs_df_cols)
        #     bs_match_df = match_both_df[bs_df_cols]
        bs_match_df = bs_match_df.drop_duplicates(subset=["entry_id_bs"])

        # try:
        gl_match_df = match_both_df[gl_df_cols]
        # except:
        # # to handle cols with same name that have suffix now.
        # gl_with_suffix_cols = [i for i in match_both_cols if "_gl" in i]
        # i = 0
        # for j in gl_df_cols:
        #     if f"{j}_gl" in gl_with_suffix_cols:
        #         gl_df_cols[i] = f"{j}_gl"
        #     i += 1
        # # pprint(gl_df_cols)
        # gl_match_df = match_both_df[gl_df_cols]
        gl_match_df = gl_match_df.drop_duplicates(subset=["entry_id_gl"])

        clean_filter_fields = dict(zip(bs_filter, gl_filter))

        return (
            bs_match_df.to_dict("records"),
            gl_match_df.to_dict("records"),
            clean_filter_fields,
        )
    else:
        bs_match_df = pd.DataFrame(unmatched_bs)
        gl_match_df = pd.DataFrame(unmatched_gl)

        clean_filter_fields = {}
        return (
            bs_match_df.to_dict("records"),
            gl_match_df.to_dict("records"),
            clean_filter_fields,
        )



def push_data(collection: str, qry: dict, db):
    target_collection = db[collection]
    if target_collection.find_one({"report_name": qry["report_name"]}):
        target_collection.replace_one({"report_name": qry["report_name"]}, qry)
    else:
        target_collection.insert_one(qry)

    return target_collection.find_one({"report_name": qry["report_name"]})


def get_all_reports(db):
    target_collection = db["reports"]
    qry_data = list(
        target_collection.find({}, {"_id": 1, "report_name": 1, "approved": 1}).sort(
            [("_id", -1), ("report_name", 1)]
        )
    )

    approved, pending, not_approved = [], [], []

    for data in qry_data:
        if data["approved"] is True:
            approved.append(
                {"report_name": data["report_name"], "report_id": str(data["_id"])}
            )
        elif data["approved"] == "pending":
            pending.append(
                {"report_name": data["report_name"], "report_id": str(data["_id"])}
            )
        elif data["approved"] is False:
            not_approved.append(
                {"report_name": data["report_name"], "report_id": str(data["_id"])}
            )

    return approved, pending, not_approved

    
def get_all_reports_2(db):
    target_collection = db["reports"]
    pipeline = {
        "$group": {}
    }
    qry_data = list(
        target_collection.find({}, {"_id": 1, "report_name": 1, "approved": 1}).sort(
            [("_id", -1), ("report_name", 1)]
        )
    )

    approved, pending, not_approved = [], [], []

    for data in qry_data:
        if data["approved"] is True:
            approved.append(
                {"report_name": data["report_name"], "report_id": str(data["_id"])}
            )
        elif data["approved"] == "pending":
            pending.append(
                {"report_name": data["report_name"], "report_id": str(data["_id"])}
            )
        elif data["approved"] is False:
            not_approved.append(
                {"report_name": data["report_name"], "report_id": str(data["_id"])}
            )

    return approved, pending, not_approved


def get_keys(collection, db):
    # src_collection = client.ref_db("bank_recon")
    return list(data for data in db[collection].find_one())


def get_record_keys(collection, db):
    # src_collection = client.ref_db("bank_recon")
    return list(
        data
        for data in db[collection].find_one(
            {}, {"_id": 0, "date_modified": 0, "approved": 0}
        )
    )


def get_keys_user_access(collection, user_access, db):
    # src_collection = client.ref_db("bank_recon")
    accessible_keys = db["user_access"].find_one(
        user_access, {"_id": 0, "key_access": 1}
    )
    return list(data for data in db[collection].find_one() if data in accessible_keys)


def get_collections(db) -> list:
    list_collections = list(db.list_collection_names())
    return list(
        str(collection)
        for collection in list_collections
        # can be recoded
        if not str(collection).lower().startswith("summary_reports")
        and not str(collection).lower().startswith("reports")
        and not str(collection).lower().startswith("cash_flow_report")
        and not str(collection).lower().startswith("bank_accounts")
        and not str(collection).lower().startswith("match_reports")
        and not str(collection).lower().startswith("summary_match_reports")
        and not str(collection).lower().startswith("check")
        and not str(collection).lower().startswith("main_bank_statement")
        and not str(collection).lower().startswith("upload_list")
        and not str(collection).lower().startswith("deleted_documents")
        and not str(collection).lower().startswith("carryover")
    )


def get_general_ledger(db) -> list:
    list_collection = list(db.list_collection_names(0))
    import re

    print(list_collection)
    r = re.compile(".*_gl_.*")
    return list(filter(r.match, list_collection))


def get_accounts_v2(db):
    # account_list = list(x.get("account_number") for x in db["bank_accounts"].find({},{"account_number":1}))
    account_list = list(
        x.get("account_number")
        for x in db["bank_accounts"].find({}, {"account_number": 1})
    )
    print(account_list)
    return account_list


def get_accounts_by_account_number(db, acct_no):
    return db["bank_accounts"].find_one(
        {"account_number": acct_no}, {"subaccount": 1, "account_number": 1}
    )


def get_accounts_data_v2(db):
    list_collection = list(db.list_collection_names(0))
    final_list = {}
    account_list = []

    for x in list_collection:
        if "_pb_" in x:
            data = db[x].find_one()
            account = data.get("account_number", "None")
            account_list.append(f"{account}")

    for account in account_list:
        final_list[account] = []

    for x in list_collection:
        if "_pb_" in x:
            final_list[account].append(x)

    print(final_list)
    return final_list


def get_accounts(request, db):
    accounts = list(db["bank_accounts"].find({}))
    for account in accounts:
        account["_id"] = str(account["_id"])
        account["id"] = account.pop("_id")
    return accounts


def get_banks(db):
    src_bs_coll = db["bank_statement_collection"]
    src_bs_disb = db["bank_statement_disbursement"]

    list_bs_coll = list(src_bs_coll.find({}))
    list_bs_disb = list(src_bs_disb.find({}))

    for bs_coll in list_bs_coll:
        bs_coll["_id"] = str(bs_coll["_id"])
    for bs_disb in list_bs_disb:
        bs_disb["_id"] = str(bs_disb["_id"])

    bss = {
        "bank_statement_collection": list_bs_coll,
        "bank_statement_disbursement": list_bs_disb,
    }

    return bss


def get_ledgers(db):
    src_gl_coll = db["general_ledger_collection"]
    src_gl_disb = db["general_ledger_disbursement"]

    list_gl_coll = list(src_gl_coll.find({}))
    list_gl_disb = list(src_gl_disb.find({}))

    for gl_coll in list_gl_coll:
        gl_coll["_id"] = str(gl_coll["_id"])
    for gl_disb in list_gl_disb:
        gl_disb["_id"] = str(gl_disb["_id"])

    gls = {
        "general_ledger_collection": list_gl_coll,
        "general_ledger_disbursement": list_gl_disb,
    }

    # pprint(gls)
    return gls


def get_reports(db):
    list_collections = list(db.list_collection_names())
    return list(
        str(collection)
        for collection in list_collections
        if "reports" in str(collection)
    )


#  dont change
def get_transactions(collections, db):
    start = time.time()
    src_transactions = db[collections]
    data_list = list(src_transactions.find({}))
    final_list = []
    for data in data_list:
        new_dict = {}
        for key in data:
            if isinstance(data[str(key)], ObjectId):
                new_dict[str(key)] = str(data[str(key)])
            else:
                key_list = [split_key for split_key in (key.split("_"))]
                new_key = " ".join(key_list)
                new_dict[new_key] = data[key]
        final_list.append(new_dict)
    end = time.time()
    
    print("TIME >>>>>>>>> ", (end - start) * 10**3, "ms")
    
    return final_list


def get_cfr_references(db):
    list_collections = list(db.list_collection_names())
    return list(
        str(collection)
        for collection in list_collections
        if str(collection).lower().startswith("cfr_reference")
    )


# refactor this sht
def get_cfr_bank_statement(db):
    list_collections = list(db.list_collection_names())
    return list(
        str(collection)
        for collection in list_collections
        if str(collection).lower().startswith("cfr_bank_statement")
    )


def get_cfr_bank_account_info(db):
    list_collections = list(db.list_collection_names())
    return list(
        str(collection)
        for collection in list_collections
        if str(collection).lower().startswith("cfr_bank_account")
    )


# end


def push_report_to_upload_list(report, db):
    target_collection = db["upload_list"]

    try:
        target_collection.update_one(
            {"record_name": report["source"]},
            {
                "$addToSet": {
                    "reports": {
                        "report_name": report["report_name"],
                        "report_matched": report["target"],
                    }
                }
            },
        )
        target_collection.update_one(
            {"record_name": report["target"]},
            {
                "$addToSet": {
                    "reports": {
                        "report_name": report["report_name"],
                        "report_matched": report["source"],
                    }
                }
            },
        )
    except Exception as e:
        return pprint(f"{e}")


def del_report(request, db):
    try:
        report_ids = request.POST.getlist("report_id")

        if request.POST.get("remarks") == "":
            messages.error(
                request, "Please input a remarks for deleting and try again.")
            return False
        
        if not report_ids:
            messages.error(request, "Please select report/s to delete and try again.")
            return False

        if request.POST.get("collection") == "reports":
            rep_obj_id = [
                ObjectId(report_id)
                for report_id in report_ids
            ]
            
            report_name_list = list(db["reports"].find(
                {"_id": {"$in": rep_obj_id}}
            ))
            for rep_name in report_name_list:
                db[f'summary_reports_{rep_name["report_name"]}'].drop()
                db[f'reports_{rep_name["report_name"]}'].drop()
                db["reports"].delete_one({"report_name": rep_name["report_name"]})
                
                db["general_ledger"].update_many(
                    {
                        "trndate": { "$gte": rep_name["date_period_from"], "$lte": rep_name["date_period_to"] },
                        "subacct": rep_name["subaccount"]
                    },
                    {
                        "$set": {
                            "approved": "pending",
                            "is_matched": False,
                            "record_matched_id": None,
                        }
                    }
                )
                db["bank_statement"].update_many(
                    {
                         "transaction_date": { "$gte": rep_name["date_period_from"], "$lte": rep_name["date_period_to"] },
                         "account_number": rep_name["account_number"]
                    },
                    {
                        "$set": {
                            "approved": "pending",
                            "is_matched": False,
                            "record_matched_id": None,
                        }
                    }
                )
                carryovers = db["carryover"].find_one(
                    {"account_number": rep_name["account_number"],
                     "subaccount": rep_name["subaccount"]}
                )
                
                # carryover_gl_ids = [ObjectId(co["entry_id_gl"]) for co in carryovers["carryover_gl"]]
                # carryover_bs_ids = [ObjectId(co["entry_id_bs"]) for co in carryovers["carryover_bs"]]
                
                db["general_ledger"].update_many(
                    { "_id": {"$in": carryovers["carryover_gl"]} },
                    {
                        "$set": {
                            "approved": "approved",
                            "is_matched": False,
                            "record_matched_id": None,
                        }
                    }
                )
                db["bank_statement"].update_many(
                    { "_id": {"$in": carryovers["carryover_bs"]} },
                    {
                        "$set": {
                            "approved": "approved",
                            "is_matched": False,
                            "record_matched_id": None,
                        }
                    }
                )
                    
                db["reports_database"].delete_one({"report_name": rep_name["report_name"]})
                db["upload_list"].update_many(
                    {"reports": {"$elemMatch": {"report_name": rep_name["report_name"]}}},
                    {"$pull": {"reports": {"report_name": rep_name["report_name"]}}},
                )
                deleted_docs = {
                    "src_collection": "reports",
                    "deleted_documents": [rep_name],
                    "remarks": request.POST.get("remarks"),
                    "user": str(request.user),
                    "deleted_at": datetime.now()
                }
                db["deleted_documents"].insert_one(deleted_docs)
                
                to_trail = {
                    "report": rep_name,
                    "deleted_docs": deleted_docs
                }
                trail = db_mo.connect("trail", "trail_recon")
                trail.insert_one(
                    {
                        "user": str(request.user),
                        "action": "deleting recon report @ del_report",
                        "action_type": "delete",
                        "company": str(request.session["company_code"]),
                        "time": datetime.now(),
                        "actions": [to_trail],
                    }
                )
                
        elif request.POST.get("collection") == "cash_flow_report":
            res = [
                db[request.POST.get("collection")].delete_one({"_id": ObjectId(id)})
                for id in report_ids
            ]

        return True
    except Exception as e:
        messages.error(request, f"Deleting report failed. {e}")
        return False


def push_records_to_check_monitoring(request, collection, db):
    target_collection = db[f"check_monitoring_{collection}"]
    target_collection.insert_many(request.POST.getlist("report"))


def fetch_one_document(db, collection: str, query: dict):
    return db[collection].find_one(query)


def update_document(db, collection: str, query: dict, set_statement: dict):
    return db[collection].update_one(query, set_statement)


def fetch_one_document_with_2_query(db, collection: str, query1: dict, query2: dict):
    return db[collection].find_one(query1, query2)

    # fetch_one_document_with_2_query(db, collection, query1,query2)
    # db[collection].find_one(query1, query2)


def fetch_bulk_documents(db, collection: str, query: dict):
    return list(db[collection].find(query))


def apply_index(db, collection: str, field: str):
    db[collection].create_index(field, unique=True)


def filtered_to_unmatched(gl, bs, report_id, db):
    requests = [
        pymongo.UpdateOne(
            {"report_id": report_id}, {"$push": {"unmatched_records_source": gl}}
        ),
        pymongo.UpdateOne(
            {"report_id": report_id}, {"$push": {"unmatched_records_target": bs}}
        ),
        pymongo.UpdateOne(
            {"report_id": report_id},
            {
                "$pull": {
                    "filtered_match": {"entry_id_gl": ObjectId(gl.get("entry_id_gl"))}
                }
            },
        ),
    ]

    try:
        db["reports_database"].bulk_write(requests)
    except pymongo.errors.BulkWriteError as bwe:
        pprint("ERROR: ", bwe.details)


def get_standard_recon_keyword(keyword):
    db = db_mo.ref_db("bank_recon_field_keywords")
    crsr = db["field_keywords"].find({"keywords": keyword})
    return [i for i in crsr][0]
