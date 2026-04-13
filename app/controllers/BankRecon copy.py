import timeit
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


db_mo = DbOps.MainOps()
db_bank_recon = "bank_recon"
col_accounts = "bank_accounts"
col_bs1 = "bank_statement_format_1"
col_bs2 = "bank_statement_format_2"
col_gl = "general_ledger"
bank_account_collection = "bank_account"
# src_bank_accounts = client.connect(
#     database=db_bank_recon, collection=col_accounts)

def fct(s_time:str):
    df = pd.DataFrame()
    df[s_time] = pd.to_datetime(df[s_time])
    df[s_time] = df[s_time].astype(object).where(df[s_time].notnull(), None)
    return df


def get_one_record(collection: str, db) -> dict:
    return dict(
        db[collection].find_one({}, {"_id": 0, "date_modified": 0, "approved": 0})
    )


def get_specific_report(collection_id: str, db, request,date_start=None,date_end=None) -> dict:
    """Fetch a report from DB and process the 2 collections to merge and match.

    This function gets the selected report to filter out fields and to
    perform the matching of 2 collections (records) with computation
    of bank recon report. This uses helper functions which are:
    app/controllers/helpers/match_unmatch_segregate.py
        => Handles the matching and merging of 2 df collections and computing
        the bank recon report output.
    app/controllers/helpers/bank_account_module.py
        => Handles computation of beginning balance of specific bank account.

    ARGS
    ==========
        collection_id: Object id of selected report in reports collection
        db (pymongo.database.Database): MongoClient instance of selected DB
            base on company.
        request: Django build-in request for user session.

    RETURNS
    ==========
    dict => output of matched reports and its computations

    RAISES
    ==========

    TODO
    ==========
        1. complexity of the function
        2. deposit in transit dynamic computation
        3. levels of matching filter

    FIXME
    ==========
        get_monthly_adj_cash_bal(): should dynamically compute
            beginning balance base on date period of the report
    """
    # start = timeit.default_timer()
    # start = process_time()

    #  fetching the selected report from reports collection
    target_collection = db["reports"]
    target_return = target_collection.find_one({"_id": ObjectId(collection_id)})
    # match_level = 1

    query_data = get_report_filter(collection_id, db)
    if query_data:
        # pprint(target_return['report_name'])
        pprint(query_data.get("is_submitted"))
        # fetch report data here
        if query_data.get("is_submitted"):

            report = dict(
                db["reports_database"].find_one(
                    {"report_name": target_return["report_name"]}
                )
            )

            unmatched_records_source_df = ""
            unmatched_records_target_df = ""
            filtered_match_df = pd.DataFrame(report["filtered_match"])
            book_error_list_df = pd.DataFrame(report["book_error_list"])
            matched_records_df = pd.DataFrame(report["matched_records"])

            if not (report["unmatched_records_source"] and report["unmatched_records_target"]):
                unmatched_records_source_df = pd.DataFrame(columns = list(matched_records_df.columns))   
                unmatched_records_target_df = pd.DataFrame(columns = list(matched_records_df.columns)) 

            else:
                unmatched_records_source_df = pd.DataFrame(
                report["unmatched_records_source"]
                )
                unmatched_records_target_df = pd.DataFrame(
                    report["unmatched_records_target"]
                )

                     

            rep_calc = ReportCalculator(
                general_ledger_df=unmatched_records_source_df,
                bank_statement_df=unmatched_records_target_df,
                matched_df=matched_records_df,
                filtered_df=filtered_match_df,
                book_errors_df=book_error_list_df,
                to_match=report["matched_fields"],
            )

            # fetching amounts of bank recon report
            adjusted_cash_bal = report["adjusted_cash_bal"]
            credit_memo = rep_calc.credit_memo_amount
            debit_memo = rep_calc.debit_memo_amount
            reversal_of_aje = rep_calc.reversal_of_aje_amount
            stale_checks = rep_calc.stale_checks_amount
            outstanding_checks = rep_calc.outstanding_checks_amount
            # computation of deposit in transit will base previous month of report
            deposit_in_transit = rep_calc.deposit_on_transit_amount
            book_errors = rep_calc.book_errors_amt
            total_gl_dr_amt = report["total_gl_dr_amt"]
            total_gl_cr_amt = report["total_gl_cr_amt"]
            total_bs_dr_amt = report["total_bs_dr_amt"]
            total_bs_cr_amt = report["total_bs_cr_amt"]
            total_gl_movement = report["total_gl_movement"]
            total_bs_movement = report["total_bs_movement"]

            total_gl_aje = (
                reversal_of_aje
                + stale_checks
                + outstanding_checks
                + debit_memo
                + credit_memo
                + book_errors
            )

            total_gl_fin_bal = sum([adjusted_cash_bal, total_gl_movement, total_gl_aje])
            total_pb_fin_bal = sum(
                [adjusted_cash_bal, total_bs_movement, deposit_in_transit]
            )

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
                "total_gl_fin_bal": total_gl_fin_bal,
                "total_pb_fin_bal": total_pb_fin_bal,
            }

            db["reports_database"].update_one(
                {"report_name": target_return["report_name"]},
                {"$set": updated_report_calc},
                upsert=True,
            )

        return dict(
            db["reports_database"].find_one(
                {"report_name": target_return["report_name"]}
            )
        )

    #  iterating the fields to filter of 2 collections to match and merge
    filter_qry_src = {}
    filter_qry_src["approved"]=1
    for key, value in target_return["filter_source"].items():
        if value != 0 and key != "":
            filter_qry_src[key] = int(value)
    filter_qry_trg = {}
    filter_qry_trg["approved"]=1
    for key, value in target_return["filter_target"].items():
        if value != 0 and key != "":
            filter_qry_trg[key] = int(value)

    #  fetching documents of 2 collections to match and merge
    if filter_qry_src or filter_qry_trg:
        report_source = list(db["general_ledger"].find({
            "subacct": target_return["subaccount"]}
        ))
        report_target = list(db["bank_statement"].find({
            "account_number": target_return["account_number"] }
        ))
    else:
        report_source = list(db["general_ledger"].find({
            "subacct": target_return["subaccount"]
        }))
        report_target = list(db["bank_statement"].find({
            "account_number": target_return["account_number"]
        }))
        

    unmatched_records_source = []
    unmatched_records_target = []
    selected_match_fields = {}


    print(report_target[0])
    # iterating to get the selected match fields
    for key in target_return["match"]:
        if target_return["match"][key] != "":
            selected_match_fields[key] = target_return["match"][key]

    # maybe oneliner
    for obj in report_source:
        obj["entry_id"] = obj.pop("_id")
    for obj in report_target:
        obj["entry_id"] = obj.pop("_id")

    report_filter = []

    source_df = pd.DataFrame(report_source)
    target_df = pd.DataFrame(report_target)
    filtered_df = pd.DataFrame(report_filter)

    # initializing instance of Segregator class to merge and match the
    # collection and calculate the bank recon report amounts.
    seg = Segregator(
        source_df,
        target_df,
        filtered_df,
        selected_match_fields,
        target_return["date_period_from"],
        target_return["date_period_to"],
    )

    # storing matched records in DB
    to_insert_reports = {
        "report_name": target_return["report_name"],
        "matched_report": seg.match,
        "unmatched_gl_report": seg.in_gl_no_bs,
        "unmatched_bs_report": seg.in_bs_no_gl,
        "book_errors": seg.book_errors,
    }


    db[f"reports_{target_return['report_name']}"].remove({})

    try:
        print(to_insert_reports)
        db[f"reports_{target_return['report_name']}"].insert_one(to_insert_reports)
    except pymongo.errors.DuplicateKeyError:
        # skip document because it already exists in new collection
        pass

    company_code = request.session["company_code"]
    db_conn = DbOps.MainOps()
    try:
        # computes the beginning cash balance of bank accounts
        adjusted_cash_bal = get_monthly_adj_cash_bal(report_target, company_code)
    except:
        raise Exception(
            "Unregistered Bank Account Number found. Please add the \
             account number to the system and try accessing the report again."
        )

    # fetching amounts of bank recon report
    credit_memo = seg.credit_memo_amount
    debit_memo = seg.debit_memo_amount
    reversal_of_aje = seg.reversal_of_aje_amount
    stale_checks = seg.stale_checks_amount
    outstanding_checks = seg.outstanding_checks_amount
    # computation of deposit in transit will based on previous month of report
    deposit_in_transit = seg.deposit_on_transit_amount
    book_errors = seg.book_errors_amt
    total_gl_dr_amt = seg.total_dr_cr_amts["gl_debit_total_amt"]
    total_gl_cr_amt = seg.total_dr_cr_amts["gl_credit_total_amt"]
    total_bs_dr_amt = seg.total_dr_cr_amts["bs_debit_total_amt"]
    total_bs_cr_amt = seg.total_dr_cr_amts["bs_credit_total_amt"]
    total_gl_movement = seg.total_dr_cr_amts["gl_total_cash_movement"]
    total_bs_movement = seg.total_dr_cr_amts["bs_total_cash_movement"]
    credit_memo = seg.credit_memo_amount

    total_gl_aje = (
        reversal_of_aje
        + stale_checks
        + outstanding_checks
        + debit_memo
        + credit_memo
        + book_errors
    )

    total_gl_fin_bal = sum([adjusted_cash_bal, total_gl_movement, total_gl_aje])
    total_pb_fin_bal = sum([adjusted_cash_bal, total_bs_movement, deposit_in_transit])

    # creates a random filename and creates sheet for downloading unmatched
    # list for processing AJE
    print( target_return["source"])
    print( target_return["target"])
    if "GL" in target_return["source"].upper() or ("GENERAL_LEDGER" in target_return["source"].upper()):
        data = unmatched_records_source
        type_of_list = "GL"
        filename = rsg.RandomString_Generator.get_random_filename(type_of_list)
        pe.create_sheet(filename, data, type_of_list, company_code)
    elif "PB" in target_return["target"].upper() or ("BANK_STATEMENT" in target_return["target"].upper() ):
        data = unmatched_records_target
        type_of_list = "PB"
        filename = rsg.RandomString_Generator.get_random_filename(type_of_list)
        pe.create_sheet(filename, data, type_of_list, company_code)
    else:
        messages.error(request, "Some error happen with type")
        raise Exception

    conn3 = db_conn.ref_db(f"{company_code}_AJE_records")
    # fetches uploaded AJE draft
    recon_aje_list = list(conn3["gl_aje_draft"].find({"recon": 1}))
    for aje in recon_aje_list:
        aje["_id"] = str(aje["_id"])
        aje["original_id"] = str(aje["original_id"])

    final_data = {
        "report_name": target_return["report_name"],
        "report_id": collection_id,
        "source": target_return["source"],
        "target": target_return["target"],
        # "match_level": match_level,
        "date_period_from": target_return["date_period_from"].strftime("%m/%d/%Y"),
        "date_period_to": target_return["date_period_to"].strftime("%m/%d/%Y"),
        "matched_records": seg.match,
        "matched_fields": selected_match_fields,
        # "matched_2_fields": selected_match_2_fields,
        # "matched_3_fields": selected_match_3_fields,
        "unmatched_records_source": seg.in_gl_no_bs,
        "unmatched_records_target": seg.in_bs_no_gl,
        "filter_qry_src": target_return["filter_source"],
        "filter_qry_trg": target_return["filter_target"],
        "adjusted_cash_bal": adjusted_cash_bal,
        "total_gl_dr_amt": total_gl_dr_amt,
        "total_gl_cr_amt": -total_gl_cr_amt,
        "total_bs_dr_amt": -total_bs_dr_amt,
        "total_bs_cr_amt": total_bs_cr_amt,
        "total_gl_movement": total_gl_movement,
        "total_bs_movement": total_bs_movement,
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
        # "new_match": seg.match,
        "book_error_list": seg.book_errors,
        # "level_2_match": seg.level_2_match_df,
        # "level_3_match": seg.level_3_match_df,
        "unmatched_bs_cols": seg.unmatched_bs_cols,
        "unmatched_gl_cols": seg.unmatched_gl_cols,
        "filtered_match": [],
    }

    test = db["reports_database"].update_one(
        {"report_name": target_return["report_name"]}, {"$set": final_data}, upsert=True
    )

    # end = timeit.default_timer()
    # end = process_time()
    # time_spd = end - start
    # pprint(f"CONTROLLER FUNC TIME => {time_spd} sec")

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
):
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


def fetch_and_push_to_matched(
    entry_bs_id: list, entry_gl_id: list, report_filter_id: str, db
):

    filter_fields = db["reports_filter"].find_one(
        {"_id": ObjectId(report_filter_id)}, {"filter_fields": 1}
    )
    # pprint(filter_fields)
    # cont here

    # convert list of ids to int
    entry_bs_id = list(map(int, entry_bs_id))
    entry_gl_id = list(map(int, entry_gl_id))

    # this can be improved
    # do find all fields by ids of each entries of gl and bs
    matched_entry = []
    records_bs_filter_cols = []
    records_gl_filter_cols = []
    if entry_bs_id and entry_gl_id:
        for bs_oid in entry_bs_id:
            records_bs_filter_cols = []
            bs_crsr = list(
                db["reports_filter"].find(
                    {"_id": ObjectId(report_filter_id)},
                    {
                        "unmatched_records_target": {
                            "$elemMatch": {"filter_bs_id": bs_oid}
                        }
                    },
                )
            )
            matched_dict_bs = {}
            for k, v in bs_crsr[0]["unmatched_records_target"][0].items():
                if k == "filter_bs_id":
                    matched_dict_bs["filter_bs_id"] = v
                    records_bs_filter_cols.append("filter_bs_id")
                    continue
                matched_dict_bs[k] = v
                records_bs_filter_cols.append(k)

            matched_dict_gl = {}
            for gl_oid in entry_gl_id:
                records_gl_filter_cols = []
                gl_crsr = list(
                    db["reports_filter"].find(
                        {"_id": ObjectId(report_filter_id)},
                        {
                            "unmatched_records_source": {
                                "$elemMatch": {"filter_gl_id": gl_oid}
                            }
                        },
                    )
                )
                for key, val in gl_crsr[0]["unmatched_records_source"][0].items():
                    if key == "filter_gl_id":
                        matched_dict_gl["filter_gl_id"] = val
                        records_gl_filter_cols.append("filter_gl_id")
                        continue
                    matched_dict_gl[key] = val
                    records_gl_filter_cols.append(key)

                for i, j in filter_fields["filter_fields"].items():
                    if matched_dict_gl[i] != matched_dict_bs[j]:
                        return {
                            "status": "failed",
                            "msg": f"{i} and {j} does not match.",
                        }

                matched_dict = {**matched_dict_bs, **matched_dict_gl}
                matched_dict[
                    "filter_id"
                ] = f'{matched_dict["filter_bs_id"]}_{matched_dict["filter_gl_id"]}'
                matched_entry.append(matched_dict)
                matched_dict = {}

        db["reports_filter"].update(
            {"_id": ObjectId(report_filter_id)},
            {
                "$set": {
                    "records_bs_filter_cols": records_bs_filter_cols,
                    "records_gl_filter_cols": records_gl_filter_cols,
                }
            },
            upsert=True,
        )

        # pprint(records_bs_filter_cols)
        # pprint(records_gl_filter_cols)
    else:
        return {
            "status": "failed",
            "msg": "select 1 entry of GL and BS then try again.",
        }

    try:
        for item in matched_entry:
            db["reports_filter"].update(
                {"_id": ObjectId(report_filter_id)},
                {"$addToSet": {"matched_records_filter": item}},
            )
    except Exception as e:
        # apply error msg here
        return {"status": "failed", "msg": "error in adding to matched entries"}

    try:
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


def set_unmatch_filter(matched_ids: list, report_filter_id: str, db):
    # pprint(report_filter_id)
    crsr = db["reports_filter"].find_one({"_id": ObjectId(report_filter_id)})
    # pprint(crsr)

    for f_id in matched_ids:
        for item in crsr["matched_records_filter"]:
            if item["filter_id"] == f_id:
                gl_dict = {}
                bs_dict = {}
                for col_key in crsr["records_bs_filter_cols"]:
                    bs_dict[col_key] = item[col_key]
                for col_key in crsr["records_gl_filter_cols"]:
                    gl_dict[col_key] = item[col_key]
                # pprint(type(gl_dict))
                # pprint(type(bs_dict))
                # make it shorter code
                db["reports_filter"].update(
                    {"_id": ObjectId(report_filter_id)},
                    {"$push": {"unmatched_records_source": gl_dict}},
                )
                db["reports_filter"].update(
                    {"_id": ObjectId(report_filter_id)},
                    {"$push": {"unmatched_records_target": bs_dict}},
                )
                db["reports_filter"].update(
                    {"_id": ObjectId(report_filter_id)},
                    {"$pull": {"matched_records_filter": {"filter_id": f_id}}},
                )

    return {"status": "success", "msg": "successfully updated the lists"}


def merge_filter_reports(report_coll_id: str, report_id: str, filter_id: str, db):

    crsr = get_report_filter(report_id, db)

    for item in crsr.get("matched_records_filter"):
        # adding to filtered_match list each entry
        try:
            db["reports_database"].update(
                {"_id": ObjectId(report_coll_id)},
                {"$addToSet": {"filtered_match": item}},
            )
        except:
            return {"status": "failed", "msg": "error in adding filtered matches"}

        # removes filtered matched entries to unmatched lists
        try:
            # pprint('Deleting..')
            # pprint(report_coll_id)
            # pprint("---"*10)
            # pprint(item.get("entry_id_gl"))
            # pprint(item.get("entry_id_bs"))
            db["reports_database"].update(
                {"_id": ObjectId(report_coll_id)},
                {
                    "$pull": {
                        "unmatched_records_source": {
                            "entry_id_gl": ObjectId(item.get("entry_id_gl"))
                        },
                        "unmatched_records_target": {
                            "entry_id_bs": ObjectId(item.get("entry_id_bs"))
                        },
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
    res = db["reports_filter"].update(
        {"_id": ObjectId(filter_id)}, {"$set": {"is_submitted": True}}, upsert=True
    )
    # pprint(res)
    # pprint("dahek?")
    return {"status": "success", "msg": "successfully merged to filtered matches"}


def merge_unmatched_reports(
    unmatched_bs: dict, unmatched_gl: dict, filter_fields: dict
):
    bs_df = pd.DataFrame(unmatched_bs)
    gl_df = pd.DataFrame(unmatched_gl)

    bs_df_cols = bs_df.columns.tolist()
    gl_df_cols = gl_df.columns.tolist()

    bs_filter = list(filter_fields.values())
    gl_filter = list(filter_fields.keys())

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

    try:
        bs_match_df = match_both_df[bs_df_cols]
    except:
        # to handle cols with same name that have suffix now.
        bs_with_suffix_cols = [i for i in match_both_cols if "_x" in i]
        i = 0
        for j in bs_df_cols:
            if f"{j}_x" in bs_with_suffix_cols:
                bs_df_cols[i] = f"{j}_x"
            i += 1
        # pprint(bs_df_cols)
        bs_match_df = match_both_df[bs_df_cols]
    bs_match_df = bs_match_df.drop_duplicates(subset=["entry_id_bs"])

    try:
        gl_match_df = match_both_df[gl_df_cols]
    except:
        # to handle cols with same name that have suffix now.
        gl_with_suffix_cols = [i for i in match_both_cols if "_y" in i]
        i = 0
        for j in gl_df_cols:
            if f"{j}_y" in gl_with_suffix_cols:
                gl_df_cols[i] = f"{j}_y"
            i += 1
        # pprint(gl_df_cols)
        gl_match_df = match_both_df[gl_df_cols]
    gl_match_df = gl_match_df.drop_duplicates(subset=["entry_id_gl"])

    return bs_match_df.to_dict("records"), gl_match_df.to_dict("records")


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


def get_collections(db)->list:
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
    )

def get_general_ledger(db)->list:
    list_collection = list(db.list_collection_names(0))
    import re
    print(list_collection)
    r = re.compile(".*_gl_.*")
    return list(filter(r.match,list_collection))

def get_accounts_v2(db):
    account_list = list(x.get("account_number") for x in db["bank_accounts"].find({},{"account_number":1}))
    return account_list

def get_accounts_by_account_number(db, acct_no):
    return db["bank_accounts"].find_one(
        {"account_number": acct_no},
        {"subaccount":1, "account_number": 1})
    
def get_accounts_data_v2(db):
    list_collection = list(db.list_collection_names(0))
    final_list= {}
    account_list= [] 
    
    for x in list_collection:
        if "_pb_" in x:
            data = db[x].find_one()
            account = data.get("account_number","None")
            account_list.append(f'{account}')

    for account in account_list:
        final_list[account] = []

    for x in list_collection:
        if "_pb_" in x:
            final_list[account].append(x)
    
    print(final_list)
    return(final_list)


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

        if not report_ids:
            messages.error(request, "Please select report/s to delete and try again.")
            return False

        if request.POST.get("collection") == "reports":
            report_name_list = [
                db["reports"].find_one({"_id": ObjectId(report_id)}, {"report_name": 1})
                for report_id in report_ids
            ]

            for r_name in report_name_list:
                db[f'summary_reports_{r_name["report_name"]}'].drop()
                db[f'reports_{r_name["report_name"]}'].drop()
                db["reports"].delete_one({"report_name": r_name["report_name"]})

                db["upload_list"].update_many(
                    {"reports": {"$elemMatch": {"report_name": r_name["report_name"]}}},
                    {"$pull": {"reports": {"report_name": r_name["report_name"]}}},
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

def update_document(db,collection:str,query:dict,set_statement:dict):
    return db[collection].update_one(query,set_statement)


def fetch_one_document_with_2_query(db, collection: str, query1: dict, query2: dict):
    return db[collection].find_one(query1, query2)


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
