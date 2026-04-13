import collections
from operator import itemgetter
from bson.objectid import ObjectId
from django.contrib import messages
from pprint import pprint
from datetime import datetime as dt
from . import DbOps, BankRecon as br

# client = DbOps.MainOps()
# db = client.ref_db("bank_recon")


def create_comparative_cfr(request, db):
    if db["cfr_references"].find().count() == 0:
        messages.error(
            request, "CFR References are needed. Upload the references and try again."
        )
        return False

    dict_struct = {
        "report_name": request.POST.get("form_1-report_name"),
        "cfr_ref": request.POST.get("form_1-cfr_ref"),
        "period": {
            "from": request.POST.get("form_1-period_from"),
            "to": request.POST.get("form_1-period_to"),
        },
        "target_records": request.POST.get("form_1-records"),
        "approved": "pending",
        "date_created": dt.now().strftime("%m/%d/%Y %H:%M:%S"),
    }

    try:
        db["cash_flow_report"].insert_one(dict_struct)

        return True

    except Exception as e:
        messages.error(request, "Creating New Comparative CFR Failed")
        pprint(repr(e))
        return False


def create_bank_balance_summary(request, db):
    dict_struct = {
        "report_name": request.POST.get("form_1-report_name"),
        "bank_account_info": request.POST.get("form_1-bank_account_info"),
        "bank_statement_col": request.POST.get("form_1-bank_statement_col"),
        "bank_statement_disb": request.POST.get("form_1-bank_statement_disb"),
        "report_period": request.POST.get("form_1-report_period"),
        "approved": "pending",
        "date_created": dt.now().strftime("%m/%d/%Y %H:%M:%S"),
    }

    try:
        db["bank_balance_summary"].insert_one(dict_struct)
        return True

    except Exception as e:
        messages.error(request, "Creating New Bank Balance Summary Failed")
        pprint(repr(e))
        return False


def get_comparative_cfr_list(db):
    qry_data = list(
        db["cash_flow_report"]
        .find({}, {"_id": 1, "report_name": 1, "approved": 1})
        .sort([("_id", -1), ("report_name", 1)])
    )

    dict_struct = {}
    approved, pending, rejected = [], [], []

    if qry_data:
        for data in qry_data:
            if data["approved"] is True:
                approved.append(
                    {
                        "report_id": str(data["_id"]),
                        "report_name": data["report_name"],
                        "approved": data["approved"],
                    }
                )
            elif data["approved"] == "pending":
                pending.append(
                    {
                        "report_id": str(data["_id"]),
                        "report_name": data["report_name"],
                        "approved": data["approved"],
                    }
                )
            elif data["approved"] is False:
                rejected.append(
                    {
                        "report_id": str(data["_id"]),
                        "report_name": data["report_name"],
                        "approved": data["approved"],
                    }
                )

        dict_struct["approved"], dict_struct["pending"], dict_struct["rejected"] = (
            approved,
            pending,
            rejected,
        )

    # pprint(dict_struct)
    return dict_struct


def get_cfr(db):
    return_data = {}
    qry_res = db["cfr_gl"].aggregate(
        [
            {"$match": {"approved": True}},
            {
                "$group": {
                    "_id": {
                        "ref_code_1": "$ref_1",
                        "ref_code_2": "$ref_2",
                        "month": {"$month": "$trndate"},
                        "year": {"$year": "$trndate"},
                    },
                    "total_amt": {"$sum": {"$subtract": ["$dr_amt", "$cr_amt"]}},
                }
            },
            {
                "$lookup": {
                    "from": "cfr_references",
                    "localField": "_id.ref_code_1",
                    "foreignField": "ref_code",
                    "as": "cfr_ref_docs",
                }
            },
            {
                "$project": {
                    "ref_1": 1,
                    "ref_2": 1,
                    "dr_amt": 1,
                    "cr_amt": 1,
                    "trndate": 1,
                    "cfr_ref_docs.ref_name": 1,
                    "total_amt": 1,
                }
            },
            {"$sort": {"_id": 1, "trndate": 1}},
        ]
    )

    qry_res_total = db["cfr_gl"].aggregate(
        [
            {"$match": {"approved": True}},
            {
                "$group": {
                    "_id": {
                        "month": {"$month": "$trndate"},
                        "year": {"$year": "$trndate"},
                    },
                    "net_total_amt": {"$sum": {"$subtract": ["$dr_amt", "$cr_amt"]}},
                }
            },
            {"$sort": {"_id": 1}},
        ]
    )

    ccfr_data = []
    for data in qry_res:
        for key, val in data.items():
            if key == "_id":
                dict_struct = {
                    "month": val["month"],
                    "year": val["year"],
                    "ref_code_1": val["ref_code_1"],
                    "ref_code_2": val["ref_code_2"],
                }
            elif key == "total_amt":
                if val < 0:
                    dict_struct["total_amt"] = f"({'{:,.2f}'.format(abs(val))})"
                else:
                    dict_struct["total_amt"] = "{:,.2f}".format(val)
            elif key == "cfr_ref_docs":
                dict_struct["item"] = [doc["ref_name"] for doc in val]
        ccfr_data.append(dict_struct)

    # pprint(ccfr_data)

    mon_yr_list = []
    mon_yr = []
    ccfr_total_data = []
    for data in qry_res_total:
        for key, val in data.items():
            if key == "_id":
                mon_yr.append({"month": val["month"], "year": val["year"]})
                mon_yr_list.append(f'{str(val["month"]).zfill(2)}-{val["year"]}')
                dict_struct = {"month": val["month"], "year": val["year"]}
            else:
                if val < 0:
                    dict_struct["net_total_amt"] = f"({'{:,.2f}'.format(abs(val))})"
                else:
                    dict_struct["net_total_amt"] = "{:,.2f}".format(val)
        ccfr_total_data.append(dict_struct)

    cfr_total_by_mon = []
    for obj_1, obj_2 in zip(ccfr_total_data, mon_yr_list):
        dict_list = {"cfr_total": obj_1.get("net_total_amt"), "mon_yr": obj_2}
        cfr_total_by_mon.append(dict_list)

    return_data["ccfr"] = ccfr_data
    return_data["ccfr_total"] = ccfr_total_data
    return_data["month_year_period"] = mon_yr_list
    return_data["cfr_total_by_mon"] = cfr_total_by_mon

    return return_data


def get_comparative_cfr(collection_id, db):
    return_data = {}
    qry_data = db["cash_flow_report"].find_one({"_id": ObjectId(collection_id)})
    pprint(qry_data)
    qry_res = db[qry_data["target_records"]].aggregate(
        [
            {
                "$match": {
                    "trndate": {
                        "$gte": dt.strptime(qry_data["period"]["from"], "%m/%d/%Y"),
                        "$lte": dt.strptime(qry_data["period"]["to"], "%m/%d/%Y"),
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "ref_code_1": "$ref_1",
                        "ref_code_2": "$ref_2",
                        "month": {"$month": "$trndate"},
                        "year": {"$year": "$trndate"},
                    },
                    "total_amt": {"$sum": {"$subtract": ["$dr_amt", "$cr_amt"]}},
                }
            },
            {
                "$lookup": {
                    "from": qry_data["cfr_ref"],
                    "localField": "_id.ref_code_1",
                    "foreignField": "ref_code",
                    "as": "cfr_ref_docs",
                }
            },
            {
                "$project": {
                    "ref_1": 1,
                    "ref_2": 1,
                    "dr_amt": 1,
                    "cr_amt": 1,
                    "trndate": 1,
                    "cfr_ref_docs.ref_name": 1,
                    "total_amt": 1,
                }
            },
            {"$sort": {"_id": 1, "trndate": 1}},
        ]
    )

    qry_res_total = db[qry_data["target_records"]].aggregate(
        [
            {
                "$match": {
                    "trndate": {
                        "$gte": dt.strptime(qry_data["period"]["from"], "%m/%d/%Y"),
                        "$lte": dt.strptime(qry_data["period"]["to"], "%m/%d/%Y"),
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "month": {"$month": "$trndate"},
                        "year": {"$year": "$trndate"},
                    },
                    "net_total_amt": {"$sum": {"$subtract": ["$dr_amt", "$cr_amt"]}},
                }
            },
            {"$sort": {"_id": 1}},
        ]
    )

    ccfr_data = []
    for data in qry_res:
        for key, val in data.items():
            if key == "_id":
                dict_struct = {
                    "month": val["month"],
                    "year": val["year"],
                    "ref_code_1": val["ref_code_1"],
                    "ref_code_2": val["ref_code_2"],
                }
            elif key == "total_amt":
                if val < 0:
                    dict_struct["total_amt"] = f"({'{:,.2f}'.format(abs(val))})"
                else:
                    dict_struct["total_amt"] = "{:,.2f}".format(val)
            elif key == "cfr_ref_docs":
                dict_struct["item"] = [doc["ref_name"] for doc in val]
        ccfr_data.append(dict_struct)

    # pprint(ccfr_data)

    mon_yr_list = []
    mon_yr = []
    ccfr_total_data = []
    for data in qry_res_total:
        # pprint(data)
        for key, val in data.items():
            if key == "_id":
                mon_yr.append({"month": val["month"], "year": val["year"]})
                mon_yr_list.append(f'{str(val["month"]).zfill(2)}-{val["year"]}')
                dict_struct = {"month": val["month"], "year": val["year"]}
            else:
                if val < 0:
                    dict_struct["net_total_amt"] = f"({'{:,.2f}'.format(abs(val))})"
                else:
                    dict_struct["net_total_amt"] = "{:,.2f}".format(val)
        ccfr_total_data.append(dict_struct)

    return_data["cfr"] = qry_data
    return_data["ccfr"] = ccfr_data
    return_data["ccfr_total"] = ccfr_total_data
    return_data["month_year_period"] = mon_yr_list

    return return_data


def get_cfr_references(collection, db):
    return list(data for data in db[collection].find({}))


def get_detailed_cfr_by_month(collection_id, month_year, db):
    return_data = {}
    qry_data = db["cash_flow_report"].find_one({"_id": ObjectId(collection_id)})
    qry_refs = list(data for data in db[qry_data.get("cfr_ref")].find({}))
    qry_ref_1_data = list(
        db[qry_data.get("cfr_ref")].find({"ref_type": "transaction_category"})
    )
    qry_ref_2_data = list(
        db[qry_data.get("cfr_ref")].find({"ref_type": "process_type"})
    )
    period = month_year.split("-")
    qry_res = db[qry_data["target_records"]].aggregate(
        [
            {
                "$lookup": {
                    "from": qry_data["cfr_ref"],
                    "localField": "ref_1",
                    "foreignField": "ref_code",
                    "as": "cfr_ref_docs",
                }
            },
            {
                "$project": {
                    "ref_1": 1,
                    "ref_2": 1,
                    "dr_amt": 1,
                    "cr_amt": 1,
                    "month": {"$month": "$trndate"},
                    "cfr_ref_docs": 1,
                    "total_amt": 1,
                }
            },
            {"$match": {"month": int(period[0])}},
            {"$sort": {"_id": 1, "trndate": 1}},
        ]
    )

    dict_struct = {}
    cfr_mon_data = []
    for doc in qry_res:
        cfr_mon_data.append(doc)
        if doc.get("ref_1") in dict_struct:
            if doc.get("ref_2") not in dict_struct[doc.get("ref_1")]:
                dict_struct[doc.get("ref_1")].update({doc.get("ref_2"): 0})
        else:
            dict_struct[doc.get("ref_1")] = {doc.get("ref_2"): 0}
        dict_struct[doc.get("ref_1")][doc.get("ref_2")] += float(
            doc.get("dr_amt")
        ) - float(doc.get("cr_amt"))

    total_amts = {}
    for ref_1, dict_data in dict_struct.items():
        total_amt = 0
        for ref_2, val in dict_data.items():
            dict_struct[ref_1][ref_2] = "{0:,.2f}".format(val / 1000)
            total_amt += val / 1000
            if val < 0:
                dict_struct[ref_1][ref_2] = f'({"{0:,.2f}".format(abs(val / 1000))})'
        if total_amt > 0:
            total_amts[ref_1] = "{0:,.2f}".format(total_amt)
        else:
            total_amts[ref_1] = f'({"{0:,.2f}".format(abs(total_amt))})'

    for ref_1 in qry_ref_1_data:
        for ref_2 in qry_ref_2_data:
            if ref_1.get("ref_code") in dict_struct:
                if not dict_struct[ref_1.get("ref_code")].get(ref_2.get("ref_code")):
                    dict_struct[ref_1.get("ref_code")].update(
                        {ref_2.get("ref_code"): 0}
                    )
            else:
                dict_struct[ref_1.get("ref_code")] = {ref_2.get("ref_code"): 0}
        if ref_1.get("ref_code") not in total_amts:
            total_amts.update({ref_1.get("ref_code"): "0"})

    # pprint(total_amts)
    return_data["cashflows"] = collections.OrderedDict(sorted(dict_struct.items()))
    return_data["cfr_details"] = qry_data
    return_data["cfr_monthly_data"] = cfr_mon_data
    return_data["cfr_monthly_total_data"] = collections.OrderedDict(
        sorted(total_amts.items())
    )
    return_data["cfr_refs"] = sorted(qry_refs, key=itemgetter("ref_code"))
    return return_data


def get_cfr_by_month(month_year, db):
    return_data = {}
    qry_refs = list(data for data in db["cfr_references"].find({}))
    qry_ref_1_data = list(
        db["cfr_references"].find({"ref_type": "transaction_category"})
    )
    qry_ref_2_data = list(db["cfr_references"].find({"ref_type": "process_type"}))
    period = month_year.split("-")
    qry_res = db["cfr_gl"].aggregate(
        [
            {
                "$lookup": {
                    "from": "cfr_references",
                    "localField": "ref_1",
                    "foreignField": "ref_code",
                    "as": "cfr_ref_docs",
                }
            },
            {
                "$project": {
                    "ref_1": 1,
                    "ref_2": 1,
                    "dr_amt": 1,
                    "cr_amt": 1,
                    "month": {"$month": "$trndate"},
                    "cfr_ref_docs": 1,
                    "total_amt": 1,
                }
            },
            {"$match": {"month": int(period[0])}},
            {"$sort": {"_id": 1, "trndate": 1}},
        ]
    )

    dict_struct = {}
    cfr_mon_data = []
    for doc in qry_res:
        cfr_mon_data.append(doc)
        if doc.get("ref_1") in dict_struct:
            if doc.get("ref_2") not in dict_struct[doc.get("ref_1")]:
                dict_struct[doc.get("ref_1")].update({doc.get("ref_2"): 0})
        else:
            dict_struct[doc.get("ref_1")] = {doc.get("ref_2"): 0}
        dict_struct[doc.get("ref_1")][doc.get("ref_2")] += float(
            doc.get("dr_amt")
        ) - float(doc.get("cr_amt"))

    sub_total_amts = {}
    total_amts = {}
    excluded_refs = ["N/A", "JE", "FT"]
    for ref_1, dict_data in dict_struct.items():
        total_amt = 0
        for ref_2, val in dict_data.items():
            dict_struct[ref_1][ref_2] = "{0:,.2f}".format(val / 1000)
            total_amt += val / 1000
            if not any(ex_ref in ref_1 for ex_ref in excluded_refs):
                if ref_1.split("-")[0] not in sub_total_amts:
                    sub_total_amts[ref_1.split("-")[0]] = 0
                sub_total_amts[ref_1.split("-")[0]] += val / 1000
            if val < 0:
                dict_struct[ref_1][ref_2] = f'({"{0:,.2f}".format(abs(val / 1000))})'
        if total_amt > 0:
            total_amts[ref_1] = "{0:,.2f}".format(total_amt)
        else:
            total_amts[ref_1] = f'({"{0:,.2f}".format(abs(total_amt))})'

    for k, v in sub_total_amts.items():
        if v < 0:
            sub_total_amts[k] = f'({"{0:,.2f}".format(abs(v))})'
        else:
            sub_total_amts[k] = "{0:,.2f}".format(v)
    pprint(sub_total_amts)

    for ref_1 in qry_ref_1_data:
        for ref_2 in qry_ref_2_data:
            if ref_1.get("ref_code") in dict_struct:
                if not dict_struct[ref_1.get("ref_code")].get(ref_2.get("ref_code")):
                    dict_struct[ref_1.get("ref_code")].update(
                        {ref_2.get("ref_code"): 0}
                    )
            else:
                dict_struct[ref_1.get("ref_code")] = {ref_2.get("ref_code"): 0}
        if ref_1.get("ref_code") not in total_amts:
            total_amts.update({ref_1.get("ref_code"): "0"})

    # pprint(total_amts)
    return_data["cashflows"] = collections.OrderedDict(sorted(dict_struct.items()))
    # return_data["cfr_details"] = qry_data
    return_data["cfr_monthly_data"] = cfr_mon_data
    return_data["cfr_monthly_total_data"] = collections.OrderedDict(
        sorted(total_amts.items())
    )
    return_data["cfr_monthly_sub_total_data"] = sub_total_amts
    return_data["cfr_refs"] = sorted(qry_refs, key=itemgetter("ref_code"))
    return return_data


def get_bank_balances_summary_list(db):
    qry_data = list(
        db["bank_balance_summary"].find({}).sort([("_id", -1), ("report_name", 1)])
    )

    dict_struct = {}
    approved, pending, rejected = [], [], []

    if qry_data:
        for data in qry_data:
            if data["approved"] is True:
                approved.append(
                    {
                        "report_id": str(data["_id"]),
                        "report_name": data["report_name"],
                        "approved": data["approved"],
                    }
                )
            elif data["approved"] == "pending":
                pending.append(
                    {
                        "report_id": str(data["_id"]),
                        "report_name": data["report_name"],
                        "approved": data["approved"],
                    }
                )
            elif data["approved"] is False:
                rejected.append(
                    {
                        "report_id": str(data["_id"]),
                        "report_name": data["report_name"],
                        "approved": data["approved"],
                    }
                )

        dict_struct["approved"], dict_struct["pending"], dict_struct["rejected"] = (
            approved,
            pending,
            rejected,
        )

    # pprint(dict_struct)
    return dict_struct


def get_bank_balances_summary(collection_id, db):
    return_data = {}
    qry_bank_statement = db["bank_balance_summary"].find_one(
        {"_id": ObjectId(collection_id)}
    )
    qry_bank_acct_info = list(db[qry_bank_statement["bank_account_info"]].find({}))

    qry_unadjusted_disb = db[qry_bank_statement["bank_statement_disb"]].aggregate(
        [
            {
                "$match": {
                    "transaction_date": {
                        "$lte": dt.strptime(
                            qry_bank_statement["report_period"], "%m/%d/%Y"
                        )
                    }
                }
            },
            {"$sort": {"transaction_date": -1}},
            {"$limit": 1},
        ]
    )

    qry_unadjusted_col = db[qry_bank_statement["bank_statement_col"]].aggregate(
        [
            {
                "$match": {
                    "transaction_date": {"$lte": qry_bank_statement["report_period"]}
                }
            },
            {"$sort": {"transaction_date": -1}},
            {"$limit": 1},
        ]
    )

    return_data["bank_bal"] = qry_bank_statement
    return_data["bank_acct_info"] = qry_bank_acct_info
    return_data["unadjusted_disb"] = list(qry_unadjusted_disb)
    return_data["unadjusted_col"] = list(qry_unadjusted_col)
    # pprint(return_data["unadjusted_disb"])
    # pprint(return_data["unadjusted_col"])
    # pprint(dt.strptime(qry_bank_statement["report_period"],"%m/%d/%Y"))
    return return_data


"""

CFR 04 = {
    bank_acct_info: select box > gets period date and acct_num
    bank_statment_collection
    bank_statment_disbursement
    date_created
}

"""
