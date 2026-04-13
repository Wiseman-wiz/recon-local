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


def get_one_record(collection: str, db) -> dict:
    return dict(
        db[collection].find_one({}, {"_id": 0, "date_modified": 0, "approved": 0})
    )

def merge_all_gl():
    db = db_mo.use_database("BDC_bank_recon")
    collections = db.list_collections()
    