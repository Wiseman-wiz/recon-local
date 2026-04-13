# import core python packages
import os, time, csv, re, subprocess, math

# import other modules
from io import open
from datetime import date, datetime, timedelta
from uuid import uuid4
from pprint import pprint

# import django core packages
from django.contrib import messages
from django.shortcuts import redirect
from django.core.files.storage import default_storage

# import app modules
from app.controllers.helpers import Company
from app.controllers import DbOps
from . import DbOps, BankRecon as br


DB_CONFIGS = {
    "gl_disbursement": "v2_gl_disbursement",
    "gl_collection": "v2_gl_disbursement",
    "consolidated_bs": "v2_gl_disbursement",
}

db_mo = DbOps.MainOps()

class UploaderModuleV2:

    def __init__(self):
        self.client = pymongo.MongoClient("localhost:27017")
        self.company_code = ""


    def use_database(self, request):
        user_id = request.user.id
        print(f'USER_ID => {user_id}')
        company_details = Company.objects.raw(f'SELECT * FROM app_usercompanyassignment where user_id={user_id}')
        self.company_code = company_details[0].company_id
        db_name = f"{self.company_code}_bank_recon"
        print(f'db_name => {db_name}')
        return self.client[db_name]


    def connect(self, database, collection):
        assert isinstance(database, str)
        assert isinstance(collection, str)
        conn = self.client[database][collection]

        return conn


    def ref_db(self, database):
        assert isinstance(database, str)

        return self.client[database]
