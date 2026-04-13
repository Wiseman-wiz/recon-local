import os
import time
import csv
import re
import subprocess
import math
from io import open
from django.contrib import messages
from pprint import pprint
from . import DbOps, BankRecon as br
from datetime import date, datetime, timedelta
from uuid import uuid4
from django.core.files.storage import default_storage
from .helpers.aje_export_module import (
    populate_entries as pe,
    check_updated_entries as cue
)
from app.controllers.helpers import Company


def reupload_aje(request):
    file = request.FILES['file_reupload']
    file_name = request.FILES['file_reupload'].name
    try:
        path = r'/home/admin/apps/bank_recon/development/recon/uploadfiles/AJE/'
        default_storage.save(path + file_name, file)
        company_code = Company.get_company_code(request)
        up_check = cue.Upload_Checker(file_name, company_code)
        res = up_check.get_pass_fail_list()

        # pprint(res)
    except Exception as e:
        return messages.error(
            request,
            'Failed reuploading AJE. '
            + f"{e}",
        )