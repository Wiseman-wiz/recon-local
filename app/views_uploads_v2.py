import csv
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from django.views.decorators.csrf import csrf_exempt

from app.controllers.main_logic.Crumble.Crumb import Crumb
from .controllers import BankRecon as br, revamp_upload as up
from .controllers.helpers import Group as gr
from app import context_styling as ctx
from authentication.decorators import allowed_users
from app.controllers import DbOps


db_mo = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling


def merge(d1, d2):
    return d1.update(d2)


@csrf_exempt
@login_required(login_url="/login/")
@allowed_users(allowed_roles=["finance_admin", "processor", "approvers"])
def bank_recon_upload(request):
    context = {}
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    if request.method == "POST":
        if "file_access" in request.POST:
            response = up.upload_access_file(request)  # unused
        elif "file_csv" in request.POST:
            response = up.upload_file_v2(request)  # unused
        return HttpResponseRedirect(request.path_info)
    
    merge(context, context_styling)
    context["collections"] = br.get_accounts_v2(db)
    context["general_ledger_data"] = br.get_general_ledger(db)
    context["dataase_per_account"] = br.get_accounts_data_v2(db)

    html_template = loader.get_template("bank_recon/upload/uploads_v2.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
@allowed_users(allowed_roles=["finance_admin", "processor", "approvers"])
def bank_recon_export_csv(request, filename=None):
    if not request.session["company_code"]:
        return redirect("logout")
    
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="' + filename + '_TEMPLATE.csv"'
    
    bs_fields = [
        "TRANSACTION_DATE", "CHECK_NUMBER", "TRANSACTION_DESCRIPTION", 
        "DEBIT_AMOUNT", "CREDIT_AMOUNT", "NET", "TRANSACTION_REFERENCE", 
        "FROM", "TO", "ACCOUNT_NUMBER"
    ]

    gl_fields = [
        "TRNDATE", "TRNNO", "SUBACCT", "OTHER_01", "OTHER_03", 
        "DR_AMT", "CR_AMT", "NET", "FROM", "TO", "REF_1", "REF_2","CHECK_DATE","CHECK_NUMBER"
    ]
    
    if filename == "GENERAL_LEDGER":
        csv_headers = gl_fields
    elif filename == "BANK_STATEMENT":
        csv_headers = bs_fields
        
    writer = csv.writer(response)
    writer.writerow(csv_headers)

    return response

