from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from app.controllers import BankAccounts as BA, DbOps
from app.controllers.main_logic.Crumble.Crumb import Crumb
from app.forms import bank_recon_forms as brf

# from .controllers.helpers import Group as gr
from app import context_styling as ctx
from .BankAccountsModule.BankAccounts import BankAccount as BA
from .ReportsModule.Reports import Report as RP
from .ReportGenerationModule.ReportsGeneration import ReportGeneration as RG

bank_accounts = BA()
reports = RP()
db_conn = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling

@login_required(login_url="/login/")
def bank_recon_v3_main(request):
    context = {}
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    get_html_template = loader.get_template("bank_recon_v3/recon_v3_index.html")
    context["company_code"] = request.session["company_code"]
    context["user"]=request.user
    context["template_name"] = "bank_recon_v3/recon_v3_bank_accounts.html"


    ba = bank_accounts.get_all(["account_number","subaccount"])
    tables = []
    
    tables.append({
        "headers":ba[0].keys(),
        "data":ba
    })
    context["tables"] = tables
    context["page"] = "Reports"
    
    return HttpResponse(get_html_template.render(context, request))

@login_required(login_url="/login/")
def bank_recon_reports(request,accounts):
    context = {}
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    get_html_template = loader.get_template("bank_recon_v3/recon_v3_index.html")
    context["company_code"] = request.session["company_code"]
    context["user"]=request.user

    context["page"] = f"Reports {accounts}"
    ba = bank_accounts.get_one(accounts)
    account_number = ba.get("account_number")
    sub_account_number = ba.get("subaccount")
    context["account_details"] = ba    
    
    sample_bs = reports.get_sample_bs(account_number)
    sample_gl = reports.get_sample_gl(sub_account_number)

    context["sample_accounts"] = {
        "sample_bs":sample_bs,
        "sample_gl":sample_gl
    }
    
    context["accounts_fileds_form"] = {
        "bs_fields":reports.get_bs_fields(account_number),
        "gl_fields":reports.get_gl_fields(sub_account_number)
    }

    context["min_max_date_range"] = {
        "bs_date_range":reports.get_daterange_bs(account_number),
        "gl_date_range":reports.get_daterange_gl(sub_account_number)
    }
    context["account"]= accounts

    reports_list = reports.get_reports_list(account_number)
    print(reports_list)
    context["reports_list"] = None
    if reports_list[0]:
        context["reports_list"] = reports_list[1]
    context["message"] = None
    if request.method == 'POST':
        print(request.POST)
        report_name = request.POST.get("report_name")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        message = reports.create_report(report_name,accounts,start_date,end_date,account_number,sub_account_number)
        
        context["message"] = message[1]
        
        return HttpResponse(get_html_template.render(context, request))

    return HttpResponse(get_html_template.render(context, request))



@login_required(login_url="/login/")
def bank_recon_reports_generate(request,report):
    context = {}
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    get_html_template = loader.get_template("bank_recon_v3/recon_v3_index.html")
    context["company_code"] = request.session["company_code"]
    context["user"]=request.user

    report_generator = RG(report_id=report)
    context["initial_data"] = report_generator.get_initial_report_data()
    reports_unformat = report_generator.one_to_one_matches()
    # for report in reports_unformat:
    context["reports"] = reports_unformat

    return HttpResponse(get_html_template.render(context, request))

