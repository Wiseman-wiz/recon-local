from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from app import context_styling as ctx
from app.controllers.main_logic.Crumble.Crumb import Crumb
from app.forms import bank_recon_filter as forms
from pprint import pprint
from django.contrib import messages
from app.controllers import CashFlowReport as CFR, DbOps


db_mo = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def view_bank_recon_reports_comparative_cfr_main(request):  # complexity 8
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    merge(context, context_styling)

    if request.method == "POST":
        # to add validation
        if CFR.create_comparative_cfr(request, db):
            messages.success(request, "Successfully created the report.")

        return HttpResponseRedirect(request.path_info)

    context["form_1"] = forms.ComparativeCFRForm(prefix="form_1", database=db)

    data_fecthed = CFR.get_comparative_cfr_list(db)

    context["approved"], context["pending"], context["rejected"] = [], [], []

    if data_fecthed:
        for key, val in data_fecthed.items():
            if key == "approved":
                context["approved"] = val
            elif key == "pending":
                context["pending"] = val
            elif key == "rejected":
                context["rejected"] = val

        # pprint(context["pending"])
    html_template = loader.get_template(
        "bank_recon/reports/reports_comparative_cfr_main.html"
    )
    return HttpResponse(html_template.render(context, request))


# main CFR_GL
@login_required(login_url="/login/")
def view_bank_recon_reports_cfr(request):
    context = {}

    merge(context, context_styling)
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"bank_recon")  # {company_code}_
    context["company_code"] = company_code
    data_fetched = CFR.get_cfr(db)
    context["ccfr"] = data_fetched["ccfr"]
    context["ccfr_total"] = data_fetched["ccfr_total"]
    context["month_year_period"] = data_fetched["month_year_period"]
    context["cfr_total_by_mon"] = data_fetched["cfr_total_by_mon"]
    # pprint(context["cfr_total_by_mon"])

    html_template = loader.get_template("bank_recon/reports/reports_cfr.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def bank_recon_reports_comparative_cfr(request, collection):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"bank_recon")  # {company_code}_
    context["company_code"] = company_code
    data_fetched = CFR.get_comparative_cfr(collection, db)
    context["cfr"] = data_fetched["cfr"]
    context["ccfr"] = data_fetched["ccfr"]
    context["ccfr_total"] = data_fetched["ccfr_total"]
    context["month_year_period"] = data_fetched["month_year_period"]
    context["report"] = collection
    # context["month_year_period"] = data_fetched["month_year_period"]
    html_template = loader.get_template(
        "bank_recon/reports/reports_comparative_cfr.html"
    )
    return HttpResponse(html_template.render(context, request))


# do not remote yet
@login_required(login_url="/login/")
def bank_recon_reports_monthly_cfr(request, month_year, collection):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"bank_recon")  # {company_code}_
    context["company_code"] = company_code
    data_fetched = CFR.get_detailed_cfr_by_month(collection, month_year, db)
    context["cfr_refs"] = data_fetched["cfr_refs"]

    context["cfr_monthly_data"] = data_fetched["cfr_monthly_data"]
    context["cfr_monthly_total_data"] = data_fetched["cfr_monthly_total_data"]
    # pprint(context["cfr_monthly_total_data"])
    context["cfr_details"] = data_fetched["cfr_details"]
    context["cashflows"] = data_fetched["cashflows"]
    # pprint(context["cfr_ref"])
    period = month_year.split("-")
    context["report"] = collection
    context["month"] = period[0]
    context["year"] = period[1]
    # pprint(context["cashflows"])

    html_template = loader.get_template("bank_recon/reports/reports_monthly_cfr.html")
    return HttpResponse(html_template.render(context, request))


# main CFR_GL
@login_required(login_url="/login/")
def bank_recon_reports_cfr_monthly(request, month_year):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"bank_recon")  # {company_code}_
    context["company_code"] = company_code
    data_fetched = CFR.get_cfr_by_month(month_year, db)
    context["cfr_refs"] = data_fetched["cfr_refs"]
    context["cfr_monthly_data"] = data_fetched["cfr_monthly_data"]
    context["cfr_monthly_total_data"] = data_fetched["cfr_monthly_total_data"]
    context["cfr_monthly_sub_total_data"] = data_fetched["cfr_monthly_sub_total_data"]
    context["cashflows"] = data_fetched["cashflows"]
    period = month_year.split("-")
    context["month"] = period[0]
    context["year"] = period[1]

    html_template = loader.get_template("bank_recon/reports/reports_monthly_cfr.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def view_bank_recon_reports_cfr_references(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    

    html_template = loader.get_template(
        "bank_recon/reports/cfr/reports_cfr_references.html"
    )
    return HttpResponse(html_template.render(context, request))
