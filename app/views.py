# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
#django imports
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from django import template

#custom imports
from .controllers import BankRecon as br, Companies as cm
from app import context_styling as ctx
from app.controllers import DbOps
from app.controllers.helpers import Company
from app.forms import sms_system as sms_forms
from app.controllers import SmsOps
from app.controllers.main_logic.Crumble.Crumb import Crumb


db_conn = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def bank_recon_main(request):
    context = {}
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    context["company_code"] = request.session["company_code"]
    merge(context, context_styling)
    html_template = loader.get_template("bank_recon/main_bank_recon.html")
    return HttpResponse(html_template.render(context, request))


#  old account module
@login_required(login_url="/login/")
def bank_recon_accounts(request):
    context = {}
    merge(context, context_styling)
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    db = db_conn.use_database(request)
    context["company_code"] = db_conn.company_code
    context["account"] = br.get_accounts(db)
    html_template = loader.get_template("bank_recon/accounts/accounts.html")
    return HttpResponse(html_template.render(context, request))


#  static from template
@login_required(login_url="/login/")
def main_page(request):
    context = {}
    merge(context, context_styling)
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    html_template = loader.get_template("buttons.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def index(request):
    context = {}
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    groups = request.user.groups.all()
    list_group = list(groups)
    if "text_blast" in list_group:
        redirect("home2")
    merge(context, context_styling)

    context["segment"] = "index"
    if not request.session["company_code"]:
        return redirect("logout")
    context["company_code"] = request.session["company_code"]
    
    import app.controllers.test_main as tm
    data = tm.test_event_id()
    
    context["testing"] = {
        "Test Case 1: uploading.event_id": data
    }

    print(context)

    html_template = loader.get_template("index.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def pages(request):
    context = {}
    merge(context, context_styling)
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    # All resource paths end in .html.
    # Pick out the html file name from the url. And load that template.
    try:

        load_template = request.path.split("/")[-1]
        context["segment"] = load_template

        html_template = loader.get_template(load_template)
        return HttpResponse(html_template.render(context, request))

    except template.TemplateDoesNotExist:

        html_template = loader.get_template("page-404.html")
        return HttpResponse(html_template.render(context, request))

    except Exception:

        html_template = loader.get_template("page-500.html")
        return HttpResponse(html_template.render(context, request))


#  old code for changing company => del for code clean up
@login_required(login_url="/login/")
def bank_recon_change_company(request):
    db = db_conn.use_database(request)
    cm.change_company(request, db)

    return redirect(request.POST.get("redirect_path"))


@login_required(login_url="/login/")
def bank_recon_select_company(request):
    if request.method == "POST":
        if "selected_company" in request.POST:
            request.session["company_code"] = request.POST.get("selected_company")
            return redirect("/")
        else:
            return redirect("/bank-recon/select-company/")
    companies = Company.get_all_companies(request)
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    context["companies"] = companies
    html_template = loader.get_template("select-company.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def index_2(request):
    context = {}
    merge(context, context_styling)
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    context["create_template"] = sms_forms.create_template(prefix="create_form")
    context["source_data"] = {"test": "test"}
    template_table = SmsOps.get_data("get_templates")
    html_template = loader.get_template("index2.html")
    context["template_table"] = template_table
    context["numbers_table"] = SmsOps.get_data("get_numbers")

    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def data_handler_debug(request):
    context = {}
    return HttpResponse(request)


@login_required(login_url="/login")
def post_templates(request):
    if request.method == "POST":
        form = sms_forms.create_template(request.POST)
        template_name = form.data["create_form-template_name"]
        template_text = form.data["create_form-template_area"]
        SmsOps.post_data(template_name, template_text, "post_templates")
        return redirect("home2")


@login_required(login_url="/login")
def get_templates(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    data = SmsOps.get_data("get_templates")
    return HttpResponse(data)


@login_required(login_url="login")
def post_numbers(request):
    if request.method == "POST":
        form = sms_forms.create_template(request.POST)
        template_name = form.data["create_form-template_name"]
        template_text = form.data["create_form-template_area"]
        SmsOps.post_data(template_name, template_text, "post_numbers")
        return redirect("home2")
    return redirect("home2")


@login_required(login_url="/login")
def get_numbers(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    data = SmsOps.get_data("get_numbers")
    return HttpResponse(data)


@login_required(login_url="/login")
def post_campaign(request):
    if request.method == "POST":
        number_id = []
        template_id = str
        SmsOps.post_data(number_id, template_id, "post_campaign")
    return redirect("home2")



@login_required(login_url="/login/")
def bank_recon_data_checker(request,field_name,field_data):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    field_name = field_name
    field_data = field_data

    databases = db_conn.database_names()
    return HttpResponse(databases)
