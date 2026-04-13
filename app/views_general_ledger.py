# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse

from app.controllers.main_logic.Crumble.Crumb import Crumb
from .controllers import BankRecon as BR, DbOps
from app import context_styling as ctx

context_styling = ctx.context_styles.context_styling
db_mo = DbOps.MainOps()


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def bank_recon_general_ledger_main(request):
    context = {}
    merge(context, context_styling)
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    context["general_ledger"] = BR.get_ledgers(db)
    html_template = loader.get_template(
        "bank_recon/general_ledger/main_general_ledger.html"
    )
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def bank_recon_general_ledger(request, collection):
    context = {}
    merge(context, context_styling)
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    context["general_ledger"] = BR.get_transactions(collection, db)
    context["collection"] = collection
    html_template = loader.get_template("bank_recon/general_ledger/general_ledger.html")
    return HttpResponse(html_template.render(context, request))
