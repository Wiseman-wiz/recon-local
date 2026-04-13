from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from django.shortcuts import redirect
from django.template import loader
from django.views.decorators.csrf import csrf_exempt

from app import context_styling as ctx
from app.controllers.main_logic.Crumble.Crumb import Crumb
from .controllers import (
    BankRecon as BR,
    Approvals as AP,
    Uploader as UP,
    DbOps,
)


db_mo = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def bank_recon_transactions_main(request):
    context = {}
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    context["transactions"] = BR.get_banks(db)
    html_template = loader.get_template(
        "bank_recon/transactions/main_transactions.html"
    )
    return HttpResponse(html_template.render(context, request))


# @login_required(login_url="/login/")
# def bank_recon_transactions(request, collection):
#     context = {}
    
    # crumble = Crumb()
    # context["crumble"] = crumble.get_crumble_html()
    
#     merge(context, context_styling)
#     context["transactions"] = BR.get_transactions(collection)
#     context["collection"] = collection
#     html_template = loader.get_template(
#         "bank_recon/transactions/transactions.html")
#     return HttpResponse(html_template.render(context, request))


@csrf_exempt
@login_required(login_url="/login/")
def bank_recon_transactions(request):  # complexity 8
    context = {}

    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    collection = "main_bank_statement"

    if request.method == "POST":
        if "btn_approval" in request.POST:
            if AP.set_approval(request, collection, db):
                messages.success(request, "Successfully updated record status.")
            else:
                messages.error(request, "Failed updating record status.")
        else:
            UP.upload_record_data(request, collection)
        return HttpResponseRedirect(request.path_info)
    merge(context, context_styling)
    """
        case when user change collection to a non-existing
        collection will still proceed.
        func below should be rename for general use.
    """
    cheque_data = BR.get_transactions(collection, db)
    context["approved"] = []
    context["pending"] = []
    context["rejected"] = []
    for data in cheque_data:
        if data["approved"] is True:
            context["approved"].append(data)
        elif data["approved"] is False:
            context["rejected"].append(data)
        elif data["approved"] == "pending":
            context["pending"].append(data)

    context["collection"] = collection
    html_template = loader.get_template("bank_recon/transactions/transactions.html")
    return HttpResponse(html_template.render(context, request))
