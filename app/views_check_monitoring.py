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
    CheckMonitoring as CM,
    Approvals as AP,
    Uploader as UP,
    DbOps,
)


context_styling = ctx.context_styles.context_styling
db_mo = DbOps.MainOps()


def merge(d1, d2):
    return d1.update(d2)


#  old code to be removed when not needed anymore.
@login_required(login_url="/login/")
def bank_recon_check_monitoring_main(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    context["check_monitoring"] = CM.get_checks(db)
    html_template = loader.get_template(
        "bank_recon/check_monitoring/main_check_monitoring.html"
    )
    return HttpResponse(html_template.render(context, request))


@csrf_exempt
@login_required(login_url="/login/")
def bank_recon_check_monitoring(request):  # complexity 8
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    collection = "check_monitoring"
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
    html_template = loader.get_template(
        "bank_recon/check_monitoring/check_monitoring.html"
    )
    return HttpResponse(html_template.render(context, request))
