from pprint import pprint
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from .controllers import (
    BankRecon as br,
    Uploader as up,
    Records as rc,
    Approvals as ap,
    DbOps,
)
from .controllers.helpers import Group as gr
from app import context_styling as ctx
from app.controllers.main_logic.Crumble.Crumb import Crumb


context_styling = ctx.context_styles.context_styling
db_mo = DbOps.MainOps()


def merge(d1, d2):
    return d1.update(d2)


@csrf_exempt
@login_required(login_url="/login/")
def bank_recon_records_main(request):
    context = {}
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    if request.method == "POST":
        response = rc.create_record(request, db)
        return HttpResponseRedirect(request.path_info)
    merge(context, context_styling)
    context["grouped_records"] = gr.group_by_collection_name(br.get_collections(db))
    html_template = loader.get_template("bank_recon/records/main_records.html")
    return HttpResponse(html_template.render(context, request))


@csrf_exempt
@login_required(login_url="/login/")
def bank_recon_records(request, collection):  # complexity 8
    context = {}
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    if request.method == "POST":
        if "btn_approval" in request.POST:
            if ap.set_approval(request, collection, db):
                messages.success(request, "Successfully updated record status.")
            else:
                messages.error(request, "Failed updating record status.")
        else:
            response = up.upload_record_data(request, collection)  # unused
        return HttpResponseRedirect(request.path_info)
    merge(context, context_styling)
    """
        case when user change collection to a non-existing
        collection will still proceed.
        func below should be rename for general use.
    """
    records_data = br.get_transactions(collection.replace("-", "_"), db)
    context["approved"] = []
    context["pending"] = []
    context["rejected"] = []
    for data in records_data:
        if data["approved"] is True:
            context["approved"].append(data)
        elif data["approved"] is False:
            context["rejected"].append(data)
        elif data["approved"] == "pending":
            context["pending"].append(data)

    context["collection"] = collection.replace("-", " ")
    html_template = loader.get_template("bank_recon/records/records.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def delete_record(request):
    context = {}
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    if request.method == "POST":
        if rc.del_record(request, db):
            messages.success(request, "Successfully deleted the record.")
        else:
            messages.error(
                request, "Deleting Record failed. Record name is not in upload list."
            )

    response = redirect("/bank-recon-records-main/")
    return response


@csrf_exempt
@login_required(login_url="/login/")
def bank_recon_records_list(request):
    context = {}
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    # implement the server-side pagination for this.
    context["collection_list"] = br.get_collections(db)
    html_template = loader.get_template("bank_recon/records/records_list.html")
    return HttpResponse(html_template.render(context, request))
