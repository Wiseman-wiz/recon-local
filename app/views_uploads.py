import csv
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from django.views.decorators.csrf import csrf_exempt

from app.controllers.main_logic.Crumble.Crumb import Crumb
from .controllers import BankRecon as br, Uploader as up
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
@allowed_users(allowed_roles=["finance_admin", "uploaders"])
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
            response = up.upload_file(request)  # unused
        return HttpResponseRedirect(request.path_info)
    merge(context, context_styling)
    context["collections"] = gr.group_by_collection_name(br.get_collections(db))
    html_template = loader.get_template("bank_recon/upload/upload.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
@allowed_users(allowed_roles=["finance_admin", "uploaders"])
def bank_recon_export_csv(request, filename=None):
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    response = HttpResponse(content_type="text/csv")
    try:
        response["Content-Disposition"] = (
            'attachment; filename="' + filename.replace(" ", "_") + '.csv"'
        )

        csv_headers = br.get_keys(filename.replace(" ", "_"), db)
        csv_headers.remove("_id")
        csv_headers.remove("date_modified")

        writer = csv.writer(response)
        writer.writerow(csv_headers)

        return response

    except Exception:
        return response
