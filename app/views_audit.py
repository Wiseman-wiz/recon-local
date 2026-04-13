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
@allowed_users(allowed_roles=["finance_admin", "uploaders"])
def bank_recon_audit(request):
    context = {}
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.connect("trail","trail_recon")
    
    data = db.find({})
    execution = []
    for d in data:
        d["_id"] = str(d.get("_id"))
        d["actions"] = str(d.get("actions"))
        execution.append(d)
    
    context["company_code"] = company_code
    context["execution"] = execution
    html_template = loader.get_template("bank_recon/audit_trails.html")
    return HttpResponse(html_template.render(context, request))
