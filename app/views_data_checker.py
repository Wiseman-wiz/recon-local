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
import pymongo
# from .controllers.helpers import Group as gr
from app import context_styling as ctx


db_conn = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def bank_recon_data_checker(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    field_name = request.GET["field_name"]
    field_data = request.GET["field_data"]

    databases = db_conn.database_names()
    return HttpResponse(databases)