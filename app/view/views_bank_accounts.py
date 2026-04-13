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


db_conn = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def bank_recon_bank_accounts_main(request):
    context = {}
    
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if request.method == "POST":
        if "btn_add" in request.POST:
            if BA.create_bank_account(request):
                messages.success(request, "Successfully created a new Bank Account.")
        elif "btn_update" in request.POST:
            if BA.update_bank_account(request):
                messages.success(request, "Successfully updated a Bank Account.")
        elif "btn_del" in request.POST:
            if BA.delete_bank_account(request):
                messages.success(request, "Successfully deleted a Bank Account.")
        return HttpResponseRedirect(request.path_info)
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    context["company_code"] = request.session["company_code"]
    context["form_1"] = brf.BankAccountsForm(prefix="form_1")
    context["account"] = BA.get_bank_accounts(context["company_code"])
    html_template = loader.get_template("bank_recon/accounts/accounts.html")
    return HttpResponse(html_template.render(context, request))
