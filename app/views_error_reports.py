from django.contrib.auth.decorators import login_required
from django.template import loader
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from django.views.decorators.csrf import csrf_exempt

# from .controllers import BankRecon as br, ErrorHandling as eh
# from app.forms import bank_recon_filter as forms
from app import context_styling as ctx
from app.controllers.helpers import Company
from app.controllers.main_logic.Crumble.Crumb import Crumb

context_styling = ctx.context_styles.context_styling


def merge(d1, d2):
    return d1.update(d2)


@csrf_exempt
@login_required(login_url="/login/")
def error_reports_main(request, collection):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    context["company_code"] = Company.get_company_code(request)
    if request.method == "POST":
        return HttpResponseRedirect(request.path_info)

    merge(context, context_styling)
    html_template = loader.get_template("shared/error_reports.html")
    return HttpResponse(html_template.render(context, request))
