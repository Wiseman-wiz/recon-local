from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from app import context_styling as ctx
from app.controllers.main_logic.Crumble.Crumb import Crumb
from .controllers import Features, DbOps

context_styling = ctx.context_styles.context_styling
db_mo = DbOps.MainOps()


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def view_features_admin(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    # company_code = request.session["company_code"]
    # db = db_mo.ref_db(f"{company_code}_bank_recon")
    if not request.session["company_code"]:
        return redirect("logout")
    context["company_code"] = request.session["company_code"]
    #  no need to change db for now
    context["features"] = Features.get_features()
    html_template = loader.get_template("features/admin.html")
    return HttpResponse(html_template.render(context, request))
