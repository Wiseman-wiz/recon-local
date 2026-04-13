from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from app import context_styling as ctx
from app.controllers.main_logic.Crumble.Crumb import Crumb
from app.forms import bank_recon_filter as forms
from pprint import pprint

# from django.shortcuts import redirect
from django.contrib import messages
from ..controllers import (
    Approvals as AP,
    MatchReports as MR,
    DbOps,
)
from authentication.decorators import allowed_users


context_styling = ctx.context_styles.context_styling
db_mo = DbOps.MainOps()


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def match_reports(request, collection):
    context = {}
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    if request.method == "POST":
        list_dict = []
        list = request.POST.getlist("report[]")
        for i in list:
            list_dict.append(i)
        return HttpResponseRedirect(request.path_info)

    data = MR.get_specific_report(collection, db)
    context["collection"] = collection
    context["report_name"] = data["report_name"].replace("_", " ")
    context["target"] = data["target"].replace("_", " ")
    context["source"] = data["source"].replace("_", " ")
    context["matched_records"] = data["matched_records"]
    context["matched_fields"] = data["matched_fields"]
    context["unmatched_records_source"] = data["unmatched_records_source"]
    context["unmatched_records_target"] = data["unmatched_records_target"]
    context["summary_report"] = data["summary_report"]
    context["filter_qry_src"] = data["filter_qry_src"]
    context["filter_qry_trg"] = data["filter_qry_trg"]
    context["merge_matched_list"] = data["merge_matched_list"]
    html_template = loader.get_template(
        "bank_recon/reports/match_reports/match_reports.html"
    )
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def view_bank_recon_match_reports_main(request):
    context = {}
    print("START")
    merge(context, context_styling)
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()

    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context["company_code"] = company_code
    # context["reports"] = MR.get_reports() #
    if request.method == "POST":
        second_level = False
        if all([request.POST.get("var_source"), request.POST.get("var_target")]):
            var_source = request.POST.get("var_source")
            # pprint(var_source)
            var_target = request.POST.get("var_target")
            if all(
                [
                    forms.QueryForm(
                        request.POST,
                        source=var_source,
                        target=var_target,
                        database1=db,
                        database2=db,
                        prefix="form_2",
                    ),
                    forms.FilterForm(
                        request.POST, source=var_source, database=db, prefix="form_3"
                    ),
                    forms.FilterForm(
                        request.POST, source=var_target, database=db, prefix="form_4"
                    ),
                    forms.ReportForm(
                        request.POST,
                        prefix="form_5",
                    ),
                ]
            ):
                form_data_2 = forms.QueryForm(
                    request.POST,
                    source=var_source,
                    target=var_target,
                    database1=db,
                    database2=db,
                    prefix="form_2",
                )
                form_data_3 = forms.FilterForm(
                    request.POST, source=var_source, database=db, prefix="form_3"
                )
                form_data_4 = forms.FilterForm(
                    request.POST, source=var_target, database=db, prefix="form_4"
                )
                form_data_5 = forms.ReportForm(request.POST, prefix="form_5")
                if all(
                    [
                        form_data_2.is_valid(),
                        form_data_3.is_valid(),
                        form_data_4.is_valid(),
                        form_data_5.is_valid(),
                    ]
                ):
                    match_fields = {
                        key.replace(" ", "_"): val
                        for (key, val) in form_data_2.cleaned_data.items()
                    }
                    dict_query = {
                        "source": var_source,
                        "target": var_target,
                        "filter_source": form_data_3.cleaned_data,
                        "filter_target": form_data_4.cleaned_data,
                        "match": match_fields,
                        "report_name": " ".join(
                            form_data_5.cleaned_data["report_name"].lower().split()
                        ).replace(" ", "_"),
                        "date_period_from": form_data_5.cleaned_data[
                            "date_period_from"
                        ],
                        "date_period_to": form_data_5.cleaned_data["date_period_to"],
                        "approved": "pending",
                    }
                    MR.push_data("match_reports", dict_query, db)
                    MR.push_report_to_upload_list(dict_query, db)
                    # MR.check_field_names(match_fields, db)

        if forms.SimpleForm(request.POST, database=db, prefix="form_1"):
            form_data = forms.SimpleForm(request.POST, database=db, prefix="form_1")
            if form_data.is_valid():
                var_source = form_data.cleaned_data["source"]
                var_target = form_data.cleaned_data["target"]
                context["source_data"] = MR.get_one_record(var_source, db)
                context["target_data"] = MR.get_one_record(var_target, db)
                context["source"] = var_source
                context["target"] = var_target
                context["form_2"] = forms.QueryForm(
                    source=var_source,
                    target=var_target,
                    database1=db,
                    database2=db,
                    prefix="form_2",
                )
                context["form_3"] = forms.FilterForm(
                    source=var_source, prefix="form_3", database=db
                )
                context["form_4"] = forms.FilterForm(
                    source=var_target, prefix="form_4", database=db
                )
                context["form_5"] = forms.ReportForm(
                    prefix="form_5",
                )

                html_template = loader.get_template(
                    "bank_recon/reports/match_reports/match_reports_main.html"
                )
                return HttpResponse(html_template.render(context, request))
    context["form_1"] = forms.SimpleForm(prefix="form_1", database=db)
    # pprint(MR.get_all_reports())
    reports = MR.get_all_reports(db)

    context["approved"] = []
    context["not_approved"] = []
    context["pending"] = []

    for report in reports:
        for r_key, r_val in report.items():
            if r_key == "approved":
                context["approved"].append(r_val)
            elif r_key == "not_approved":
                context["not_approved"].append(r_val)
            elif r_key == "pending":
                context["pending"].append(r_val)

    html_template = loader.get_template(
        "bank_recon/reports/match_reports/match_reports_main.html"
    )
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
@allowed_users(allowed_roles=["finance_admin", "reporters", "approvers"])
def view_redirect_match_report(request):
    context = {}
        
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")

    if request.method == "POST":
        form_2 = forms.QueryForm(request.POST, database=db, prefix="form_2")
        form_3 = forms.FilterForm(request.POST, database=db, prefix="form_3")
        form_4 = forms.ReportForm(request.POST, database=db, prefix="form_4")

        if form_2.is_valid() and form_3.is_valid() and form_4.is_valid():
            pprint(form_2)  # why?
            pprint(form_3)  # why?
            pprint(form_4)  # why?
    response = redirect("/bank-recon-match-reports-main/")
    return response


@login_required(login_url="/login/")
def view_bank_recon_match_reports_posts(request):
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    if request.method == "POST":
        if "btn_delete" in request.POST:
            if MR.del_report(request, db):
                messages.success(request, "Successfully deleted the report.")

        elif "btn_approval" in request.POST:
            if AP.set_approval(request, request.POST.get("collection"), db):
                messages.success(request, "Successfully updated report status.")

    response = redirect(request.POST.get("redirect_to"))
    return response
