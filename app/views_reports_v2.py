# from django.shortcuts import redirect
import timeit
from time import process_time

# django imports core
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.template import loader
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect
from django.utils.safestring import SafeString
from django.contrib import messages

#app imports core
from app import context_styling as ctx
from app.controllers.main_logic.Crumble.Crumb import Crumb
from app.forms import bank_recon_filter as forms
from .controllers import (
    BankRecon as BR,
    AJE,
    Approvals as AP,
    CashFlowReport as CFR,
    DbOps
)
from .controllers.helpers.aje_export_module import (
    populate_entries as pe,
    check_updated_entries as cue
)
from app.models import Company

#lib imports core
from bson.objectid import ObjectId
from pprint import pprint

#initialization of db
db_mo = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling


def merge(d1:dict, d2:dict)->dict:
    '''
        Merge dictionaries
    '''
    return d1.update(d2)


@login_required(login_url="/login/")
def reports(request, collection: str):
    #start = timeit.default_timer()
    # start = process_time()
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    

    if not request.session["company_code"]:
        return redirect("logout")
    
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code

    if request.method == "POST":
        if "btn_undo" in request.POST:
            ids = list(i.split("_") for i in request.POST.getlist("report1[]"))
            report_data = BR.fetch_one_document(
                db,
                "reports",
                {"_id": ObjectId(collection)}
            )

            #fields to match
            selected_match_fields = {}
            for key in report_data["match"]:
                if report_data["match"][key] != '':
                    selected_match_fields[key] = report_data["match"][key]

            #id to upload
            for id in ids:
                gl_record = BR.fetch_one_document_with_2_query(
                    db,
                    report_data.get("source"),
                    {"_id": ObjectId(id[0])},
                    report_data.get("filter_source")
                )

                for key in selected_match_fields:
                    crsr = BR.get_standard_recon_keyword(key)
                    std_kw = crsr["field"]
                    if std_kw != "transaction_reference":
                        std_kw = crsr["field"] + "_gl"
                    gl_record[std_kw] = gl_record.pop(key)
                
                gl_record["entry_id_gl"] = gl_record.pop("_id")
                gl_record["ref_count_gl"] = 1

                bs_record = BR.fetch_one_document_with_2_query(
                    db,
                    report_data.get("target"),
                    {"_id": ObjectId(id[1])},
                    report_data.get("filter_target")
                )

                for key, val in selected_match_fields.items():
                    crsr = BR.get_standard_recon_keyword(val)
                    std_kw = crsr["field"]
                    if std_kw != "transaction_reference":
                        std_kw = crsr["field"] + "_bs"
                    bs_record[std_kw] = bs_record.pop(val)

                bs_record["entry_id_bs"] = bs_record.pop("_id")
                bs_record["ref_count_bs"] = 1

                # res = BR.filtered_to_unmatched(gl_record, bs_record, collection, db)

        return HttpResponseRedirect(request.path_info)
            
        # list_dict = []
        # list = request.POST.getlist("report[]")
        # for i in list:
        #     list_dict.append(i)
        # pprint(list_dict)
        # BR.push_records_to_chec   k_monitoring(request, collection)
        
    # try:
    date_start=None 
    date_end=None
    data = BR.get_specific_report(collection, db, request,date_start, date_end)

    report_filter_data = BR.get_report_filter(data["report_id"], db)
    # except Exception as e:
    #     messages.error(request, 'Failed to view the report. ERROR: ' + repr(e))
    #     return redirect("/bank-recon-reports-main/")
    
    context["filter_form"] = forms.ReportFilterForm(
        unmatched_gl_cols=data["unmatched_gl_cols"],
        unmatched_bs_cols=data["unmatched_bs_cols"],
        prefix="filter_form",
    )
    
    context["collection_id"] = collection
    context["report_coll_id"] = data["_id"]
    context["report_filter_data"] = report_filter_data
    context["filtered_match"] = data["filtered_match"]
    context["filename"] = data["filename"]
    context["report_name"] = data["report_name"].replace('_', ' ')
    context["target"] = data["target"].replace('_', ' ')
    context["source"] = data["source"].replace('_', ' ')
    context["date_period_from"] = data["date_period_from"]
    context["date_period_to"] = data["date_period_to"]
    context["matched_records"] = data["matched_records"]
    context["matched_fields"] = data["matched_fields"]
    context["unmatched_records_source"] = data["unmatched_records_source"]
    context["unmatched_records_target"] = data["unmatched_records_target"]
    context["book_error_list"] = data["book_error_list"]
    # context["summary_report"] = data["summary_report"]
    context["filter_qry_src"] = data["filter_qry_src"]
    context["filter_qry_trg"] = data["filter_qry_trg"]
    context["adjusted_cash_bal"] = "{0:,.2f}".format(data["adjusted_cash_bal"])
    context["total_gl_dr_amt"] = "{0:,.2f}".format(data["total_gl_dr_amt"])
    context["total_gl_cr_amt"] = "{0:,.2f}".format(data["total_gl_cr_amt"])
    context["total_bs_dr_amt"] = "{0:,.2f}".format(data["total_bs_dr_amt"])
    context["total_bs_cr_amt"] = "{0:,.2f}".format(data["total_bs_cr_amt"])
    context["total_gl_movement"] = "{0:,.2f}".format(data["total_gl_movement"])
    context["total_bs_movement"] = "{0:,.2f}".format(data["total_bs_movement"])
    context["credit_memo"] = "{0:,.2f}".format(data["credit_memo"])
    context["debit_memo"] = "{0:,.2f}".format(data["debit_memo"])
    context["reversal_of_aje"] = "{0:,.2f}".format(data["reversal_of_aje"])
    context["stale_checks"] = "{0:,.2f}".format(data["stale_checks"])
    context["outstanding_checks"] = "{0:,.2f}".format(
        data["outstanding_checks"])
    context["deposit_in_transit"] = "{0:,.2f}".format(
        data["deposit_in_transit"])
    context["book_errors"] = "{0:,.2f}".format(data["book_errors"])
    context["total_gl_aje"] = "{0:,.2f}".format(data["total_gl_aje"])
    context["total_gl_fin_bal"] = "{0:,.2f}".format(data["total_gl_fin_bal"])
    context["total_pb_fin_bal"] = "{0:,.2f}".format(data["total_pb_fin_bal"])
    context["recon_aje_list"] = data["recon_aje_list"]
    html_template = loader.get_template("bank_recon/reports/reports.html")

    end = timeit.default_timer()
    # end = process_time()
    #time_spd = end - start
    #pprint(f"VIEWS FUNC TIME => {time_spd} sec")

    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def filter_reports(request, report_coll_id: str):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code

    report_db_data, report_data = BR.get_reports_database(report_coll_id, db)

    if request.method == "POST":
        if request.POST.get('btn_is_match'):

            res = BR.fetch_and_push_to_matched(
                entry_bs_id=request.POST.getlist("entry_bs"),
                entry_gl_id=request.POST.getlist("entry_gl"),
                report_filter_id=request.POST.get("report_filter_id"),
                db=db
            )

            if res.get("status") == "failed":
                messages.error(request, res.get("msg"))

            return redirect("filter-reports", report_coll_id=report_coll_id)

        if request.POST.get('btn_unmatch'):
            print(request.POST.getlist("matched_entry"))
            res = BR.set_unmatch_filter(
                matched_ids=request.POST.getlist("matched_entry"),
                report_filter_id=request.POST.get("filter_id"),  # check naming
                db=db             
            )
            return redirect("filter-reports", report_coll_id=report_coll_id)

        if request.POST.get('btn_save_filter'):
            res = BR.merge_filter_reports(
                report_coll_id=report_coll_id,
                report_id=report_data["_id"],
                filter_id=request.POST.get("filter_id"),
                db=db
            )
            return redirect("bank-recon-reports",
                             collection=request.POST.get("report_id"))

        form_data = forms.ReportFilterForm(
            request.POST,
            unmatched_gl_cols=report_db_data["unmatched_gl_cols"],
            unmatched_bs_cols=report_db_data["unmatched_bs_cols"],
            prefix="filter_form",
        )

        if form_data.is_valid():
            selected_filter_fields = {}

            for key in form_data.cleaned_data:
                if form_data.cleaned_data[key] != '':
                    selected_filter_fields[key] = form_data.cleaned_data[key]
            
            bs_filtered, gl_filtered, selected_filter_fields = BR.merge_unmatched_reports(
                unmatched_bs=report_db_data["unmatched_records_target"],
                unmatched_gl=report_db_data["unmatched_records_source"],
                filter_fields=selected_filter_fields
            )

            # temp solution to put unique id for each transaction due to 
            # nature of data of users (distinctness).
            i = 0
            for row_dict in bs_filtered:
                row_dict["filter_bs_id"] = i
                i += 1
            i = 0
            for row_dict in gl_filtered:
                row_dict["filter_gl_id"] = i
                i += 1
            
            res = BR.create_report_filter(
                report_data["_id"],
                selected_filter_fields,
                gl_filtered,
                bs_filtered,
                db
            )

            return redirect("filter-reports", report_coll_id=report_coll_id)

    report_filter_data = BR.get_report_filter(report_data["_id"], db)
    context["report_name"] = report_db_data["report_name"]
    context["report_coll_id"] = report_coll_id
    context["report_id"] = report_data["_id"]
    context["report_filter_id"] = str(report_filter_data.get("_id"))
    context["unmatched_records_source"] = report_filter_data.get("unmatched_records_source")
    context["unmatched_records_target"] = report_filter_data.get("unmatched_records_target")
    context["matched_records_filter"] = report_filter_data.get("matched_records_filter")
    context["unmatched_records_source_filter"] = report_filter_data.get("unmatched_records_source_filter")
    context["unmatched_records_target_filter"] = report_filter_data.get("unmatched_records_target_filter")
    html_template = loader.get_template("bank_recon/reports/reports_filter_match.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="login/")
def view_bank_balance(request):
    # old function
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]

    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code
    context["reports"] = "" # BR.get_all_reports(db)
    context["form_1"] = forms.SimpleForm2(prefix="form_1", database=db)

    if request.method == 'POST':
        html_template = loader.get_template("bank_recon/reports/recon2.html")
        return HttpResponse(html_template.render(context, request))

    html_template = loader.get_template("bank_recon/reports/recon.html")
    return HttpResponse(html_template.render(context, request))



@login_required(login_url="/login/")
def view_bank_recon_reports_main_2(request):  # complexity 12
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code
    # context["reports"] = BR.get_reports()
    if request.method == 'POST':
        # second_level = False
        if all([request.POST.get("var_source"),
                request.POST.get("var_target")]):
            var_source = request.POST.get("var_source")
            # pprint(var_source)
            var_target = request.POST.get("var_target")
            if all(
                [
                    forms.QueryForm(
                        request.POST,
                        source=var_source,
                        target=var_target,
                        prefix="form_2",
                        database1=db,
                        database2=db,
                    ),
                    forms.FilterForm(
                        request.POST,
                        source=var_source,
                        prefix="form_3",
                        database=db
                    ),
                    forms.FilterForm(
                        request.POST,
                        source=var_target,
                        prefix="form_4",
                        database=db
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
                    prefix="form_2",
                    database1=db,
                    database2=db,
                )
                form_data_3 = forms.FilterForm(
                    request.POST,
                    source=var_source,
                    prefix="form_3",
                    database=db
                )
                form_data_4 = forms.FilterForm(
                    request.POST,
                    source=var_target,
                    prefix="form_4",
                    database=db
                )
                form_data_5 = forms.ReportForm(
                    request.POST,
                    prefix="form_5",
                )
 
                if all(
                    [
                        form_data_2.is_valid(),
                        form_data_3.is_valid(),
                        form_data_4.is_valid(),
                        form_data_5.is_valid(),
                    ]
                ):
                    dict_query = {
                        "source": var_source,
                        "target": var_target,
                        "filter_source": form_data_3.cleaned_data,
                        "filter_target": form_data_4.cleaned_data,
                        "match": {
                            key.replace(' ', '_'): val
                            for (key, val) in form_data_2.cleaned_data.items()
                        },

                        "report_name": " ".join(
                            form_data_5.cleaned_data["report_name"].
                            lower().split()).replace(' ', '_'),
                        "match_level": 1,
                        "date_period_from": form_data_5.cleaned_data["date_period_from"],
                        "date_period_to": form_data_5.cleaned_data["date_period_to"],
                        "approved": "pending"
                    }
                    BR.push_data("reports", dict_query, db)
                    BR.push_report_to_upload_list(dict_query, db)

        if forms.SimpleForm(request.POST, prefix="form_1", database=db):
            form_data = forms.SimpleForm(request.POST, prefix="form_1", database=db)
            if form_data.is_valid():
                var_source = form_data.cleaned_data["source"]
                var_target = form_data.cleaned_data["target"]
                context["source_data"] = BR.get_one_record(var_source, db)
                context["target_data"] = BR.get_one_record(var_target, db)
                context["source"] = var_source
                context["target"] = var_target
                context['form_2'] = forms.QueryForm(
                    source=var_source,
                    target=var_target,
                    database1=db,
                    database2=db,
                    prefix="form_2",
                )
                context['form_3'] = forms.FilterForm(
                    source=var_source,
                    database=db,
                    prefix="form_3",
                )
                context['form_4'] = forms.FilterForm(
                    source=var_target,
                    database=db,
                    prefix="form_4",
                )
                context['form_5'] = forms.ReportForm(
                    prefix="form_5",
                )

                html_template = loader.get_template(
                    "bank_recon/reports/reports_main.html"
                )
                return HttpResponse(html_template.render(context, request))
    context["form_1"] = forms.SimpleForm(
        prefix="form_1",
        database=db
    )

    context["approved"], context["pending"], context["not_approved"] = BR.get_all_reports(db)

    html_template = loader.get_template("bank_recon/reports/reports_main.html")
    return HttpResponse(html_template.render(context, request))



@login_required(login_url="/login/")
def view_bank_recon_reports_main(request):  # complexity 12
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code
    # context["reports"] = BR.get_reports()
    if request.method == 'POST':
        # second_level = False
        if all([request.POST.get("var_source"),
                request.POST.get("var_target")]):
            var_source = request.POST.get("var_source")
            # pprint(var_source)
            var_target = request.POST.get("var_target")
            if all(
                [
                    forms.QueryForm(
                        request.POST,
                        source=var_source,
                        target=var_target,
                        prefix="form_2",
                        database1=db,
                        database2=db,
                    ),
                    forms.FilterForm(
                        request.POST,
                        source=var_source,
                        prefix="form_3",
                        database=db
                    ),
                    forms.FilterForm(
                        request.POST,
                        source=var_target,
                        prefix="form_4",
                        database=db
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
                    prefix="form_2",
                    database1=db,
                    database2=db,
                )
                form_data_3 = forms.FilterForm(
                    request.POST,
                    source=var_source,
                    prefix="form_3",
                    database=db
                )
                form_data_4 = forms.FilterForm(
                    request.POST,
                    source=var_target,
                    prefix="form_4",
                    database=db
                )
                form_data_5 = forms.ReportForm(
                    request.POST,
                    prefix="form_5",
                )
 
                if all(
                    [
                        form_data_2.is_valid(),
                        form_data_3.is_valid(),
                        form_data_4.is_valid(),
                        form_data_5.is_valid(),
                    ]
                ):
                    dict_query = {
                        "source": var_source,
                        "target": var_target,
                        "filter_source": form_data_3.cleaned_data,
                        "filter_target": form_data_4.cleaned_data,
                        "match": {
                            key.replace(' ', '_'): val
                            for (key, val) in form_data_2.cleaned_data.items()
                        },

                        "report_name": " ".join(
                            form_data_5.cleaned_data["report_name"].
                            lower().split()).replace(' ', '_'),
                        "match_level": 1,
                        "date_period_from": form_data_5.cleaned_data["date_period_from"],
                        "date_period_to": form_data_5.cleaned_data["date_period_to"],
                        "approved": "pending"
                    }
                    BR.push_data("reports", dict_query, db)
                    BR.push_report_to_upload_list(dict_query, db)

        if forms.SimpleForm(request.POST, prefix="form_1", database=db):
            form_data = forms.SimpleForm(request.POST, prefix="form_1", database=db)
            if form_data.is_valid():
                var_source = form_data.cleaned_data["source"]
                var_target = form_data.cleaned_data["target"]
                context["source_data"] = BR.get_one_record(var_source, db)
                context["target_data"] = BR.get_one_record(var_target, db)
                context["source"] = var_source
                context["target"] = var_target
                context['form_2'] = forms.QueryForm(
                    source=var_source,
                    target=var_target,
                    database1=db,
                    database2=db,
                    prefix="form_2",
                )
                context['form_3'] = forms.FilterForm(
                    source=var_source,
                    database=db,
                    prefix="form_3",
                )
                context['form_4'] = forms.FilterForm(
                    source=var_target,
                    database=db,
                    prefix="form_4",
                )
                context['form_5'] = forms.ReportForm(
                    prefix="form_5",
                )

                html_template = loader.get_template(
                    "bank_recon/reports/reports_main.html"
                )
                return HttpResponse(html_template.render(context, request))
    context["form_1"] = forms.SimpleForm(
        prefix="form_1",
        database=db
    )

    context["approved"], context["pending"], context["not_approved"] = BR.get_all_reports(db)

    html_template = loader.get_template("bank_recon/reports/reports_main.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def view_redirect_bank_recon(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")

    if request.method == 'POST':
        form_2 = forms.QueryForm(
            request.POST,
            prefix="form_2",
            database=db
        )
        form_3 = forms.FilterForm(
            request.POST,
            prefix="form_3",
            database=db
        )
        form_4 = forms.ReportForm(
            request.POST,
            prefix="form_4",
        )

        if form_2.is_valid() and form_3.is_valid() and form_4.is_valid():
            pprint(form_2)  # why?
            pprint(form_3)  # why?
            pprint(form_4)  # why?
    response = redirect('/bank-recon-reports-main/')
    return response




@login_required(login_url="/login/")
def bank_recon_report_aje(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")

    if request.method == 'POST':
        AJE.reupload_aje(request, db)

    response = redirect(request.POST.get("redirect_path"))
    return response


@login_required(login_url="/login/")
def bank_recon_export_aje(request, filename=None):
    file_path = "/home/admin/apps/bank_recon/development/recon/app/controllers/helpers/aje_export_module/output"

    path = f"{file_path}/{filename}"
    excel_file = open(path, 'rb')
    data = excel_file.read()

    response = HttpResponse(data, content_type='application/ms-excel')
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    
    return response


@login_required(login_url="/login/")
def view_bank_recon_reports_posts(request):
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    if request.method == 'POST':
        if "btn_delete" in request.POST:
            if BR.del_report(request, db):
                messages.success(request, "Successfully deleted the report.")

        elif "btn_approval" in request.POST:
            # pprint("got here")
            if AP.set_approval(request, request.POST.get("collection"), db):
                messages.success(
                    request, "Successfully updated report status.")

    response = redirect(request.POST.get("redirect_to"))
    return response


@login_required(login_url="/login/")
def post_recon_report(request, collection_id):
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")

    if request.method == "POST":
        if "btn_post_report" in request.POST:
            report_db = BR.fetch_one_document(
                db,
                "reports_database",
                {"report_id": collection_id}
            )
            report = BR.fetch_one_document(
                db,
                "reports",
                {"_id": ObjectId(collection_id)}
            )

            gl_ids = [ i["entry_id_gl"] for i in report_db.get("matched_records") ]
            bs_ids = [ i["entry_id_bs"] for i in report_db.get("matched_records") ]

            gl = BR.fetch_bulk_documents(
                db,
                report["source"],
                {"_id": {"$in": gl_ids}}
            )

            bs = BR.fetch_bulk_documents(
                db,
                report["target"],
                {"_id": {"$in": bs_ids}}
            )

        elif "btn_reset_report" in request.POST:
            pprint("bruh")
            pass            
        # messages.success(
        #     request, "Successfully posted the recon report.")

    response = redirect(request.POST.get("redirect_to"))
    return response


@login_required(login_url="/login/")
# complexity 8
def view_bank_recon_reports_bank_balances_summary_main(request):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code

    if request.method == "POST":
        if CFR.create_bank_balance_summary(request, db):
            messages.success(request, "Successfully created the report.")

        return HttpResponseRedirect(request.path_info)

    context["form_1"] = forms.BankBalancesForm(prefix="form_1", database=db)

    data_fecthed = CFR.get_bank_balances_summary_list(db)

    context["approved"], context["pending"], context["rejected"] = [], [], []

    if data_fecthed:
        for key, val in data_fecthed.items():
            if key == "approved":
                context["approved"] = val
            elif key == "pending":
                context["pending"] = val
            elif key == "rejected":
                context["rejected"] = val

        # pprint(context["pending"])
    html_template = loader.get_template(
        "bank_recon/reports/reports_bank_bal_summary_main.html")
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def bank_recon_reports_bank_balances_summary(request, collection):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    merge(context, context_styling)
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_mo.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code
    data_fetched = CFR.get_bank_balances_summary(collection, db)
    context["bank_bal"] = data_fetched["bank_bal"]
    context["bank_acct_info"] = data_fetched["bank_acct_info"]
    context["unadjusted_disb"] = data_fetched["unadjusted_disb"]
    context["unadjusted_col"] = data_fetched["unadjusted_col"]
    # context["ccfr"] = data_fetched["ccfr"]
    # context["ccfr_total"] = data_fetched["ccfr_total"]
    # context["month_year_period"] = data_fetched["month_year_period"]
    context["report"] = collection
    # context["month_year_period"] = data_fetched["month_year_period"]
    html_template = loader.get_template(
        "bank_recon/reports/reports_bank_bal_summary.html")
    return HttpResponse(html_template.render(context, request))
