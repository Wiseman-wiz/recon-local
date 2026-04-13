# from django.shortcuts import redirect
from datetime import datetime
import timeit
import json
import pandas as pd
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
db_main_ops = DbOps.MainOps()
context_styling = ctx.context_styles.context_styling


def merge(d1:dict, d2:dict)->dict:
    '''
        Merge dictionaries
    '''
    return d1.update(d2)


@login_required(login_url="/login/")
def reports(request, collection: str):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()

    if not request.session["company_code"]:
        return redirect("logout")
    
    company_code = request.session["company_code"]
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code
    report_data_dates = BR.fetch_one_document(
                db,
                "reports",
                {"_id": ObjectId(collection)}
            )
    
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

        if "btn_rev_of_aje_gl" in request.POST:
            BR.update_document(
                db,
                "reports_database",
                { "report_id": collection },
                { "$set": {"manual_reversal_of_aje_gl": float(request.POST.get("rev_of_aje_gl").replace(',', ''))} }
            )

        if "btn_rev_of_aje_bs" in request.POST:
            BR.update_document(
                db,
                "reports_database",
                { "report_id": collection },
                { "$set": {"manual_reversal_of_aje_bs": float(request.POST.get("rev_of_aje_bs").replace(',', ''))} }
            )
        
        return HttpResponseRedirect(request.path_info)
            
        # list_dict = []
        # list = request.POST.getlist("report[]")
        # for i in list:
        #     list_dict.append(i)
        # pprint(list_dict)
        # BR.push_records_to_check_monitoring(request, collection)

    date_start = None
    date_end = None
        
    try:
        date_start=report_data_dates.get("date_period_from")
        date_end=report_data_dates.get("date_period_to")
        
        # Create a dictionary to hold the data you want to dump
        # data_to_dump = {
        #     "collection": collection,
        #     "database": db,
        #     "request_details": request, # You might want to refine this key based on what 'request' contains
        #     "start_date": date_start,
        #     "end_date": date_end,
        #     "source": "try"
        # }
        # with open("/home/admin/apps/bank_recon/ViewReportsLogs.json", "w+") as f:
        #     json.dump(data_to_dump, f, indent=2, default=str)  # default=str handles datetime serialization

        data = BR.get_specific_report(collection, db, request,date_start, date_end)
    except:
        date_start=report_data_dates.get("date_period_from")
        date_end=report_data_dates.get("date_period_to")
        
        # date_start = None
        # date_end = None

        # Create a dictionary to hold the data you want to dump
        # data_to_dump = {
        #     "collection": collection,
        #     "database": db,
        #     "request_details": request, # You might want to refine this key based on what 'request' contains
        #     "start_date": date_start,
        #     "end_date": date_end,
        #     "source": "except"
        # }
        # with open("/home/admin/apps/bank_recon/ViewReportsLogs.json", "w+") as f:
        #     json.dump(data_to_dump, f, indent=2, default=str)  # default=str handles datetime serialization

        data = BR.get_specific_report(collection, db, request,date_start, date_end)
        
    report_filter_data = BR.get_report_filter(data["report_id"], db)
    # except Exception as e:
    #     messages.error(request, 'Failed to view the report. ERROR: ' + repr(e))
    #     return redirect("/bank-recon-reports-main/")
    
    data["matched_records"] = BR.add_record_matched_id(data["matched_records"], db)
    # data["filtered_match"] = BR.add_record_matched_id_filtered(data["filtered_match"])
    
    gl_conn = db["general_ledger"]
    bs_conn = db["bank_statement"]

    gl_conn.update_many(
        { "record_matched_id": { "$ne": None } },
        { "$set": { "is_matched": True } }
    )
    
    bs_conn.update_many(
        { "record_matched_id": { "$ne": None } },
        { "$set": { "is_matched": True } }
    )

    if report_filter_data:
        if report_filter_data.get("matched_gl_records_filter"):
            for fglm in report_filter_data["matched_gl_records_filter"]:
                if not fglm["is_filter_saved"]:
                    gl_conn.update_one(
                        { "_id": fglm["entry_id_gl"] },
                        { "$set": { "is_matched": False, "record_matched_id": None } }
                    )
                else:
                    gl_conn.update_one(
                        { "_id": fglm["entry_id_gl"] },
                        { 
                            "$set": { 
                                "is_matched": True,
                                "record_matched_id": fglm["record_matched_id"] 
                            } 
                        }
                    )

        if report_filter_data.get("matched_bs_records_filter"):
            for fbsm in report_filter_data["matched_bs_records_filter"]:
                if not fbsm["is_filter_saved"]:
                    bs_conn.update_one(
                        { "_id": fbsm["entry_id_bs"] },
                        { "$set": { "is_matched": False, "record_matched_id": None } }
                    )
                else:
                    bs_conn.update_one(
                        { "_id": fbsm["entry_id_bs"] },
                        { 
                            "$set": { 
                                "is_matched": True,
                                "record_matched_id": fbsm["record_matched_id"] 
                            } 
                        }
                    )

    context["filter_form"] = forms.ReportFilterForm(
        unmatched_gl_cols=data["unmatched_gl_cols"],
        unmatched_bs_cols=data["unmatched_bs_cols"],
        prefix="filter_form",
    )
    context["is_recon_approved"] = report_data_dates.get("approved")
    context["account_number"] = report_data_dates.get("account_number")
    context["sub_account"] = report_data_dates.get("subaccount")
    context["collection_id"] = collection
    context["report_coll_id"] = data["_id"]
    context["report_filter_data"] = report_filter_data
    context["filtered_gl_matched"] = data["filtered_gl_matched"]
    context["filtered_bs_matched"] = data["filtered_bs_matched"]
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
    context["adjusted_cash_bal"] = "{0:,.2f}".format(data.get("adjusted_cash_bal",0.00))
    context["manual_reversal_of_aje_gl"] = "{0:,.2f}".format(float(data.get("manual_reversal_of_aje_gl", 0.00)))
    context["manual_reversal_of_aje_bs"] = "{0:,.2f}".format(float(data.get("manual_reversal_of_aje_bs", 0.00)))
    context["beginning_cash_bal_gl"] = "{0:,.2f}".format(data.get("beginning_cash_bal_gl", 0.00))
    context["beginning_cash_bal_bs"] = "{0:,.2f}".format(data.get("beginning_cash_bal_bs", 0.00))
    context["total_gl_dr_amt"] = "{0:,.2f}".format(data.get("total_gl_dr_amt",0.00))
    context["total_gl_cr_amt"] = "{0:,.2f}".format(data.get("total_gl_cr_amt",0.00))
    context["total_bs_dr_amt"] = "{0:,.2f}".format(data.get("total_bs_dr_amt",0.00))
    context["total_bs_cr_amt"] = "{0:,.2f}".format(data.get("total_bs_cr_amt",0.00))
    context["total_gl_movement"] = "{0:,.2f}".format(data.get("total_gl_movement",0.00))
    context["total_bs_movement"] = "{0:,.2f}".format(data.get("total_bs_movement",0.00))
    context["ending_cash_bal_gl"] = "{0:,.2f}".format(data.get("ending_cash_bal_gl", 0.00))
    context["ending_cash_bal_bs"] = "{0:,.2f}".format(data.get("ending_cash_bal_bs", 0.00))
    context["credit_memo"] = "{0:,.2f}".format(data.get("credit_memo",0.00))
    context["debit_memo"] = "{0:,.2f}".format(data.get("debit_memo",0.00))
    context["reversal_of_aje"] = "{0:,.2f}".format(data.get("reversal_of_aje",0.00))
    context["stale_checks"] = "{0:,.2f}".format(data.get("stale_checks",0.00))
    context["outstanding_checks"] = "{0:,.2f}".format(
        data.get("outstanding_checks",0.00))
    context["deposit_in_transit"] = "{0:,.2f}".format(
        data.get("deposit_in_transit",0.00))
    context["book_errors"] = "{0:,.2f}".format(data.get("book_errors",0.00))
    context["total_gl_aje"] = "{0:,.2f}".format(data.get("total_gl_aje",0.00))
    context["total_gl_fin_bal"] = "{0:,.2f}".format(data.get("total_gl_fin_bal",0.00))
    context["total_pb_fin_bal"] = "{0:,.2f}".format(data.get("total_pb_fin_bal",0.00))
    context["recon_aje_list"] = data["recon_aje_list"]
    html_template = loader.get_template("bank_recon/reports/reports.html")

    end = timeit.default_timer()
    # end = process_time()
    #time_spd = end - start
    #pprint(f"VIEWS FUNC TIME => {time_spd} sec")

    try:
        report_data_dates = db["reports"].find_one(
                    {"_id": ObjectId(collection)})
        data = report_data_dates
        report_db = dict(
                    db["reports_database"].find_one(
                        {"report_name": data["report_name"]}
                    )
                )

        approved_bs_ids = [i for i in report_db.get("matched_records")]
        for _id in approved_bs_ids:
            _id = db["bank_statement"].find_one({"_id": ObjectId(_id["entry_id_bs"])})
            if _id["record_matched_id"]:
                    db["bank_statement"].update_many(
                        {"transaction_reference": _id["transaction_reference"]},
                        {
                            "$set": {
                                "record_matched_id": _id["record_matched_id"],
                            }
                        },
                    )
                    db["general_ledger"].update_many(
                        {"other_03": _id["transaction_reference"]},
                        {
                            "$set": {
                                "record_matched_id": _id["record_matched_id"],
                            }
                        },
                    )
    except:
        pass
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def filter_reports(request, report_coll_id: str):
    context = {}
            
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    
    if not request.session["company_code"]:
        return redirect("logout")
    company_code = request.session["company_code"]
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")
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
            res = BR.set_unmatch_filter(
                gl_matched_ids=request.POST.getlist("matched_gl_entry"),
                bs_matched_ids=request.POST.getlist("matched_bs_entry"),
                report_filter_id=request.POST.get("filter_id"),
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
            if res.get("status") == "success":
                return redirect("bank-recon-reports",
                                collection=request.POST.get("report_id"))
            elif res.get("status") == "failed":
                messages.error(request, res.get("msg"))
                return redirect("filter-reports", report_coll_id=report_coll_id)

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
                    
            # JMR start
            # Convert to DataFrames for inspection
            bs_df_log = pd.DataFrame(report_db_data["unmatched_records_target"])
            gl_df_log = pd.DataFrame(report_db_data["unmatched_records_source"])

            # Prepare logging info
            log_data = {
                "unmatched_records_target": {
                    "columns": bs_df_log.columns.tolist(),
                    "duplicate_columns": bs_df_log.columns[bs_df_log.columns.duplicated()].tolist(),
                    "shape": bs_df_log.shape,
                    "sample_data": bs_df_log.head(3).to_dict(orient="records")
                },
                "unmatched_records_source": {
                    "columns": gl_df_log.columns.tolist(),
                    "duplicate_columns": gl_df_log.columns[gl_df_log.columns.duplicated()].tolist(),
                    "shape": gl_df_log.shape,
                    "sample_data": gl_df_log.head(3).to_dict(orient="records")
                }
            }
            
            bs_filtered, gl_filtered, selected_filter_fields = BR.merge_unmatched_reports(
                unmatched_bs=report_db_data["unmatched_records_target"],
                unmatched_gl=report_db_data["unmatched_records_source"],
                filter_fields=selected_filter_fields
            )
            
            # Dump to a file
            # with open("/home/admin/apps/bank_recon/UnmatchedDataLog.json", "w") as f:
            #     json.dump(log_data, f, indent=2, default=str)
                
            # JMR end

            # temp solution to put unique id for each transaction due to 
            # nature of data of users (distinctness).
            i = 0
            for row_dict in bs_filtered:
                row_dict["filter_bs_id"] = i
                row_dict["is_filter_saved"] = False
                i += 1
            i = 0
            for row_dict in gl_filtered:
                row_dict["filter_gl_id"] = i
                row_dict["is_filter_saved"] = False
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
    context["matched_gl_records_filter"] = report_filter_data.get("matched_gl_records_filter")
    context["matched_bs_records_filter"] = report_filter_data.get("matched_bs_records_filter")
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

    db = db_main_ops.ref_db(f"{company_code}_bank_recon")
    context['company_code'] = company_code
    context["reports"] = "" # BR.get_all_reports(db)
    context["form_1"] = forms.SimpleForm2(prefix="form_1", database=db)

    if request.method == 'POST':
        html_template = loader.get_template("bank_recon/reports/recon2.html")
        return HttpResponse(html_template.render(context, request))

    html_template = loader.get_template("bank_recon/reports/recon.html")
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
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")
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
                    forms.ReportReconForm(
                        request.POST, 
                        prefix="form_5",
                        database=db
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
                form_data_5 = forms.ReportReconForm(
                    request.POST,
                    prefix="form_5",
                    database=db
                )
 
                if all(
                    [
                        form_data_2.is_valid(),
                        form_data_3.is_valid(),
                        form_data_4.is_valid(),
                        form_data_5.is_valid(),
                    ]
                ):
                    account_list = BR.get_accounts_by_account_number(
                        db,
                        form_data_5.cleaned_data["account_number"]
                    )
                    
                    dict_query = {
                        "source": var_source,  # GL
                        "target": var_target,  # BS
                        "filter_source": form_data_3.cleaned_data,
                        "filter_target": form_data_4.cleaned_data,
                        "match": {
                            key.replace(' ', '_'): val
                            for (key, val) in form_data_2.cleaned_data.items()
                        },
                        "report_name": f'{form_data_5.cleaned_data["account_number"]} {form_data_5.cleaned_data["date_period_from"].strftime("%m-%d-%Y")} to {form_data_5.cleaned_data["date_period_to"].strftime("%m-%d-%Y")}',
                        "account_number": form_data_5.cleaned_data["account_number"],
                        "subaccount": account_list["subaccount"],
                        "match_level": 1,
                        "date_period_from": form_data_5.cleaned_data["date_period_from"],
                        "date_period_to": form_data_5.cleaned_data["date_period_to"],
                        "is_report_viewed": False,
                        "approved": "pending"
                    }
                    pprint(dict_query)
                    BR.push_data("reports", dict_query, db)
                    BR.push_report_to_upload_list(dict_query, db)
                    
                    response = redirect('/bank-recon-reports-main/')
                    return response

        if forms.SimpleForm(request.POST, prefix="form_1", database=db):
            # form_data = forms.SimpleForm(request.POST, prefix="form_1", database=db)
            # if form_data.is_valid():
            var_source = "general_ledger"
            var_target = "bank_statement"
            # var_source = form_data.cleaned_data["source"]
            # var_target = form_data.cleaned_data["target"]
            try:
                context["source_data"] = BR.get_one_record(var_source, db)
                context["target_data"] = BR.get_one_record(var_target, db)
            except Exception as e:
                messages.error(request, "No records data found. Please upload GL and BS then try again.")
                response = redirect('/bank-recon-reports-main/')
                return response
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
            context['form_5'] = forms.ReportReconForm(
                database=db,
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
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")

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
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")

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
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")
    if request.method == 'POST':
        if "btn_delete" in request.POST:
            if BR.del_report(request, db):
                messages.success(request, "Successfully deleted the report.")

        elif "btn_approval" in request.POST:
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
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")
    
    if request.method == "POST":
        print("============ POSTING RECEON ============")
        if "btn_post_report" in request.POST:

            report_db = BR.fetch_one_document(
                db,
                "reports_database",
                {"report_id": collection_id}
            )

            BR.update_document(
                db,
                "reports_database",
                {"_id":ObjectId(report_db.get("_id"))},
                {
                    "$set":{
                        "approved":"approved"
                    }
                }
            )
            
            # update the last_updates

            to_exclude_refs = ["DM", "CM", "ENC", "TAX", "INT"]  # TODO: make this dynamic
            
            carryover_gl_ids = []
            for record in report_db["unmatched_records_source"]:
                if record["transaction_reference"] in to_exclude_refs or record["transaction_reference"] == "":
                    carryover_gl_ids.append(record["entry_id_gl"])
                else:
                    if int(record["ref_count_gl"]) > 1:  # * multi-entry records
                        gl_carryover_oids = db["general_ledger"].find({"other_03": record["transaction_reference"]}, {"_id": 1})
                        carryover_gl_ids.extend(oid["_id"] for oid in gl_carryover_oids)
                    else:
                        carryover_gl_ids.append(record["entry_id_gl"])
            
            carryover_bs_ids = []
            for record in report_db["unmatched_records_target"]:
                if record["transaction_reference"] in to_exclude_refs or record["transaction_reference"] == "":
                    carryover_bs_ids.append(record["entry_id_bs"])
                else:
                    if int(record["ref_count_bs"]) > 1:  # * multi-entry records
                        bs_carryover_oids = db["bank_statement"].find({"transaction_reference": record["transaction_reference"]}, {"_id": 1})
                        carryover_bs_ids.extend(oid["_id"] for oid in bs_carryover_oids)
                    else:
                        carryover_bs_ids.append(record["entry_id_bs"])
            
            db["carryover"].update_one(
                {
                    "account_number": report_db["report_name"].split(' ')[0], 
                    "subaccount": report_db["subaccount"]
                },
                { 
                    "$set": {
                        "account_number": report_db["report_name"].split(' ')[0], 
                        "subaccount": report_db["subaccount"],
                    },
                    "$addToSet": {
                        "carryover_gl": {"$each": carryover_gl_ids},
                        "carryover_bs": {"$each": carryover_bs_ids},
                    }
                },
                upsert = True
            )

            approved_bs_ids = [ i for i in report_db.get("matched_records") ]
            for _id in approved_bs_ids:
                db["bank_statement"].update_many(
                    {"transaction_reference":_id["transaction_reference"]},
                    {
                        "$set":{
                            "approved":"approved",
                            "is_matched":True
                        }
                        
                    }
                )
                if _id["transaction_reference"]:
                    db["bank_statement"].update_many(
                        {"transaction_reference":_id["transaction_reference"]},
                        {
                            "$set":{
                                "approved":"approved",
                                "record_matched_id":_id["record_matched_id_bs"],
                            }
                            
                        }
                )
                if _id["record_matched_id_bs"]:
                    db["general_ledger"].update_many(
                        {"other_3":_id["transaction_reference"]},
                        {
                            "$set":{
                                "approved":"approved",
                                "record_matched_id":_id["record_matched_id_bs"],
                            }
                            
                        }
                )
            
            approved_bs_ids = [ i for i in report_db.get("matched_records") ]

            for _id in approved_bs_ids:
                db["general_ledger"].update_many(
                    {"other_03":_id["transaction_reference"]},
                    {
                        "$set":{
                            "approved":"approved",
                            "is_matched":True
                        }
                        
                    }
                )
                if _id["record_matched_id_gl"]:
                    db["general_ledger"].update_many(
                        {"other_03":_id["transaction_reference"]},
                        {
                            "$set":{
                                "approved":"approved",
                                "record_matched_id":_id["record_matched_id_gl"],
                            }
                            
                        }
                )

            # to_exclude_refs = ["DM", "CM", "ENC", "TAX", "INT"]
            
            for i in report_db.get("filtered_gl_matched"):
                val = i.get("transaction_reference_gl", i.get("transaction_reference"))
                if val != "":
                    q = {
                        "approved":"approved",
                        "is_matched":True,
                        "record_matched_id":i.get("record_matched_id"),
                    }
                    if val not in to_exclude_refs:
                        db["general_ledger"].update_many(
                            {"other_03": val},
                            { "$set": q }
                        )
                    else:
                        #* treat to exclude trn_refs list as 1 entry only
                        gl_oid = i.get("entry_id_gl")
                        db["general_ledger"].update_one(
                            {"_id": gl_oid},
                            { "$set": q }
                        )

            for i in report_db.get("filtered_bs_matched"):
                val = i.get("transaction_reference_bs", i.get("transaction_reference"))
                if val != "":
                    q = {
                        "approved":"approved",
                        "is_matched":True,
                        "record_matched_id":i.get("record_matched_id"),
                    }
                    if val not in to_exclude_refs:
                        db["bank_statement"].update_many(
                            {"transaction_reference": val},
                            { "$set": q }
                        )
                    else:
                        bs_oid = i.get("entry_id_bs")
                        db["bank_statement"].update_one(
                            {"_id": bs_oid},
                            { "$set": q }
                        )
                        
            
            approved_gl_ids = [ i["entry_id_gl"] for i in report_db.get("matched_records") ]
            approved_bs_ids = [ i["entry_id_bs"] for i in report_db.get("matched_records") ]
            
            for _id in approved_gl_ids:
                _id = str(_id)
                BR.update_document(
                    db,
                    "general_ledger",
                    {"_id":ObjectId(_id)},
                    {
                        "$set":{
                            "approved":"approved",
                            "is_matched":True
                        }
                        
                    }
                )
            
            for _id in approved_bs_ids:
                _id = str(_id)
                BR.update_document(
                    db,
                    "bank_statement",
                    {"_id":ObjectId(_id)},
                    {
                        "$set":{
                            "approved":"approved",
                            "is_matched":True
                        }
                        
                    }
                )
            
            approved_gl_ids = [ i["entry_id_gl"] for i in report_db.get("unmatched_records_source") ]
            approved_bs_ids = [ i["entry_id_bs"] for i in report_db.get("unmatched_records_target") ]

            if report_db.get("original_bs"):
                original_bs = [ str(i["entry_id"]) for i in report_db.get("original_bs") ]

                for _id1 in original_bs:

                    if _id1 not in list(str(x) for x in approved_bs_ids):
                        BR.update_document(
                            db,
                            "bank_statement",
                            {"_id":ObjectId(_id1)},
                            {
                                "$set":{
                                    "approved":"approved",
                                    "is_matched":True
                                }
                                
                            }
                        )
            if report_db.get("original_gl"):
                original_gl = [ str(i["entry_id"]) for i in report_db.get("original_gl") ]
                
                for _id1 in original_gl:
                    if _id1 not in list(str(x) for x in approved_gl_ids):
                        BR.update_document(
                            db,
                            "general_ledger",
                            {"_id":ObjectId(_id1)},
                            {
                                "$set":{
                                    "approved":"approved",
                                    "is_matched":True
                                }
                                
                            }
                        )
            for _id in approved_gl_ids:
                _id = str(_id)
                BR.update_document(
                    db,
                    "general_ledger",
                    {"_id":ObjectId(_id)},
                    {
                        "$set":{
                            "approved":"approved",
                            "is_matched":False
                        }
                        
                    }
                )

            for i in report_db.get("unmatched_records_source"):
                if i["transaction_reference"] != "":
                    from_date = datetime.strptime(report_db.get("date_period_from"), "%m/%d/%Y")
                    to_date = datetime.strptime(report_db.get("date_period_to"), "%m/%d/%Y")
                    db["general_ledger"].update_many(
                        { 
                            "other_03": i["transaction_reference"],
                            "trndate": {
                                "$gte": from_date,
                                "$lte": to_date
                            }
                        },
                        {
                            "$set":{
                                "approved" : "approved",
                                "is_matched" : False
                            }
                            
                        }
                    )

            for _id in approved_bs_ids:
                _id = str(_id)
                BR.update_document(
                    db,
                    "bank_statement",
                    {"_id":ObjectId(_id)},
                    {
                        "$set":{
                            "approved":"approved",
                            "is_matched":False
                        }
                        
                    }
                )

            for i in report_db.get("unmatched_records_target"):
                if i["transaction_reference"] != "":
                    from_date = datetime.strptime(report_db.get("date_period_from"), "%m/%d/%Y")
                    to_date = datetime.strptime(report_db.get("date_period_to"), "%m/%d/%Y")
                    db["bank_statement"].update_many(
                        { 
                            "transaction_reference" : i["transaction_reference"],
                            "transaction_date": {
                                "$gte": from_date,
                                "$lte": to_date
                            }
                        },
                        {
                            "$set":{
                                "approved" : "approved",
                                "is_matched" : False
                            }
                            
                        }
                    )
                
            conn = db["bank_accounts"]
            conn.update_one(
                {"subaccount": report_db.get("subaccount") },
                {"$set": {"current_balance": report_db.get("total_gl_fin_bal"),"last_updates":report_db.get("dateperiod")}}
            )

            messages.success(request, "Successfully posted the recon report.")
               

        elif "btn_reset_report" in request.POST:
            pprint("bruh")
            pass            

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
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")
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
    db = db_main_ops.ref_db(f"{company_code}_bank_recon")
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
