from django.urls import path
from app import views_reports
from app import views_reports_v2
from inspect import getmembers, isfunction

report_urls2 = list(
    path(
        f'{view_name.replace("_","-").replace("view-","")}/',
        view_func,
        name=f'{view_name.replace("_","-").replace("view-","")}',
    )
    for view_name, view_func in list(
        view
        for view in list(getmembers(views_reports, isfunction))
        if "view_" in view[0]
    )
)
report_urls2.append(
    path('bank-recon-reports/<str:collection>',
         views_reports.reports,
         name="bank-recon-reports"))
report_urls2.append(
    path('bank-recon-reports/filter/<str:report_coll_id>',
         views_reports.filter_reports,
         name="filter-reports"))
report_urls2.append(
    path('bank-recon-reports/post-recon/<str:collection_id>',
         views_reports.post_recon_report,
         name='post-recon-report'))
report_urls2.append(
    path('bank-recon-reports/export-aje/<str:filename>',
         views_reports.bank_recon_export_aje,
         name='bank-recon-export-aje'))
report_urls2.append(
    path('bank-recon-reports-bank-balances-summary/<str:collection>',
         views_reports.bank_recon_reports_bank_balances_summary))
report_urls2.append(
    path('bank-recon-reports-aje',
         views_reports.bank_recon_report_aje, name="aje"))

report_urls = report_urls2
