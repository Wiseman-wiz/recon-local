from django.urls import path
from app.view import views_cash_flow_report
from inspect import getmembers, isfunction

cfr_report_urls2 = list(
    path(
        f'{view_name.replace("_","-").replace("view-","")}/',
        view_func,
        name=f'{view_name.replace("_","-").replace("view-","")}',
    )
    for view_name, view_func in list(
        view
        for view in list(getmembers(views_cash_flow_report, isfunction))
        if "view_" in view[0]
    )
)

cfr_report_urls2.append(
    path('bank-recon-reports-comparative-cfr/<str:collection>',
         views_cash_flow_report.bank_recon_reports_comparative_cfr))
cfr_report_urls2.append(
    path('bank-recon-reports-monthly-cfr/<str:month_year>/<str:collection>',
         views_cash_flow_report.bank_recon_reports_monthly_cfr))
cfr_report_urls2.append(
    path('bank-recon-reports-monthly-cfr/<str:month_year>',
         views_cash_flow_report.bank_recon_reports_cfr_monthly))

cfr_report_urls = cfr_report_urls2
