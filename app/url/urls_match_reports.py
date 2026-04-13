from django.urls import path
from app.view import views_match_reports
from inspect import getmembers, isfunction

match_report_urls2 = list(
    path(
        f'{view_name.replace("_","-").replace("view-","")}/',
        view_func,
        name=f'{view_name.replace("_","-").replace("view-","")}',
    )
    for view_name, view_func in list(
        view
        for view in list(getmembers(views_match_reports, isfunction))
        if "view_" in view[0]
    )
)
match_report_urls2.append(
    path('bank-recon-match-reports/<str:collection>',
         views_match_reports.match_reports))

match_reports_urls = match_report_urls2