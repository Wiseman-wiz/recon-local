from django.urls import path
from app import views_error_reports

err_reports_urls = [
    path(
        'error-reports/<str:collection>',
        views_error_reports.error_reports_main,
        name='error_reports',
    ),
]
