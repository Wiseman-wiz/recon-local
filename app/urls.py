# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.urls import path, re_path
from app import views
from app.url import (
    urls_transactions,
    urls_reports,
    urls_records,
    urls_error_reports,
    urls_check_monitoring,
    urls_general_ledger,
    urls_uploads,
    urls_dev_admin,
    urls_match_reports,
    urls_cash_flow_report,
    urls_bank_accounts,
)

from .BankReconV3.urls_v3 import bank_recon_v3

attached = (
    urls_transactions.transaction_urls
    + urls_reports.report_urls
    + urls_records.records_urls
    + urls_error_reports.err_reports_urls
    + urls_check_monitoring.check_monitoring_urls
    + urls_general_ledger.general_ledger_urls
    + urls_uploads.uploads_urls
    + urls_dev_admin.dev_admin_urls
    + urls_match_reports.match_reports_urls
    + urls_cash_flow_report.cfr_report_urls
    + urls_bank_accounts.bank_accounts_urls
    + bank_recon_v3
)
main = [
    # The home page
    path("", views.index, name="home"),
    path("2", views.index_2, name="home2"),
    path("post_templates", views.post_templates, name="post_templates"),
    path("bank_recon_data_checker/<str:field_name>/<str:field_data>", views.bank_recon_data_checker, name="bank_recon_data_checker"),
    path("post_campaign", views.post_campaign, name="post_campaign"),
    path("post_numbers", views.post_numbers, name="post_numbers"),
    path("get_templates", views.get_templates, name="get_templates"),
    path("get_numbers", views.get_numbers, name="get_numbers"),
    path("data_handler_debug", views.data_handler_debug, name="data_handler_debug"),
    path("bank-recon/", views.bank_recon_main, name="bank-recon"),
    path(
        "bank-recon/change-company/",
        views.bank_recon_change_company,
        name="bank-recon-change-company",
    ),
    path(
        "bank-recon/select-company/",
        views.bank_recon_select_company,
        name="bank-recon-select-company",
    ),
    path("bank-recon-accounts/", views.bank_recon_accounts, name="bank-recon-accounts"),
    path("main/", views.main_page, name="main_page"),
    re_path(r"^.*\.*", views.pages, name="pages"),
]
urlpatterns = attached + main
