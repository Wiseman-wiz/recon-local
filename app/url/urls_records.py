from django.urls import path
from app import views_records
from app import views_records_v2

records_urls = [
    path(
        'bank-recon-records-main/',
        views_records.bank_recon_records_main,
        name='bank-recon-records-main',
    ),
    path(
        'bank-recon-records-deletion/',
        views_records.bank_recon_records_deletion,
        name='bank-recon-records-deletion',
    ),
    path(
        'bank-recon-records-summary/',
        views_records.bank_recon_records_summary,
        name='bank-recon-records-summary',
    ),
    path(
        'bank-recon-records-main/',
        views_records_v2.bank_recon_records_main,
        name='bank-recon-records-main-v2',
    ),
    path(
        'bank-recon-records/',
        views_records.bank_recon_records_list,
    ),
    path(
        'bank-recon-records/<str:collection>',
        views_records.bank_recon_records,
    ),
    path(
        'delete-record/',
        views_records.delete_record,
        name="del_record")
]
