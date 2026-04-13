from django.urls import path
from app import views_general_ledger

general_ledger_urls = [
    path(
        'bank-recon-general-ledger-main/',
        views_general_ledger.bank_recon_general_ledger_main,
        name='bank-recon-general-ledger-main',
    ),
    path(
        'bank-recon-general-ledger/<str:collection>',
        views_general_ledger.bank_recon_general_ledger,
    ),
]
