from django.urls import path
from app.view import views_bank_accounts

bank_accounts_urls = [
    path(
        'bank-recon-bank-accounts-main/',
        views_bank_accounts.bank_recon_bank_accounts_main,
        name='bank-recon-bank-accounts-main',
    ),
]
