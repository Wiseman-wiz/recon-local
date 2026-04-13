from django.urls import path
from app import views_transactions

transaction_urls = [
    path(
        'bank-recon-transactions-main/',
        views_transactions.bank_recon_transactions_main,
        name='bank-recon-transactions-main',
    ),
    path(
        'bank-recon-transactions/',
        views_transactions.bank_recon_transactions,
        name='bank-recon-transactions',
    ),
]
