from django.urls import path
from .views_v3 import bank_recon_v3_main,bank_recon_reports,bank_recon_reports_generate

bank_recon_v3 = [
    path(
        'bank-recon-v3-main/',
        bank_recon_v3_main,
        name='bank-recon-v3-main',
    ), 
    path('bank-recon-v3-main/<str:accounts>',
         bank_recon_reports,
         name="bank-recon-reports"),
    path('bank-recon-v3-main/generate_report/<str:report>',
         bank_recon_reports_generate,
         name="bank-recon-reports-generate")
]
