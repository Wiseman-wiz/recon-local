from django.urls import path
from app import views_uploads
from app import views_uploads_v2
from app import views_aje
from app import views_audit

uploads_urls = [
    path('bank-recon-uploads/',
         views_uploads.bank_recon_upload,
         name='bank-recon-uploads'),
     path('bank-recon-aje/',
         views_aje.bank_recon_upload_aje,
         name='bank-recon-aje'),
     
     path('bank-recon-audit/',
         views_audit.bank_recon_audit,
         name='bank-recon-audit'),
    
    path('bank-recon-uploads-v2/',
         views_uploads_v2.bank_recon_upload,
         name='bank-recon-uploads-v2'),
    path('bank-recon-export/<filename>/',
         views_uploads_v2.bank_recon_export_csv,
         name='bank-recon-export'),
]
