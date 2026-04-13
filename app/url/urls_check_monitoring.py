from django.urls import path
from app import views_check_monitoring

check_monitoring_urls = [
    path(
        'bank-recon-check-monitoring-main/',
        views_check_monitoring.bank_recon_check_monitoring_main,
        name='bank-recon-check-monitoring-main',
    ),
    path(
        'bank-recon-check-monitoring/',
        views_check_monitoring.bank_recon_check_monitoring,
        name='bank-recon-check-monitoring'
    ),
]
