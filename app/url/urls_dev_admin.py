from django.urls import path
from app import views_dev_admin

dev_admin_urls = [
    path('dev-mod-export-note/<filename>/',
         views_dev_admin.dev_module_export_note, name='dev_mod_export_note'),
    path('dev-mod-create-note/', views_dev_admin.dev_module_add_note,
         name='dev_mod_add_note'),
]
