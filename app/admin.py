# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

# Register your models here.
from django.contrib.auth.models import Permission, User
from django.contrib import admin
from app.models import Company, UserCompanyAssignment

admin.site.site_header = "Bank Recon Admin Dashboard"
admin.site.register(Permission)


@admin.register(Company)
class CompaniesAdmin(admin.ModelAdmin):
    list_display = ("company_id", "company_name")

    # pagkalog-in
    # def check_user_company(self, userid):
    #     from django.db.models import Avg
    #     result = UserCompanyAssignment.objects.filter(userid=userid)
    #     return result[0]["company_id"]


@admin.register(UserCompanyAssignment)
class UserCompanyAssignmentAdmin(admin.ModelAdmin):
    list_display = ("company_id", "user_id", "show_name")

    def show_name(self, obj):
        splitted_obj = str(obj).split(",")
        company_id = splitted_obj[0]
        userid = splitted_obj[1]
        result = User.objects.raw(f"SELECT * FROM auth_user where id={userid}")
        return result[0]
