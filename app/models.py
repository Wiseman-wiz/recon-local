# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import pymongo

client = pymongo.MongoClient(host="localhost", port=27017)
database_names = list(client.list_databases())

from django.db import models
from django.contrib.auth.models import User


class Finance(User):
    class Meta:
        permissions = (("finance_all", "Access to all features"),)


class FinanceBankReco(models.Model):
    class Meta:
        permissions = (("finance_bank_recon_all", "Access to all features"),)


class Company(models.Model):
    company_id = models.TextField()
    company_name = models.TextField()

    def __str__(self):
        return f"{self.company_id}, {self.company_name}"

    class Meta:
        verbose_name_plural = "Companies"
        ordering = ("company_id", "company_name")
        unique_together = (
            "company_id",
            "company_name",
        )


def get_choices():
    companies = Company.objects.raw(f"SELECT * FROM app_company")
    company_choices = [(str(x).split(",")[0], str(x).split(",")[0]) for x in companies]
    return company_choices


class UserCompanyAssignment(models.Model):
    company_id = models.CharField(max_length=256, choices=get_choices())
    user_id = models.TextField()

    def __str__(self):
        return f"{self.company_id}, {str(self.user_id)}"

    class Meta:
        verbose_name_plural = "UserCompanyAssignments"
        ordering = ("company_id", "user_id")
        unique_together = (
            "company_id",
            "user_id",
        )
