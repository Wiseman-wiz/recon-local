# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.db import models

# Create your models here.
class FinanceGroup(models.Model):
    class Meta:
        permission(("finance_all", "Access to all features"))
