# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
# Create your views here.
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth import authenticate, login, update_session_auth_hash
from django.contrib.auth.decorators import login_required
from .forms import LoginForm, SignUpForm, ChangePasswordForm
from .decorators import unauthenticated_user
from app.controllers.main_logic.Crumble.Crumb import Crumb


@unauthenticated_user
def login_view(request):
    form = LoginForm(request.POST or None)

    msg = None

    if request.method == "POST":

        if form.is_valid():
            username = form.cleaned_data.get("username")
            password = form.cleaned_data.get("password")
            user = authenticate(username=username, password=password)
            if user is not None:
                login(request, user)
                return redirect("/bank-recon/select-company/")
            else:    
                msg = 'Invalid credentials'    
        else:
            msg = 'Error validating the form'    

    return render(request, "accounts/login.html", {"form": form, "msg" : msg})

@unauthenticated_user
def register_user(request):
    pass

@login_required(login_url="/login/")
def change_user_password(request):
    context = {}
    crumble = Crumb()
    context["crumble"] = crumble.get_crumble_html()
    if not request.session["company_code"]:
        return redirect("logout")
    context["company_code"] = request.session["company_code"]
    
    if request.method == 'POST':
        form = ChangePasswordForm(request.user, request.POST)
        if form.is_valid():
            user = form.save()
            update_session_auth_hash(request, user)
            messages.success(request, 'Your password was successfully changed.')
    else:
        form = ChangePasswordForm(request.user)
        
    context["form"] = form
    return render(request, 'accounts/change-password.html', context)
