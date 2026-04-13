from django.urls import path
from app import views_features
from inspect import getmembers, isfunction

report_urls = [
    path(
        f'{view_name.replace("_","-").replace("view-","")}/',
        view_func,
        name=f'{view_name.replace("_","-").replace("view-","")}',
    )
    for view_name, view_func in [
        view for view in [getmembers(views_features, isfunction)]
        if "view_" in view[0]
    ]
]
