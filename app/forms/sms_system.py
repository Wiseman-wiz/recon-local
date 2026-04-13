from django import forms
# from app.controllers import BankRecon as br
from django.forms import ModelForm, Textarea


def choice_creator(list_choice: list) -> list:
    return list(
        (choice, " ".join(choice.split()).replace(' ', '_'))
        for choice in list_choice
    )


SelectSingleStyle = {"class": "form-control form-control-sm select2bs4 w-50"}
SelectMultipleStyle = {"class": "js-example-basic-multiple"}

class create_template(forms.Form):

    template_name = forms.CharField(
        label='Template Name',
        max_length=20
    )
    template_area = forms.CharField(
        label='Template Area',
        max_length=360,
        widget=forms.Textarea(
            attrs={'cols': 80, 'rows': 4}
        )
    )
