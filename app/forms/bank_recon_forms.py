from django import forms
# from app.controllers import BankRecon as br


def choice_creator(list_choice: list) -> list:
    return list(
        (choice, " ".join(choice.split()).replace(' ', '_'))
        for choice in list_choice
    )


SelectSingleStyle = {"class": "form-control form-control-sm select2bs4 w-50"}
SelectMultipleStyle = {"class": "js-example-basic-multiple"}


class BankAccountsForm(forms.Form):
    def __init__(self, *args, **kwargs):
        super(BankAccountsForm, self).__init__(*args, **kwargs)
        self.fields["subaccount"] = forms.CharField(
            required=True,
            widget=forms.TextInput(
                attrs={"class": "form-control form-control-sm"}),
        )
        self.fields["account_number"] = forms.CharField(
            required=True,
            widget=forms.TextInput(
                attrs={"class": "form-control form-control-sm"}
            ),
        )
        self.fields["account_holder"] = forms.CharField(
            required=True,
            widget=forms.TextInput(
                attrs={"class": "form-control form-control-sm"}
            ),
        )
        self.fields["bank_name"] = forms.CharField(
            required=True,
            widget=forms.TextInput(
                attrs={"class": "form-control form-control-sm"}
            ),
        )
        self.fields["bank_branch"] = forms.CharField(
            required=True,
            widget=forms.TextInput(
                attrs={"class": "form-control form-control-sm"}
            ),
        )
        self.fields["account_type_1"] = forms.CharField(
            required=True,
            widget=forms.TextInput(
                attrs={"class": "form-control form-control-sm"}
            ),
        )
        self.fields["account_type_2"] = forms.CharField(
            required=True,
            widget=forms.TextInput(
                attrs={"class": "form-control form-control-sm"}
            ),
        )
        self.fields["beginning_balance"] = forms.FloatField(
            required=True,
            widget=forms.NumberInput(
                attrs={
                    "class": "form-control form-control-sm",
                    "onkeydown": "return event.keyCode !== 69 && \
                                  event.keyCode !== 187",
                    "step": ".01"
                }),
        )
