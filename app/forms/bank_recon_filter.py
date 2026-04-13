from django import forms
from app.controllers import (
    BankRecon as br,
    DbOps
)
from app.controllers.preprocessors.process_choices import *


def choice_creator(list_choice: list) -> list:
    return list((choice, " ".join(choice.split()).replace(' ', '_')) 
                for choice in list_choice )

def choice_creator2(list_choice: list) -> list:

    main_list=[
        ("general_ledger","general_ledger")
    ]

    main_list = main_list+list((choice, " ".join(choice.split()).replace(' ', '_')) 
                for choice in list_choice if "bank_statement" in choice)
    return list(main_list)

def process_choices(list_choice: list) -> list:
    return []


SelectSingleStyle = {"class": "form-control form-control-sm select2bs4 w-50"}
SelectSingleStyleFullWidth = {"class": "form-control form-control-sm select2bs4"}
SelectMultipleStyle = {"class": "js-example-basic-multiple"}


class SimpleForm(forms.Form):
    def __init__(self, *args, **kwargs):
        self.collections = br.get_collections(kwargs.pop("database"))
        self.tables = choice_creator2(self.collections)
        super(SimpleForm, self).__init__(*args, **kwargs)
        self.fields["source"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.tables,
        )
        self.fields["target"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.tables,
        )


class SimpleForm2(forms.Form):
    def __init__(self, *args, **kwargs):
        self.collections = br.get_collections(kwargs.pop("database"))
        self.tables = choice_creator(self.collections)
        super(SimpleForm2, self).__init__(*args, **kwargs)
        self.fields["gl"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.tables,
        )
        self.fields["s1"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.tables,
        )
        self.fields["s2"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.tables,
        )


class QueryForm(forms.Form):
    def __init__(self, *args, **kwargs):
        self.s_choices = choice_creator(
            br.get_keys(
                collection=kwargs.pop("source"),
                db=kwargs.pop("database1")
            )
        )
        self.t_choices = choice_creator(
            br.get_record_keys(
                collection=kwargs.pop("target"),
                db=kwargs.pop("database2")
            )
        )
        self.a_choices = self.s_choices + self.t_choices
        super(QueryForm, self).__init__(*args, **kwargs)

        for value, key in self.s_choices:
            if key != "_id" and key != "date_modified" and key != "approved":
                dict_attrs = SelectSingleStyle
                dict_attrs["required"] = False
                self.fields[str(key)] = forms.ChoiceField(
                    required=False,
                    widget=forms.Select(attrs=dict_attrs, choices=[])
                )
                self.fields[str(key)].choices = list(self.t_choices) + [("", "")]


class FilterForm(forms.Form):
    def __init__(self, *args, **kwargs):
        self.choices = choice_creator(
            br.get_keys(
                collection=kwargs.pop("source"),
                db=kwargs.pop("database")
            )
        )
        super(FilterForm, self).__init__(*args, **kwargs)

        for value, key in self.choices:
            if key != "_id" and key != "date_modified" and key != "approved":
                self.fields[str(key)] = forms.BooleanField(
                    required=False,
                    initial=1,
                    widget=forms.CheckboxInput(attrs={"style": "height: 25px; width: 25px"})
                )


class ReportFilterForm(forms.Form):
    def __init__(self, *args, **kwargs):
        self.s_choices = choice_creator(kwargs.pop("unmatched_gl_cols"))
        self.t_choices = choice_creator(kwargs.pop("unmatched_bs_cols"))
        self.a_choices = self.s_choices + self.t_choices
        super(ReportFilterForm, self).__init__(*args, **kwargs)

        for value, key in self.s_choices:
            dict_attrs = {"class": "form-control form-control-sm select2bs4"}
            dict_attrs["required"] = False
            self.fields[str(key)] = forms.ChoiceField(
                required=False, widget=forms.Select(attrs=dict_attrs, choices=[])
            )
            self.fields[str(key)].choices = list(self.t_choices) + [("", "")]


class ReportForm(forms.Form):
    def __init__(self, *args, **kwargs):
        self.accounts = br.get_accounts_v2(kwargs.pop("database"))
        #self.accounts = br.get_accounts_v2("")
        self.tables = choice_creator(self.accounts)
        super(ReportForm, self).__init__(*args, **kwargs)
        self.fields["report_name"] = forms.CharField(
            required=True, 
            widget=forms.TextInput(
                attrs={'class': 'form-control form-control-sm'})
        )
        self.fields["account_number"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyleFullWidth),
            choices=self.tables,
        )
        self.fields["date_period_from"] = forms.DateTimeField(
            required=True,
            widget=forms.DateInput(
                format="%m/%d/%Y",
                attrs={
                    "class": "form-control form-control-sm",
                    "type": "date"
                }),
        )
        self.fields["date_period_to"] = forms.DateTimeField(
            required=True,
            widget=forms.DateInput(
                format="%m/%d/%Y",
                attrs={
                    "class": "form-control form-control-sm",
                    "type": "date"
                }),
        )

class ReportReconForm(forms.Form):
    def __init__(self, *args, **kwargs):
        self.accounts = br.get_accounts_v2(kwargs.pop("database"))
        #self.accounts = br.get_accounts_v2("")
        self.tables = choice_creator(self.accounts)
        super(ReportReconForm, self).__init__(*args, **kwargs)
        self.fields["account_number"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyleFullWidth),
            choices=self.tables,
        )
        self.fields["date_period_from"] = forms.DateTimeField(
            required=True,
            widget=forms.DateInput(
                format="%m/%d/%Y",
                attrs={
                    "class": "form-control form-control-sm",
                    "type": "date"
                }),
        )
        self.fields["date_period_to"] = forms.DateTimeField(
            required=True,
            widget=forms.DateInput(
                format="%m/%d/%Y",
                attrs={
                    "class": "form-control form-control-sm",
                    "type": "date"
                }),
        )


class Records(forms.Form):
    Record_File = forms.FileField()
    Record_Name = forms.CharField()


class ComparativeCFRForm(forms.Form):
    def __init__(self, *args, **kwargs):
        self.collections = br.get_collections(kwargs.pop("database"))
        self.cfr_ref_coll = br.get_cfr_references(kwargs.pop("database"))
        self.tables = choice_creator(self.collections)
        self.cfr_ref_tbl = choice_creator(self.cfr_ref_coll)
        super(ComparativeCFRForm, self).__init__(*args, **kwargs)
        self.fields["records"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.tables,
        )
        self.fields["cfr_ref"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.cfr_ref_tbl,
        )
        self.fields["report_name"] = forms.CharField(
            required=True,
            widget=forms.TextInput(attrs={
                "class": "form-control form-control-sm w-50"
            }),
        )
        self.fields["period_from"] = forms.DateField(
            required=True,
            widget=forms.DateInput(
                format="%m/%d/%Y", attrs={
                    "class": "form-control form-control-sm w-50",
                    "placeholder": "format: mm/dd/YYYY"
                }),
        )
        self.fields["period_to"] = forms.DateField(
            required=True,
            widget=forms.DateInput(
                format="%m/%d/%Y", attrs={
                    "class": "form-control form-control-sm w-50",
                    "placeholder": "format: mm/dd/YYYY"
                }),
        )


class BankBalancesForm(forms.Form):
    def __init__(self, *args, **kwargs):
        self.collections = br.get_collections(kwargs.pop("database"))
        self.cfr_bank_statement = br.get_cfr_bank_statement(kwargs.pop("database"))
        self.cfr_bank_account = br.get_cfr_bank_account_info(kwargs.pop("database"))
        self.tables = choice_creator(self.collections)
        self.cfr_bank_statement_tbl = choice_creator(self.cfr_bank_statement)
        self.cfr_bank_account_tbl = choice_creator(self.cfr_bank_account)
        super(BankBalancesForm, self).__init__(*args, **kwargs)
        self.fields["bank_statement_col"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.cfr_bank_statement_tbl,
        )
        self.fields["bank_statement_disb"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.cfr_bank_statement_tbl,
        )
        self.fields["bank_account_info"] = forms.ChoiceField(
            required=True,
            widget=forms.Select(attrs=SelectSingleStyle),
            choices=self.cfr_bank_account_tbl,
        )
        self.fields["report_name"] = forms.CharField(
            required=True,
            widget=forms.TextInput(attrs={
                "class": "form-control form-control-sm w-50"}),
        )
        self.fields["report_period"] = forms.DateField(
            required=True,
            widget=forms.DateInput(
                format="%m/%d/%Y", attrs={
                    "class": "form-control form-control-sm w-50",
                    "placeholder": "format: mm/dd/YYYY"}),
        )
