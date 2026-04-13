from django import forms
from app.controllers import BankRecon as br


def choice_creator(list_choice: list) -> list:
    return list((choice, " ".join(choice.split("_"))) for choice in list_choice)


SelectSingleStyle = {"class": "form-control select2bs4 w-50"}
SelectMultipleStyle = {"class": "js-example-basic-multiple"}


class SimpleForm2(forms.Form):
    def __init__(self, *args, **kwargs):
        self.collections = br.get_collections(kwargs.pop("database"))
        self.tables = choice_creator(self.collections)
        super(SimpleForm2, self).__init__(*args, **kwargs)
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


class QueryForm2(forms.Form):
    def __init__(self, *args, **kwargs):
        self.s_choices = choice_creator(
            br.get_keys(
                kwargs.pop("source"),
                kwargs.pop("database")
            )

        )
        self.t_choices = choice_creator(
            br.get_keys(
                kwargs.pop("target"),
                kwargs.pop("database")
            )
        )
        self.a_choices = self.s_choices + self.t_choices
        super(QueryForm2, self).__init__(*args, **kwargs)

        for value, key in self.s_choices:
            dict_attrs = SelectSingleStyle
            dict_attrs["required"] = False
            self.fields[str(key)] = forms.ChoiceField(
                required=False, widget=forms.Select(attrs=dict_attrs, choices=[])
            )
            self.fields[str(key)].choices = list(self.t_choices) + [("", "")]


class FilterForm2(forms.Form):
    def __init__(self, *args, **kwargs):
        self.choices = choice_creator(
            br.get_keys(
                kwargs.pop("source"),
                kwargs.pop("database")
            )
        )
        super(FilterForm2, self).__init__(*args, **kwargs)

        for value, key in self.choices:
            self.fields[str(key)] = forms.CharField(required=False)


class ReportName2(forms.Form):
    Report_Name = forms.CharField(required=True)


class Records2(forms.Form):
    Record_File = forms.FileField()
    Record_Name = forms.CharField()
