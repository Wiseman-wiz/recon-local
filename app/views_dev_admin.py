from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.http.response import HttpResponseRedirect

# from .controllers import BankRecon as br, Uploader as up
# from .controllers.helpers import Group as gr
from app import context_styling as ctx
from app.controllers.main_logic.Crumble.Crumb import Crumb

context_styling = ctx.context_styles.context_styling


def merge(d1, d2):
    return d1.update(d2)


@login_required(login_url="/login/")
def dev_module_export_note(request, filename=None):
    response = HttpResponse(content_type="text/csv")
    try:
        response["Content-Disposition"] = 'attachment; filename="' + filename + '.json"'

        return response

    except Exception:
        return response


@login_required(login_url="/login/")
def dev_module_add_note(request):
    if request.method == "POST":
        path = "/home/admin/apps/bank_recon/development/recon/dev_notes.json"
        with open(path, "a+") as f:
            to_write = (
                request.POST.get("dev_note")
                + "\n,"
                + f'{{ "url": "{request.POST.get("redirect_path")}" }}\n,'
            )
            f.writelines(to_write)
            f.close()

    return HttpResponseRedirect(request.POST.get("redirect_path", "/"))
