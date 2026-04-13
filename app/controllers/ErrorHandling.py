from . import DbOps
from bson.objectid import ObjectId
from pprint import pprint

client = DbOps.MainOps()
db_bank_recon = "errors"
col_bank_recon_errors = "bank_recon_errors"


src_bank_recon_err = client.connect(database=db_bank_recon, collection=col_bank_recon_errors)


def error_handling(func):
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        return func(*args, **kwargs)
    return wrapper