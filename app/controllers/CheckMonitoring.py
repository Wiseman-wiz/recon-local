# from . import DbOps


# client = DbOps.MainOps()
# db = client.ref_db("bank_recon")


def get_checks(db):
    list_collections = list(db.list_collection_names())
    return list(
        str(collection)
        for collection in list_collections
        if "monitoring" in str(collection).lower()
    )
