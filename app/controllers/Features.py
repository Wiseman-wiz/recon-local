from . import DbOps

client = DbOps.MainOps()
db_features = "features"
col_br_features = "bank_recon_features"


src_features = client.connect(database=db_features, collection=col_br_features)


def get_features():
    features = list(src_features.find({}))
    for feature in features:
        feature["_id"] = str(feature["_id"])
    return features
