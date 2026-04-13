from bson.objectid import ObjectId
from django.contrib import messages
from pprint import pprint
from app.controllers import DbOps


db = DbOps.MainOps()
COLLECTION_MAP = {
    "get_templates":"test_templates",
    "post_templates":"test_templates",
    "post_numbers":"test_numbers",
    "get_numbers":"test_numbers"
}

FIELD_MAPPING = {
    "post_templates": {
        1 : "template_name",
        2 : "template_text"
    },
    "post_numbers": {
        1 : "number_name",
        2 : "number_text"
    } 
}

DEFAULT_CONN = ("sms_system","test_templates")

def post_data(template_name,template_text,function_name)-> bool:
    conn = db.connect("sms_system",COLLECTION_MAP.get(function_name))
    conn.insert_one({
        FIELD_MAPPING[function_name].get(1):template_name,
        FIELD_MAPPING[function_name].get(2):template_text
    })
    
def get_data(function_name)->list:
    conn = db.connect("sms_system",COLLECTION_MAP.get(function_name))
    data = conn.find()

    return_data = []
    for d in data:
        d["id"] = str(d["_id"])
        d["_id"] = str(d["_id"])
        return_data.append(d)

    return return_data

