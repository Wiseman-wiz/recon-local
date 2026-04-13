from bson import ObjectId


def convert_object_id_to_string(data):
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = convert_object_id_to_string(value)
    elif isinstance(data, list):
        for i in range(len(data)):
            data[i] = convert_object_id_to_string(data[i])
    elif isinstance(data, ObjectId):
        return str(data)
    return data