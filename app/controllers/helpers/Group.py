from pprint import pprint


def group_by_collection_name(list_to_group:list)->dict:
    distinct_list = []
    for item in sorted(list_to_group):
        if item.split('_')[0] not in distinct_list:
            distinct_list.append(item.split('_')[0])
            
    grouped_dict = {}
    for unique_item in distinct_list:
        grouped_dict[unique_item] = []
        for original_item in sorted(list_to_group):
            if unique_item == original_item.split('_')[0]:
                grouped_dict[unique_item].append(original_item.replace('_', ' '))
                
    return grouped_dict

