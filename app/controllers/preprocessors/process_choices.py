from pprintpp import pprint
from app.controllers import DbOps


client = DbOps.MainOps()

#  excluded for now for multi db
class Check_KeyMatch:
	def __init__(self) -> None:
		self.targt_src_DB = client.ref_db("bank_recon")
		self.standard_field_DB = client.ref_db("bank_recon_field_keywords")
	
	def check_if_keyword_exist(self, keyword):
		standard_fields = [x for x in self.standard_field_DB["field_keywords"].find()]
		has_match = False
		for field in standard_fields:
			field_name = field["field"]
			field_keywords = field["keywords"]
			if keyword in field_keywords:
				has_match = True
				return {
					"match" : 1,
					"standard_field" : field_name,
					"keyword" : keyword
				}
		if has_match == False:
			return {
				"match" : 0,
				"standard_field" : None,
				"keyword" : keyword
			}
	
	def get_field_names_of_collection(self, colname):
		print(colname)
		key_list = list(self.targt_src_DB[colname].find_one().keys())
		return key_list

	
	def add_not_existing_keywords(self, kword: str, field_name: str):
		self.standard_field_DB["field_keywords"].update_one(
			{"field": field_name},
			{"$addToSet": {"keywords": kword}}
		)

###### example ######
ckm = Check_KeyMatch()
key_list = ckm.get_field_names_of_collection("test_jle07_pb_coll")
for keyword in key_list:
	res = ckm.check_if_keyword_exist(keyword)
	if res["match"] == 1:
		pass
	else:
		kword = res["keyword"]
		# print(f"add or leave it blank for {kword}")

# def populate_in_ui_src_part() -> None: 
#     pass


# def populate_in_ui_trgt_part() -> None: 
#     pass

# def src_match_or_not():
#     db_conn = client.ref_db("bank_recon")
# 	src_key_matched = False
# 	for src_key in fieldnames_src:
# 		if src_key in standard_key["keywords"]:
# 			populate_in_ui_src_part(src_key)
# 			src_key_matched = True
# 	return src_key_matched

# def trgt_match_or_not():
# 	trgt_key_matched = False
# 	for trgt_key in fieldnames_trgt:
# 		if trgt_key in standard_key["keywords"]:
# 			populate_in_ui_trgt_part(trgt_key)
# 			trgt_key_matched = True

# def add_on_list_of_keywords(field, new_keyword):
	
# 	keyword_coll.update_one()


# for standard_key in standard_keywords:
# 	src_key_matched = src_match_or_not()
# 	trgt_key_matched = trgt_match_or_not()
	
# 	if src_key_matched == False:
# 		if user_choice == "add":
# 			add_on_list_of_keywords(src_key)
# 		else:
# 			leave_it_blank()
# 	if trgt_key_matched == False:
# 		if user_choice == "add":
# 			add_on_list_of_keywords(trgt_key)
# 		else:
# 			leave_it_blank()