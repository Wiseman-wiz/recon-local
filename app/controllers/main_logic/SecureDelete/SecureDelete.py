
from typing import Dict, List

"""
Brain Dump:
	Record Deletion Module:
		Read Ops:
			Filter - Optional
			Conditions:
				"approved" : False
            
            Return:
                batch?
                selective?


		Delete Ops:
			batch
			selective 

		Update:
			disallowed

		Create:
			diasllowed


	Test??
			Test Case Optional

	`
"""
class SecureDelete:
    def __init__(self):
        pass

    def get_data(self)->List[Dict[str,str]]:
        

        return

    def batch_delete(self, db_name:str, collection_name:str, _ids:List):
        self.d_mod = {

            "db":{
                "name":str,
                "collection":{
                    "name":str,
                    "_ids":List[str] # ObjectId's?
                }
            }
        }        
    