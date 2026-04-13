from typing import Union, List
import pymongo

class KeyMaps:
    def __init__(self,uri= "localhost:27017",db_name="development",col_name="keymaps"):
        self.gl_maps = {}
        self.bs_maps = {}
        self.client = pymongo.MongoClient(uri) 
        self.db = self.client[db_name] # use if connection to another db
        self.col = self.db[col_name] # user if connection to another collection
        self.schema = {
            "name" : "str | name of the key",
            "dtype" : "str/int/float | data type of the key",
            "strd":  "str | replacement name of the key"
        }
    def data_validation(self,data,key):
        if data:
            if not data.get("name",None):
                raise ValueError(f"No set standard key for {key}")
        else:
            raise Exception(f"no record found for {key}")
        
    def get_keymaps(self,key_names:Union[List[str],str]):
        if isinstance(key_names,str):
            return self.keymaps(key_names)

        elif isinstance(key_names,list):
            if isinstance(key_names[0],str):
                return self.iterate_keymap_list        
            else:
                raise ValueError(f"list must contain string and not {type(key_names[0])}")            
        else:
            raise Exception("Invalid dtypes")
        
    def iterate_keymap_list(self, list_keys:List[str]):
        final_maps = {}
        for key in list_keys:
            data = self.col.find_one({"name":key},{"_id":0,"strd":1})
            self.data_validation(data,key)
            final_maps[key] = data.get("name")
        return final_maps
    
    def keymaps(self, key:str):
        data = self.col.find_one({"name":key},{"_id":0,"strd":1})
        self.data_validation(data,key)
        return {key : data.get("strd"),}
        
        
    
    
if __name__ == "__main__":
    import pandas as pd
    # Default Runs
    km = KeyMaps()
    field_to_match = {"cr_amt":"dr_amt"}
    
    bs_df = pd.DataFrame()
    gl_df = pd.DataFrame()
    
    gl_df_cols = gl_df.columns
    mapping = km.get_keymaps(gl_df_cols)
    gl_df.rename(mapping)
    