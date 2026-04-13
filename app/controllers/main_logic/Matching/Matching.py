import pymongo
from celery import Celery
from typing import Callable, List, Dict, Any
import timeit
from helper_v2.Performance import timer_decorator
import json


app = Celery()
DEBUG = True

CONFIG: Dict = {}
with open("matching.json", "r") as f:
    CONFIG = json.load(f)


class Matching:
    """
    Sample Main Code:
        import Matching
        matcher = Matching()
        database: str = "Database",
        criteria: Dict[int, Dict[str, str]] = "Matching criteria",
        criteria_conditions: List[Dict[str, List[int]]] = "Matching conditions",
        bank_account: str = "Bank account",
        data = matcher.multi_conditional_matching(
            database,
            criteria,
            criteria_patter,
            bank_account
        )

    Status:
        
    Description:
        
    Parameters:
        
    Methods:
        
    Improvements:
        Add Streaming Performance
        
    """
    def __init__(self):
        self.client: Callable(str) = pymongo.MongoClient("localhost:27017")
        self.company_code: str = ""

    @app.task(bind=True, name="run_matching")
    def run_match_task(
        self,
        conn_dict: Dict[str, List[str]] = CONFIG.get("connection"),
    ):
        self.infunc = InnerCall()
        self.infunc.get_conn(conn_dict)


    def get_conn(
        self, conn_dict: Dict[str, List[str]]
    ) -> Dict[str, Callable[[Any], Any]]:
        dbs_conn: Dict[str, Callable[[Any], Any]] = {}
        col_conn: Dict[str, Callable[[Any], Any]] = {}
        for db_name, col_list in conn_dict.items():
            for col_name in col_list:
                col_conn[f"{db_name}_{col_name}"] = self.client[db_name][col_name]

        for conn_name, conn in col_conn.items():
            return conn.find()

    @app.task(bind=True, name="multi_conditional_matching")
    def multi_conditional_matching(
        self,
        database: str = CONFIG.get("db"),
        criteria: Dict[int, Dict[str, str]] = CONFIG.get("criteria"),
        criteria_pattern: List[Dict[str, List[int]]] = CONFIG.get("criteria_pattern"),
        bank_account: str = CONFIG.get('bank_acct'),
        streaming: bool = False,
    ):
        """
        Description:
            Method to match data based on criteria

        Parameters:


        Returns:


        Improvements:
            None
        """
        if not streaming:
            inner_func = InnerCall()
            pass

    @app.task(bin=True, name="single_match")
    def single_match(self,
        database: str = CONFIG.get("db"),
        criteria: List[Dict[str,str]] = CONFIG.get("single_criteria")
    ):
        

        pass

    def batch_data(self):
        return 1
        pass

    def stream_data(self):
        pass


class InnerCall(Matching):
    def __init__(self):
        super().__init__()


if __name__ == "__main__":
    matching = Matching()
    data = ""
    data.__str__
    def testing():
        matching.run_match_task()


    matching_criteria = [{
        "source_field_a":"target_field_b"
    }]
    collection_mapping = {
        "database":{
            "source":"general_ledger",
            "target":"bank_statement"
        }
    }


    # Option 1:
    df_matched = matching.get_matched(matching_criteria)
    df_unmatched = matching.get_unmatched(matching_criteria)

    # Option 2:
    df_matched, df_unmatched = matching.get_matched_and_not(matching_criteria)

    # Option 3: 
    dataframe_mapping = { 
        "database":{
            "source":"general_ledger",
            "target":"bank_statement"
        },
        "criteria":{
            "source_field_a":"target_field_b"
        },
        "matched":"",
        "unmatched":""

    }

    for key,value in matching.get_result(matching_criteria).items():
        dataframe_mapping[key] = value


    testing()
    pass
