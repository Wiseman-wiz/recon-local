from typing import Dict, Optional

class Helper:
    def __init__(self) -> None:
        pass

    def match_keywords(match_fields: Dict[str, str]) -> Dict[str, str]:
        """
        Matches the keywords that are equivalent to fields.

        Args:
            match_fields (dict): A dictionary of fields to match.

        Returns:
            dict: A dictionary containing standardized field names and their corresponding keywords.
        """
        keyword_mappings = {
            "transaction_reference": "strd_trn_ref",
            "credit_amount": "strd_cr_amt",
            "debit_amount": "strd_dr_amt",
            "transaction_date": "strd_trn_date"
        }
        strd_field_names = {}

        for k, v in match_fields.items():
            qry_res = db["field_keywords"].find_one({"keywords": v})
            
            if qry_res and qry_res["field"] in keyword_mappings:
                strd_field_names[keyword_mappings[qry_res["field"]]] = v

        return strd_field_names
