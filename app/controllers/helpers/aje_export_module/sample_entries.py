from bson.objectid import ObjectId
import datetime

def samples():
    sample_entries = [{
            "transaction_date" : "7-Jan-21",	
            "posting_date" : "7-Jan-21",
            "transaction_code" : "CLX",
            "debit_amount" : 0,
            "credit_amount" : 6922.44,
            "transaction_number" : "0121-001",
            "adjustment_type_code" : "04-50",
            "description" : "TO RECORD DEPOSITS AND BONDS FROM TENANTS FOR THE MONTH OF JAN 2021"
        },
        {
            "transaction_date" : "7-Jan-21",	
            "posting_date" : "7-Jan-21",
            "transaction_code" : "CLY",
            "debit_amount" : 0,
            "credit_amount" : 6922.44,
            "transaction_number" : "0121-001",
            "adjustment_type_code" : "04-50",
            "description" : "TO RECORD DEPOSITS AND BONDS FROM TENANTS FOR THE MONTH OF JAN 2021"
        },
        {
            "transaction_date" : "7-Jan-21",	
            "posting_date" : "7-Jan-21",
            "transaction_code" : "CLZ",
            "debit_amount" : 0,
            "credit_amount" : 6922.44,
            "transaction_number" : "0121-001",
            "adjustment_type_code" : "04-50",
            "description" : "TO RECORD DEPOSITS AND BONDS FROM TENANTS FOR THE MONTH OF JAN 2021"
        },
        {
            "transaction_date" : "7-Jan-21",	
            "posting_date" : "7-Jan-21",
            "transaction_code" : "CL",
            "debit_amount" : 0,
            "credit_amount" : 6922.44,
            "transaction_number" : "0121-001",
            "adjustment_type_code" : "04-50",
            "description" : "TO RECORD DEPOSITS AND BONDS FROM TENANTS FOR THE MONTH OF JAN 2021"
        },
        {
            "transaction_date" : "7-Jan-21",	
            "posting_date" : "7-Jan-21",
            "transaction_code" : "CL",
            "debit_amount" : 0,
            "credit_amount" : 6922.44,
            "transaction_number" : "0121-001",
            "adjustment_type_code" : "04-50",
            "description" : "TO RECORD DEPOSITS AND BONDS FROM TENANTS FOR THE MONTH OF JAN 2021"
        }
    ]

    sample_PB_data = [
        {
            '_id': ObjectId('61f97184799d65edebe8682c'),
            'transaction_date': datetime.datetime(2021, 2, 5, 0, 0),
            'check_number': '',
            'transaction_description': 'CL-01',
            'debit_amount': 0.0,
            'credit_amount': 64376.55,
            'balance': '4251288.60',
            'transaction_reference': 'GW-00120',
            'from': '02/01/2021', 'to': '02/28/2021',
            'account_number': '190-7-190-813548',
            'date_modified': datetime.datetime(2022, 2, 2, 1, 44, 36, 849000),
            'approved': 'pending'
        },
        {'_id': ObjectId('61f97184799d65edebe8682f'), 'transaction_date': datetime.datetime(2021, 2, 5, 0, 0), 'check_number': '', 'transaction_description': 'DP', 'debit_amount': 0.0, 'credit_amount': 101395.87, 'balance': '4454446.07', 'transaction_reference': 'GW-00050', 'from': '02/01/2021', 'to': '02/28/2021', 'account_number': '190-7-190-813548', 'date_modified': datetime.datetime(2022, 2, 2, 1, 44, 36, 849000), 'approved': 'pending'},
        {'_id': ObjectId('61f97184799d65edebe86845'), 'transaction_date': datetime.datetime(2021, 2, 5, 0, 0), 'check_number': '', 'transaction_description': 'CL', 'debit_amount': 0.0, 'credit_amount': 7145.6, 'balance': '4913080.46', 'transaction_reference': 'GW-00121', 'from': '02/01/2021', 'to': '02/28/2021', 'account_number': '190-7-190-813548', 'date_modified': datetime.datetime(2022, 2, 2, 1, 44, 36, 849000), 'approved': 'pending'},
        {'_id': ObjectId('61f97184799d65edebe86849'), 'transaction_date': datetime.datetime(2021, 2, 8, 0, 0), 'check_number': '', 'transaction_description': 'DT', 'debit_amount': 7145.6, 'credit_amount': 0.0, 'balance': '5047233.41', 'transaction_reference': '', 'from': '02/01/2021', 'to': '02/28/2021', 'account_number': '190-7-190-813548', 'date_modified': datetime.datetime(2022, 2, 2, 1, 44, 36, 849000), 'approved': 'pending'}
    ]

    sample_GL_data = [
        {
            '_id': ObjectId('61f9712f799d65edebe86722'),
            'trndate': datetime.datetime(2021, 2, 2, 0, 0),
            'trnno': '4236',
            'subacct': '1000-005',
            'other_01': 'BPI FAMILY SAVINGS BANK',
            'other_03': 'GW-00050',
            'dr_amt': 101395.88,
            'cr_amt': 0.0,
            'from': '02/01/2021',
            'to': '02/28/2021',
            'ref_1': '11-01',
            'ref_2': 'C-02',
            'date_modified': datetime.datetime(2022, 2, 2, 1, 43, 11, 507000),
            'approved': 'pending'
        },
        {'_id': ObjectId('61f9712f799d65edebe8672c'), 'trndate': datetime.datetime(2021, 2, 4, 0, 0), 'trnno': '4251', 'subacct': '1000-005', 'other_01': 'REMALYN M.C. PHONESHOP AND TRADING INC.', 'other_03': 'GW-00134', 'dr_amt': 667.5, 'cr_amt': 0.0, 'from': '02/01/2021', 'to': '02/28/2021', 'ref_1': '11-02', 'ref_2': 'C-01', 'date_modified': datetime.datetime(2022, 2, 2, 1, 43, 11, 507000), 'approved': 'pending'},
        {'_id': ObjectId('61f9712f799d65edebe8672d'), 'trndate': datetime.datetime(2021, 2, 4, 0, 0), 'trnno': '4252', 'subacct': '1000-005', 'other_01': 'REMALYN M.C. PHONESHOP AND TRADING INC.', 'other_03': 'GW-00134', 'dr_amt': 1556.68, 'cr_amt': 0.0, 'from': '02/01/2021', 'to': '02/28/2021', 'ref_1': '11-02', 'ref_2': 'C-01', 'date_modified': datetime.datetime(2022, 2, 2, 1, 43, 11, 507000), 'approved': 'pending'}
    ]
    return sample_entries, sample_PB_data, sample_GL_data