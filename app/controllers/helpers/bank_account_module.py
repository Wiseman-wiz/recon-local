import pymongo

client = pymongo.MongoClient(host="localhost", port=27017)
# db = client["bank_recon"]
# bank_acc_col = db["bank_accounts"]

class Bank_Account_Module:
    def __init__(self, pb_list, month, company_code):
        self.month = month
        self.pb_list = [x for x in pb_list if str(x["transaction_date"].month) == str(month)]
        self.distinct_pb_acc_num = list(dict.fromkeys([x["account_number"] for x in self.pb_list]))
        self.company_code = company_code
        self.monthly_adj_cash_bal = self.calculate()

    #  test
    def calculate(self):
        monthly_adj_cash_bal_list = []
        db = client[f"{self.company_code}_bank_recon"]
        bank_acc_col = db["bank_accounts"]
        for accno in self.distinct_pb_acc_num:
            acc_begining_balance = float(bank_acc_col.find_one({"account_number" : accno})["beginning_balance"])
            one_row_res = self.total_cr_minus_dr_per_acc(accno, acc_begining_balance)
            monthly_adj_cash_bal_list.append(one_row_res)
        final = sum(monthly_adj_cash_bal_list)
        return final

    def total_cr_minus_dr_per_acc(self, acc_no, acc_begining_balance):
        total_of_cr_min_dr = sum([(x["credit_amount"] - x["debit_amount"])
                                for x in self.pb_list
                                if (x["account_number"] == acc_no)
                                ])
        basebal_minus_sum_cr_dr = acc_begining_balance + (total_of_cr_min_dr)
        return basebal_minus_sum_cr_dr

def get_monthly_adj_cash_bal(pb_list, company_code):
    month = pb_list[0]["transaction_date"].month
    bankrec_case = Bank_Account_Module(pb_list, month, company_code)
    #  test
    res = bankrec_case.calculate()
    return res