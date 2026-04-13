from pprint import pprint


class BankReconReport:
    def __init__(self, unmatched_gl_list, unmatched_pb_list):
        self.unmatched_gl_list = unmatched_gl_list
        self.unmatched_pb_list = unmatched_pb_list

    def compute_book_error(self):
        dsn_variance_list = []
        book_error_list = []
        debit_memo = 0
        for pb_entry in self.unmatched_pb_list:
            flag = 0
            for gl_entry in self.unmatched_gl_list:
                # if (gl_entry["other_03"] == pb_entry["transaction_reference"]):
                #     flag = 1
                #     break
                # if not flag:
                #     debit_memo += (pb_entry["credit_amount"] - pb_entry["debit_amount"])

                if (pb_entry["transaction_reference"] in book_error_list
                    or gl_entry["other_03"] == pb_entry["transaction_reference"]):
                    break
                if (
                    gl_entry["other_03"] and
                    pb_entry["transaction_reference"] and
                    gl_entry["other_03"] == pb_entry["transaction_reference"]
                ):
                    gl_entries_amt = sum([(x["dr_amt"] - x["cr_amt"])
                                        for x in self.unmatched_gl_list
                                        if (x["other_03"] == pb_entry["transaction_reference"])
                                        ])
                    pb_entry_amt = pb_entry["credit_amount"] - pb_entry["debit_amount"]
                    variance = pb_entry_amt - gl_entries_amt
                    if variance == 0:
                        pass
                    else:
                        book_error_list.append(pb_entry["transaction_reference"])
                        dsn_variance_list.append(variance)
        book_error_amount = sum(dsn_variance_list)
        return book_error_amount
    
    def compute_outstanding_checks(self):
        outstanding_checks = 0
        for gl_entry in self.unmatched_gl_list:
            flag = 0
            for pb_entry in self.unmatched_pb_list:
                if (gl_entry["other_03"] == pb_entry["transaction_reference"]
                    and gl_entry["trndate"].month == pb_entry["transaction_date"].month):
                    flag = 1
                    break
            if not flag:
                outstanding_checks += (gl_entry["dr_amt"] - gl_entry["cr_amt"])
        return outstanding_checks
    
    def inner_loop(unmatched_pb_list, gl_entry):
        flag = 0
        for pb_entry in unmatched_pb_list:
            if (gl_entry["other_03"] == pb_entry["transaction_reference"]
                and gl_entry["trndate"].month == pb_entry["transaction_date"].month):
                flag = 1
                break
        return flag


def get_book_error(unmatched_gl_list, unmatched_pb_list):
    recon_report = BankReconReport(unmatched_gl_list, unmatched_pb_list)
    res = recon_report.compute_book_error()
    return res

def get_outstanding_checks(unmatched_gl_list, unmatched_pb_list):
    recon_report = BankReconReport(unmatched_gl_list, unmatched_pb_list)
    res = recon_report.compute_outstanding_checks()
    return res
