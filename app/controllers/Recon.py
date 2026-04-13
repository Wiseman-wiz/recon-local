import pandas as pd

class Recon:
    def __init__(self, bs_df, gl_df, date_from, date_to, to_match_fields):
        self.gl_df = GeneralLedger(gl_df)
        self.bs_df = pd.DataFrame(gl_df)
        self.date_from = date_from
        self.date_to = date_to
        self.to_match_fields = to_match_fields
    
    def run(self):
        matched_report = self.match(bs_df=self.bs_df,gl_df = self.gl_df)
        summary_data = Summary(dataset=matched_report)
        summary_cleaned = clean(summary_data)


        
    def match(self):
        self.matched_data()
        
        merge_df = self.gl_df.merge(
            self.bs_df,
            how="outer",
            on=[self.to_match_fields],
            suffixes=(f"_gl", f"_bs"),
        )
        
        return {
            "matched":"",
            "unmatched":"",
            ".....":""
        }

    def matched_single(self):
        pass

    def matched_many(self):
        pass


class ReconSummary:
    def __init__(self, matched_gl, matched_bs, unmatched_gl, unmatched_bs, book_errors, filtered_match) -> None:
        self.matched_gl = matched_gl
        self.matched_bs = matched_bs
        self.unmatched_gl = unmatched_gl
        self.unmatched_bs = unmatched_bs
        self.book_errors = book_errors
        self.filtered_match = filtered_match 

    def compute_bank_recon_report(self):
        # adjusted_cash_beginning_balance = BankAccount.get_current_beginning_balance()
        # gl_total_collections = sum(gl_debit_amount)
        # gl_total_disbursements = -(sum(gl_credit_amount))
        # bs_total_collections = sum(bs_credit_amount)
        # bs_total_disbursements = -(sum(bs_debit_amount))
        # gl_total_cash_movement = gl_total_collections + gl_total_disbursements
        # bs_total_cash_movement = bs_total_collections + bs_total_disbursements
        # bs_deposit_in_transit = sum(gl_debit_amount_w/_DSN_reference_but_not_reflected_on_bs)
        # bs_reversal_of_aje = - bs_deposit_in_transit_previous_period
        # gl_reversal_of_aje = - gl_outstanding_checks_previous_period
        # gl_outstanding_checks = sum(gl_credit_amount_w/_cheque_no_but_not_reflected_on_bs)
        # gl_stale_checks = sum(gl_credit_amount_w/_cheque_no_but_not_reflected_on_bs_AND_cheque_date_is_past_6mos)
        # gl_debit_memo = sum(bs_debit_amount_not_reflected_on_gl)
        # gl_credit_ memo = sum(bs_credit_amount_not_reflected_on_gl)
        # gl_book_error = sum(items_with_rounding_off_OR_transposition_error)
        # gl_total_aje = sum(all computed amounts of gl aje)
        # bs_total_aje = bs_deposit_in_transit + bs_reversal_of_aje
        # gl_final_balance = gl_adjusted_cash_beginning_balance + gl_total_cash_movement + gl_total_aje
        # bs_final_balance = bs_adjusted_cash_beginning_balance + bs_total_cash_movement + bs_total_aje
        pass


class GeneralLedger:
    '''
    1. initialize the ledger
    2. function to get the ledger by date period
    3. 
    '''
    def __init__(self, gl_list) -> None:
        self.gl_df = pd.DataFrame(gl_list)
        
    def find_all(self):
        pass
    
    def find_one(self, id):
        pass
    
    def get_gl_by_date(self, date_from, date_to):
        pass

        
class BankStatement:
    '''
    1. initialize the bs
    2. function to get the bs by date period
    3. 
    '''
    def __init__(self, bs_list) -> None:
        self.bs_df = pd.DataFrame(bs_list)

    def find_one(self):
        pass

    def delete(self):
        pass

    def delete_many(self):
        pass

    def save(self):
        pass
    
    def get_bs_by_date(self, date_from, date_to):
        pass

class Report:
    def __init__(self) -> None:
        pass

    def get_matched(self):
        pass

    def get_unmatched(self):
        pass

    def get_filtered(self):
        pass

    def get_options(self):
        pass