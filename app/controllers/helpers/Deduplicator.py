# Answer:
# Include sa template (if gl or bs?).

# After i-upload check if same sa outstanding balance sa passbook (bank_statement) => check balance (debit and credit) and date.

# Sa GL get trn_no. Isama sa template. Second basis will be the check_no. Sa receipt isa lang ang cash entry per trn. 

# Logic will be:
# If hindi existing trn_no - new
# Else - check => check_no (other_3). 
# If duplicates ang check_no. - duplicate else - hindi

# for double trn_no cases:
# Check amount if same, before declaring it as duplicate. (Observe muna)



def compare_passbook_to_gl():
    pass



# db.col.update(
#     {filter : "value"},
#     {
#         "$set" : {
#             "all" : "values",
#             "new_values" : "except mongoID"
#         }
#     },
#     upsert = True
# )