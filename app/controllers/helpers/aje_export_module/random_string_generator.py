import string
import random

class RandomString_Generator:
    def get_random_filename(pb_or_gl):
        base_filename = "AJE"
        source = string.ascii_letters + string.digits
        result_str = ''.join((random.choice(source) for i in range(8)))
        filename = f"{pb_or_gl}_{base_filename}_{result_str}.xlsx"
        return filename
    
    def generate_AJE_Number(id_inserted):
        str_id = str(id_inserted)
        return f"AJE-{str_id}"



# string of length 8
# print(RandomString_Generator.get_random_filename("GL"))
# print(RandomString_Generator.get_random_filename("PB"))