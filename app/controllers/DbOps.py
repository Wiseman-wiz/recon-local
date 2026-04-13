import pymongo
from app.models import Company


class MainOps:
    def __init__(self):
        self.client = pymongo.MongoClient("localhost:27017")
        self.company_code = ""

    def use_db_2(self,db_name):
        return self.client[db_name]

    def use_database(self, request):
        user_id = request.user.id
        print(f'USER_ID => {user_id}')
        company_details = Company.objects.raw(f'SELECT * FROM app_usercompanyassignment where user_id={user_id}')
        self.company_code = company_details[0].company_id
        db_name = f"{self.company_code}_bank_recon"
        print(f'db_name => {db_name}')
        return self.client[db_name]


    def connect(self, database, collection):
        assert isinstance(database, str)
        assert isinstance(collection, str)
        conn = self.client[database][collection]

        return conn


    def ref_db(self, database):
        assert isinstance(database, str)

        return self.client[database]
