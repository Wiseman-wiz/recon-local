from app.models import Company


def get_company_code(request):
    user_id = request.user.id
    company_details = Company.objects.raw(
        f'SELECT * FROM app_usercompanyassignment where user_id={user_id}'
    )
    company_code = company_details[0].company_id
    # print(f"userid ===> {user_id}")
    # print(f"company_id ===> {company_code}")
    return company_code


def get_all_companies(request):
    user_id = request.user.id
    
    companies = Company.objects.raw(
        'SELECT * FROM app_usercompanyassignment where user_id= %s', [user_id]
    )

    company_list = [ company.company_id for company in companies ]

    return company_list
