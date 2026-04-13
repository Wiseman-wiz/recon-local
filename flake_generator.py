import os
import re
from flake_reporter import Reporter
import datetime


exemptions = ['__pycache__', '__init__.py']
project_path = input("enter project-folder's path: ")
project_name = project_path.split('/')[-1]
# C:\Users\qgee1\Works\erp-next\borland
save_path = 'flake8_output'
report_path = f'flake8_reports/flake8-report-{project_name}.csv'
today = datetime.datetime.now()
current_day = today.strftime("%d/%m/%Y %H:%M:%S")


def analyzer(flake_filepath, old_fields):
    count_per_issues = {}
    count_per_issues['date'] = current_day
    flake_result = open(flake_filepath)
    contents = flake_result.read()
    flake_result.close()
    flake8_codes = list(re.findall(': (.+?) ', contents))
    # encountered_codes = flake8_codes
    # if old_fields:
    encountered_codes = flake8_codes + old_fields
    deduplicated_codes = list(dict.fromkeys(encountered_codes))
    for code in deduplicated_codes:
        occurence = flake8_codes.count(code)
        # print({
        #     "code" : code,
        #     "occurence" : occurence
        # })
        if code != 'date':
            count_per_issues[code] = occurence
    return count_per_issues


def generate_flake(project_path):
    flake_file = f'flake8_checking-{project_name}.txt'
    flake_filepath = f'flake8_output/{flake_file}'
    old_fields = []
    old_contents = []
    if flake_file in os.listdir(save_path):
        update = True
        reporter = Reporter(report_path, update)
        old_contents = reporter.get_old_contents()
        # print(old_contents)
        # print("=====================")
        old_fields = list(old_contents[0].keys())
        # print(old_fields)
        # print("=====================")
        os.system(f'rm -r {flake_filepath}')
        os.system(f"flake8 --max-complexity 7 {project_path} >> {flake_filepath}")
        content = analyzer(flake_filepath, old_fields)
        # print(content)
        old_contents.append(content)
        current_fields = list(content.keys())
        # print(current_fields)
        reporter.generate_reports(current_fields, old_contents)
       #read from reports/report.csv
    else:
        update = False
        reporter = Reporter(report_path, update)
        os.system(f"flake8 --max-complexity 7 {project_path} >> {flake_filepath}")
        content = analyzer(flake_filepath, old_fields)
        # print(content)
        # print("=====================")
        # print("=====================")
        old_contents.append(content)
        current_fields = list(content.keys())
        reporter.generate_reports(current_fields, old_contents)
        #generate new flake_output and report


generate_flake(project_path)