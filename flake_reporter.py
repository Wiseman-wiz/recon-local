import csv
from pprint import pprint

class Reporter:
    def __init__(self, report_path, update):
        self.report_path = report_path
        self.update = update


    def generate_reports(self, fields, contents):
        with open(self.report_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            # pprint(contents)
            writer.writerows(contents)

    
    def get_old_columns(self):
        with open(self.report_path , mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter = ',')
            list_of_column_names = []
            for row in csv_reader:
                list_of_column_names+=row
                break
            # print(list_of_column_names)
            return list_of_column_names
    
    def get_old_contents(self):
        old_content = []
        with open(self.report_path , mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = ',')
            for row in csv_reader:
                old_content.append(row)
            # pprint(old_content)
            return old_content
