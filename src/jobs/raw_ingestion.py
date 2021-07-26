"""upload files to S3 raw"""
import boto3
import os


from src.utils.list_files import list_files
from src.utils.operations_s3 import upload_s3

for file in (sorted(set(list_files('C:\\Users\\kevec\\PycharmProjects\\pythonProject\\Old_venv\\Data\\tennis_atp-master')))):
    upload_s3(file, 'tennis-app-ck', 'raw/' + os.path.basename(file))

# print(os.path.dirname(path))
# print(os.path.basename(path))
# print(os.path.normpath(path))







