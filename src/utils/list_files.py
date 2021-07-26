from pathlib import Path


def list_files(path):

    files_list = []
    for elem in Path(path).rglob('*.csv'):
        files_list.append(str(elem))
    return files_list

# list_files('C:\\Users\\kevec\\PycharmProjects\\pythonProject\\Old_venv\\Data\\tennis_atp-master')

for file in list_files('C:\\Users\\kevec\\PycharmProjects\\pythonProject\\Old_venv\\Data\\tennis_atp-master'):
    print(file)