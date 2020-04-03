from Imdb_Etl.ETLManager import ETLManager
import os

error_file_path = "etl_logs/errors.txt"
success_file_path = "etl_logs/success.txt"


def get_open_flag(file_name):
    if os.path.exists(file_name):
        return "a"
    else:
        return "w"


def log_error(message):
    with open(error_file_path, get_open_flag(error_file_path)) as log_file:
        log_file.write(message + "\n")


def log_success(message):
    with open(success_file_path, get_open_flag(success_file_path)) as log_file:
        log_file.write(message + "\n")


def main():
    for file_extension in range(1, 10):
        try:
            etl_execution = ETLManager(file_extension)
            log_success(f'File {file_extension} processed')
            print(f'{file_extension} of 801 completed')
        except Exception as e:
            log_error(f'File {file_extension} failed')
            print(e)


main()