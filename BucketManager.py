import configparser
import os
import boto3
from datetime import datetime


class BucketManager:
    def __init__(self, initiation_time):
        self.initiation_time = initiation_time
        self.path_for_spark = None
        self.s3_resource = None
        self.bucket_name = None
        self.file_paths = list()

        BucketManager.parse_configurations()

    def get_path_to_process(self):
        return self.path_for_spark

    def get_s3_resource(self):
        return self.s3_resource

    def get_initiation_time(self):
        return self.initiation_time

    def get_file_paths(self):
        return self.file_paths

    def get_bucket_name(self):
        return self.bucket_name

    def set_path_to_process(self, s3_path_to_process):
        self.path_for_spark = s3_path_to_process

    def set_bucket_name(self, bucket_name):
        self.bucket_name = bucket_name

    def set_file_paths(self, bucket_files_list):
        self.file_paths = bucket_files_list

    def set_s3_resource(self):
        self.s3_resource = boto3.resource('s3')

    def add_file_path(self, file_path_to_add):
        self.file_paths.append(file_path_to_add)

    @staticmethod
    def get_bucket_name_from_config(entity_name):
        conf_parser = configparser.ConfigParser()
        conf_parser.read_file(open("aws_config.cfg", "r"))

        return conf_parser["S3_Bucket"][entity_name.upper()]

    def construct_complete_s3_address(self, file_key):
        return f"s3a://{self.get_bucket_name()}/{file_key}"

    @staticmethod
    def parse_configurations():
        conf_parser = configparser.ConfigParser()
        conf_parser.read_file(open("aws_config.cfg", "r"))

        aws_access_key = conf_parser['Credentials']['AWS_ACCESS_KEY']
        aws_secret_key = conf_parser['Credentials']['AWS_SECRET_KEY']

        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key

    def parse_bucket(self):
        s3_bucket = self.s3_resource.Bucket(self.get_bucket_name())
        for each_file_path in s3_bucket.objects.all():
            formatted_file_path = self.construct_complete_s3_address(each_file_path.key)
            file_upload_time = datetime.fromisoformat(str(each_file_path.last_modified))
            if BucketManager.is_file_to_be_processed(each_file_path.key) and not self.is_file_too_recent(file_upload_time):
                self.add_file_path(formatted_file_path)

    @staticmethod
    def extract_processing_bucket_info(file_path_to_process):
        path_components = file_path_to_process.split("/")
        file_path_bucket = path_components[2]
        old_file_path = "/".join(path_components[3:])
        new_file_path = "processing/" + "/".join(path_components[4:])
        return file_path_bucket, old_file_path, new_file_path

    @staticmethod
    def construct_copy_json(source_bucket_name, old_file_path):
        return {
            'Bucket': source_bucket_name,
            'Key': old_file_path
        }

    def move_file_to_processing_status(self, file_path_to_move):
        file_path_bucket, old_file_path, new_file_path = BucketManager.extract_processing_bucket_info(file_path_to_move)
        source_json = BucketManager.construct_copy_json(file_path_bucket, old_file_path)
        self.get_s3_resource().meta.client.copy(source_json,
                                                file_path_bucket,
                                                new_file_path)
        self.get_s3_resource().Object(file_path_bucket, old_file_path).delete()
        return self.construct_complete_s3_address(new_file_path)

    def change_files_to_processing_status(self):
        new_file_paths = list()

        for each_file_path in self.get_file_paths():
            new_file_paths.append(self.move_file_to_processing_status(each_file_path))

        self.set_file_paths(new_file_paths)

    def get_minutes_difference(self, old_time):
        diff_datetime = self.get_initiation_time() - old_time
        diff_minutes = (diff_datetime.days * 24 * 60) + (diff_datetime.seconds / 60)
        return int(diff_minutes)

    def is_file_too_recent(self, file_upload_date):
        if self.get_minutes_difference(file_upload_date) < 0:
            return True
        return False

    @staticmethod
    def is_file_to_be_processed(file_object_key):
        if file_object_key.startswith('tobeprocessed'):
            return True
        return False

    def list_all_files(self):
        for each_file in self.get_file_paths():
            print(each_file)