import configparser
import os
import boto3
from datetime import datetime, timezone


class S3Manager:

    def __init__(self):
        self.initiation_time = datetime.now(timezone.utc)
        self.bucket_details = dict()
        self.file_upload_times = dict()

        self.parse_configurations()

        self.s3_resource = boto3.resource('s3')

        self.title_basics_paths = list()
        self.title_principals_paths = list()
        self.title_ratings_paths = list()
        self.name_basics_paths = list()

        self.parse_buckets_for_keys()

    def get_initiation_time(self):
        return self.initiation_time

    def add_file_upload_time(self, file_name, file_upload_time):
        self.file_upload_times[file_name] = datetime.fromisoformat(str(file_upload_time))

    def get_file_upload_time(self, file_name):
        return self.file_upload_times[file_name]

    def get_principals_bucket(self):
        return self.bucket_details["title_principals"]

    def get_name_bucket(self):
        return self.bucket_details["name_basics"]

    def get_basics_bucket(self):
        return self.bucket_details["title_basics"]

    def get_ratings_bucket(self):
        return self.bucket_details["title_ratings"]

    def get_basics_paths(self):
        return self.title_basics_paths

    def get_principals_paths(self):
        return self.title_principals_paths

    def get_ratings_paths(self):
        return self.title_ratings_paths

    def get_names_paths(self):
        return self.name_basics_paths

    def delete_basic_path(self, basic_path_to_delete):
        self.title_basics_paths.remove(basic_path_to_delete)

    def delete_principal_path(self, principal_path_to_delete):
        self.title_principals_paths.remove(principal_path_to_delete)

    def delete_rating_path(self, rating_path_to_delete):
        self.title_ratings_paths.remove(rating_path_to_delete)

    def delete_name_path(self, name_path_to_delete):
        self.name_basics_paths.remove(name_path_to_delete)

    def add_basic_path(self, basic_path_to_add):
        self.title_basics_paths.append(basic_path_to_add)

    def add_principal_path(self, principal_path_to_add):
        self.title_principals_paths.append(principal_path_to_add)

    def add_rating_path(self, rating_path_to_add):
        self.title_ratings_paths.append(rating_path_to_add)

    def add_name_path(self, name_path_to_add):
        self.name_basics_paths.append(name_path_to_add)

    def parse_configurations(self):
        conf_parser = configparser.ConfigParser()
        conf_parser.read_file(open("aws_config.cfg", "r"))

        aws_access_key = conf_parser['Credentials']['AWS_ACCESS_KEY']
        aws_secret_key = conf_parser['Credentials']['AWS_SECRET_KEY']

        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key

        self.bucket_details["title_basics"] = conf_parser["S3_Bucket"]["TITLE_BASICS"]
        self.bucket_details["name_basics"] = conf_parser["S3_Bucket"]["NAME_BASICS"]
        self.bucket_details["title_principals"] = conf_parser["S3_Bucket"]["TITLE_PRINCIPALS"]
        self.bucket_details["title_ratings"] = conf_parser["S3_Bucket"]["TITLE_RATINGS"]

    def parse_basics_bucket(self):
        basics_bucket = self.s3_resource.Bucket(self.get_basics_bucket())
        for each_file_path in basics_bucket.objects.all():
            formatted_file_path = f"s3a://{self.get_basics_bucket()}/{each_file_path.key}"
            file_upload_time = datetime.fromisoformat(str(each_file_path.last_modified))
            if not self.is_file_too_recent(file_upload_time):
                self.add_basic_path(formatted_file_path)

    def parse_principals_bucket(self):
        principals_bucket = self.s3_resource.Bucket(self.get_principals_bucket())
        for each_file_path in principals_bucket.objects.all():
            formatted_file_path = f"s3a://{self.get_principals_bucket()}/{each_file_path.key}"
            file_upload_time = datetime.fromisoformat(str(each_file_path.last_modified))
            if not self.is_file_too_recent(file_upload_time):
                self.add_principal_path(formatted_file_path)

    def parse_ratings_bucket(self):
        ratings_bucket = self.s3_resource.Bucket(self.get_ratings_bucket())
        for each_file_path in ratings_bucket.objects.all():
            formatted_file_path = f"s3a://{self.get_ratings_bucket()}/{each_file_path.key}"
            file_upload_time = datetime.fromisoformat(str(each_file_path.last_modified))
            if not self.is_file_too_recent(file_upload_time):
                self.add_rating_path(formatted_file_path)

    def parse_names_bucket(self):
        name_bucket = self.s3_resource.Bucket(self.get_name_bucket())
        for each_file_path in name_bucket.objects.all():
            formatted_file_path = f"s3a://{self.get_name_bucket()}/{each_file_path.key}"
            file_upload_time = datetime.fromisoformat(str(each_file_path.last_modified))
            if not self.is_file_too_recent(file_upload_time):
                self.add_name_path(formatted_file_path)

    def parse_buckets_for_keys(self):
        self.parse_basics_bucket()
        self.parse_principals_bucket()
        self.parse_ratings_bucket()
        self.parse_names_bucket()

    def list_all_contents(self):
        print("Title Basic Files")
        for each_basic_file in self.get_basics_paths():
            print(each_basic_file)
        print("\n\n")

        print("Title Principal Files")
        for each_principal_file in self.get_principals_paths():
            print(each_principal_file)
        print("\n\n")

        print("Title Rating Files")
        for each_rating_file in self.get_ratings_paths():
            print(each_rating_file)
        print("\n\n")

        print("Name Basic Files")
        for each_name_file in self.get_names_paths():
            print(each_name_file)
        print("\n\n")

    def get_minutes_difference(self, old_time):
        diff_datetime = self.get_initiation_time() - old_time
        diff_minutes = (diff_datetime.days * 24 * 60) + (diff_datetime.seconds / 60)
        return int(diff_minutes)

    def is_file_too_recent(self, file_upload_date):
        # file_upload_date = self.get_file_upload_time(file_path_to_check)
        if self.get_minutes_difference(file_upload_date) < 5:
            return True
        return False

    def remove_conflicting_files(self):
        for each_basic_file in self.get_basics_paths():
            if self.is_file_too_recent(each_basic_file):
                self.delete_basic_path(each_basic_file)

        for each_principal_file in self.get_principals_paths():
            if self.is_file_too_recent(each_principal_file):
                self.delete_principal_path(each_principal_file)

        for each_rating_file in self.get_ratings_paths():
            if self.is_file_too_recent(each_rating_file):
                self.delete_rating_path(each_rating_file)

        for each_name_file in self.get_names_paths():
            if self.is_file_too_recent(each_name_file):
                self.delete_name_path(each_name_file)


if __name__ == "__main__":
    s3_manager = S3Manager()
    s3_manager.list_all_contents()

