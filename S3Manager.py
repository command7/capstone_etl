import configparser
import os
import boto3


class S3Manager:

    def __init__(self):
        self.bucket_details = dict()

        self.parse_configurations()

        self.s3_resource = boto3.resource('s3')

        self.title_basics_paths = list()
        self.title_principals_paths = list()
        self.title_ratings_paths = list()
        self.name_basics_paths = list()

        self.parse_bucket_for_keys()

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

    def add_basic_path(self, basic_path_to_add):
        self.title_ratings_paths.append(basic_path_to_add)

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

    def get_basic_keys(self):
        basics_bucket = self.s3_resource.Bucket(self.get_basics_bucket())
        for each_file_path in basics_bucket.objects.all():
            formatted_file_path = f"s3a://{self.get_basics_bucket()}/{each_file_path.key}"
            self.add_basic_path(formatted_file_path)

    def get_principal_keys(self):
        principals_bucket = self.s3_resource.Bucket(self.get_principals_bucket())
        for each_file_path in principals_bucket.objects.all():
            formatted_file_path = f"s3a://{self.get_principals_bucket()}/{each_file_path.key}"
            self.add_principal_path(formatted_file_path)

    def get_rating_keys(self):
        ratings_bucket = self.s3_resource.Bucket(self.get_ratings_bucket())
        for each_file_path in ratings_bucket.objects.all():
            formatted_file_path = f"s3a://{self.get_ratings_bucket()}/{each_file_path.key}"
            self.add_rating_path(formatted_file_path)

    def get_name_keys(self):
        name_bucket = self.s3_resource.Bucket(self.get_name_bucket())
        for each_file_path in name_bucket.objects.all():
            formatted_file_path = f"s3a://{self.get_name_bucket()}/{each_file_path.key}"
            self.add_name_path(formatted_file_path)

    def parse_bucket_for_keys(self):
        self.get_basic_keys()
        self.get_principal_keys()
        self.get_rating_keys()
        self.get_name_keys()

    def list_all_contents(self):
        basics_bucket = self.s3_resource.Bucket(self.get_basics_bucket())
        for each_item in basics_bucket.objects.all():
            formatted = f"s3a://{self.get_basics_bucket()}/{each_item.key}"
            print(formatted)


if __name__ == "__main__":
    s3_manager = S3Manager()
    s3_manager.list_all_contents()
