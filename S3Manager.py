import configparser
import os
import boto3


class S3Manager:

    def __init__(self):
        self.bucket_details = dict()

        self.s3_resource = boto3.resource('s3')

        self.parse_configurations()

    def get_principals_bucket(self):
        return self.bucket_details["title_principals"]

    def get_name_bucket(self):
        return self.bucket_details["name_basics"]

    def get_basics_bucket(self):
        return self.bucket_details["title_basics"]

    def get_ratings_bucket(self):
        return self.bucket_details["title_ratings"]

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
