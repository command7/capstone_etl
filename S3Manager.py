import configparser
import os

class S3Manager:

    def __init__(self):
        self.title_basics_bucket = None
        self.name_basics_bucket = None
        self.title_principals_bucket = None
        self.title_ratings_bucket = None
        self.parseConfigurations()

    def parseConfigurations(self):
        conf_parser = configparser.ConfigParser()
        conf_parser.read_file(open("aws_config.cfg", "r"))

        aws_access_key = conf_parser['Credentials']['AWS_ACCESS_KEY']
        aws_secret_key = conf_parser['Credentials']['AWS_SECRET_KEY']

        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key

        self.title_basics_bucket = conf_parser["S3_Bucket"]["TITLE_BASICS"]
        self.name_basics_bucket = conf_parser["S3_Bucket"]["NAME_BASICS"]
        self.title_principals_bucket = conf_parser["S3_Bucket"]["TITLE_PRINCIPALS"]
        self.title_ratings_bucket = conf_parser["S3_Bucket"]["TITLE_RATINGS"]



