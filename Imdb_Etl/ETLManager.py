from Imdb_Etl.S3Manager import S3Manager
from pyspark.sql import SparkSession
import configparser

class ETLManager:
    def __init__(self):
        self.spark = None
        self.basics_data = None
        self.principals_data = None
        self.ratings_data = None
        self.names_data = None

        self.s3_manager = S3Manager()
        self.initialize_spark_session()
        self.load_all_data()

    def get_s3_manager(self):
        return self.s3_manager

    def get_basics_bucket_path(self):
        return self.s3_manager.get_processing_path_for_basics()

    def get_principals_bucket_path(self):
        return self.s3_manager.get_processing_path_for_principals()

    def get_ratings_bucket_path(self):
        return self.s3_manager.get_processing_path_for_ratings()

    def get_names_bucket_path(self):
        return self.s3_manager.get_processing_path_for_names()

    def get_ratings_data(self):
        return self.ratings_data

    def get_basics_data(self):
        return self.basics_data

    def get_principals_data(self):
        return self.principals_data

    def get_names_data(self):
        return self.names_data

    @staticmethod
    def get_aws_credentials():
        conf_parser = configparser.ConfigParser()
        conf_parser.read_file(open("aws_config.cfg", "r"))

        aws_access_key = conf_parser['Credentials']['AWS_ACCESS_KEY']
        aws_secret_key = conf_parser['Credentials']['AWS_SECRET_KEY']

        return aws_access_key, aws_secret_key

    def set_aws_credentials(self):
        aws_access_key, aws_secret_key = ETLManager.get_aws_credentials()
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

    def initialize_spark_session(self):
        try:
            self.spark = SparkSession.builder.config("spark.jars.packages",
                                                "org.apache.hadoop:hadoop-aws:2.7.6") \
                .appName("test application").getOrCreate()

            self.set_aws_credentials()
        except Exception as e:
            print(e)

    def read_parquet_file(self, file_path_to_read):
        return self.spark.read.parquet(file_path_to_read)

    def load_basics_data(self):
        self.basics_data = self.read_parquet_file(self.get_basics_bucket_path())

    def load_principals_data(self):
        self.principals_data = self.read_parquet_file(self.get_principals_bucket_path())

    def load_ratings_data(self):
        self.ratings_data = self.read_parquet_file(self.get_ratings_bucket_path())

    def load_names_data(self):
        self.names_data = self.read_parquet_file(self.get_names_bucket_path())

    def load_all_data(self):
        self.load_basics_data()
        self.load_principals_data()
        self.load_ratings_data()
        self.load_names_data()

    def show_ratings_data(self):
        self.get_ratings_data().show(5)

    def show_basics_data(self):
        self.get_basics_data().show(5)

    def show_principals_data(self):
        self.get_principals_data().show(5)

    def show_names_data(self):
        self.get_names_data().show(5)

    def show_all_data(self):
        self.show_basics_data()
        self.show_principals_data()
        self.show_ratings_data()
        self.show_names_data()

if __name__ == "__main__":
    test = ETLManager()
    test.show_all_data()