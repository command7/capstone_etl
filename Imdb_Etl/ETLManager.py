from Imdb_Etl.S3Manager import S3Manager
from Imdb_Etl.DynamoDbManager import DynamoDbManager
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import configparser


class ETLManager:
    def __init__(self):
        self.spark = None
        self.basics_data = None
        self.principals_data = None
        self.ratings_data = None
        self.names_data = None
        self.episodes_data = None

        self.s3_manager = S3Manager()
        self.dynamo_db_manager = DynamoDbManager()
        self.initialize_spark_session()
        self.load_all_data()
        self.add_prefixes()

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

    def get_episodes_bucket_path(self):
        return self.s3_manager.get_processing_path_for_episodes()

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

        aws_access_key = conf_parser['S3Credentials']['AWS_ACCESS_KEY']
        aws_secret_key = conf_parser['S3Credentials']['AWS_SECRET_KEY']

        return aws_access_key, aws_secret_key

    def set_aws_credentials(self):
        aws_access_key, aws_secret_key = ETLManager.get_aws_credentials()
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

    def stop_spark_cluster(self):
        self.spark.stop()

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

    def load_episodes_data(self):
        self.episodes_data = self.read_parquet_file(self.get_episodes_bucket_path())

    def load_all_data(self):
        self.load_basics_data()
        self.load_principals_data()
        self.load_ratings_data()
        self.load_names_data()
        self.load_episodes_data()

    def add_prefix_to_basics_data(self):
        self.basics_data = self.basics_data.select([F.col(c).alias("tb_" + c) for c in self.basics_data.columns])

    def add_prefix_to_names_data(self):
        self.names_data = self.names_data.select([F.col(c).alias("nb_" + c) for c in self.names_data.columns])

    def add_prefix_to_ratings_data(self):
        self.ratings_data = self.ratings_data.select([F.col(c).alias("tr_" + c) for c in self.ratings_data.columns])

    def add_prefix_to_principals_data(self):
        self.principals_data = self.principals_data.select(
            [F.col(c).alias("tp_" + c) for c in self.principals_data.columns])

    def add_prefix_to_episodes_data(self):
        self.episodes_data = self.episodes_data.select([F.col(c).alias("te_" + c) for c in self.episodes_data.columns])

    def add_prefixes(self):
        self.add_prefix_to_basics_data()
        self.add_prefix_to_names_data()
        self.add_prefix_to_principals_data()
        self.add_prefix_to_ratings_data()
        self.add_prefix_to_episodes_data()

    def show_ratings_data(self):
        self.get_ratings_data().show()

    def show_basics_data(self):
        self.get_basics_data().show()

    def show_principals_data(self):
        self.get_principals_data().show()

    def show_names_data(self):
        self.get_names_data().show()

    def show_all_data(self):
        self.show_basics_data()
        self.show_principals_data()
        self.show_ratings_data()
        self.show_names_data()

    def transform_media_details_dim(self):
        initial_sk = self.dynamo_db_manager.get_media_details_starting_sk()
        w = Window.orderBy('tb_primaryTitle')
        media_details_dim = self.basics_data.withColumn("media_details_sk", F.row_number().over(w) + initial_sk) \
            .select(F.col("media_details_sk"),
                    F.col("tb_primaryTitle").alias("primary_title"),
                    F.col("tb_originalTitle").alias("original_title"),
                    F.col("tb_titleType").alias("media_type"),
                    F.col("tb_genre").alias("genre"))
        media_details_dim.show()
        last_media_details_sk = media_details_dim \
            .sort(F.desc("media_details_sk")) \
            .first().media_details_sk
        self.dynamo_db_manager.update_media_details_starting_sk(last_media_details_sk)
        return media_details_dim

    def transform_bridge_dimensions(self):
        member_dim_initial_sk = self.dynamo_db_manager.get_media_member_starting_sk()
        member_bridge_initial_sk = self.dynamo_db_manager.get_member_bridge_starting_sk()

        joined_df = self.basics_data.join(self.principals_data,
                                          self.basics_data.tb_tconst == self.principals_data.tp_tconst,
                                          how="left")
        joined_df = joined_df.join(self.names_data,
                                   joined_df.tp_nconst == self.names_data.nb_nconst,
                                   how="left").sort(F.asc("tb_tconst"),
                                                    F.asc("tp_ordering"))
        member_window = Window.orderBy('tp_tconst')
        media_member_dim = joined_df.withColumn("media_member_key",
                                                F.row_number().over(member_window) + member_dim_initial_sk) \
            .select(F.col("media_member_key"),
                    F.col("tp_nconst").alias("member_id"),
                    F.col("nb_primaryname").alias("primary_name"),
                    F.col("tp_job").alias("job_title"),
                    F.col("tp_category").alias("job_category"),
                    F.col("nb_birthyear").alias("birth_year"),
                    F.col("nb_deathyear").alias("death_year"),
                    F.col("nb_primaryprofession").alias("primary_profession"))

        media_member_bridge = joined_df.select(F.col("tb_tconst"),
                                               F.col("tp_nconst"),
                                               F.col("tp_ordering"))

        media_member_bridge = media_member_bridge.join(media_member_dim,
                                                       media_member_bridge.tp_nconst == media_member_dim.member_id,
                                                       how="inner") \
            .sort(F.asc("tb_tconst"),
                  F.asc("media_member_key")) \
            .select(F.col("tb_tconst").alias("media_member_group_key"),
                    F.col("media_member_key"))

        bridge_window = Window.orderBy(media_member_bridge.media_member_group_key)
        media_member_bridge = media_member_bridge\
            .withColumn("rank", F.dense_rank().over(bridge_window) + member_bridge_initial_sk)\
            .select(F.col("rank").alias("media_member_group_key"),
                    F.col("media_member_key"))

        last_member_bridge_starting_sk = media_member_bridge\
            .sort(F.desc("media_member_group_key"))\
            .first().media_member_group_key
        last_media_member_starting_sk = media_member_dim\
            .sort(F.desc("media_member_key"))\
            .first().media_member_key
        self.dynamo_db_manager.update_media_member_starting_sk(last_media_member_starting_sk)
        self.dynamo_db_manager.update_member_bridge_starting_sk(last_member_bridge_starting_sk)

        media_member_bridge.show()
        media_member_dim.show()

        return media_member_dim, media_member_bridge

    def transform_series_details_dim(self):
        initial_sk = self.dynamo_db_manager.get_series_details_starting_sk()
        series_window = Window.orderBy('te_tconst')
        series_details_dim = self.episodes_data.withColumn("series_details_sk",
                                                           F.row_number().over(series_window) + initial_sk) \
            .select(F.col("series_details_sk"),
                    F.col("te_tconst").alias("series_episode_id"),
                    F.col("te_parenttconst").alias("series_parent_id"),
                    F.col("te_seasonnumber").alias("season_number"),
                    F.col("te_episodenumber").alias("episode_number"))

        series_details_dim.show()
        last_series_details_sk = series_details_dim \
            .sort(F.desc("series_details_sk")) \
            .first().series_details_sk
        self.dynamo_db_manager.update_series_details_starting_sk(last_series_details_sk)
        series_details_dim.write.parquet("s3://imdbtitleepisodes/output",mode="overwrite")

        return series_details_dim

    def transform_starting_date_dim(self):
        date_window = Window.orderBy("tb_primarytitle")
        starting_date_dim = self.basics_data.withColumn("starting_date_sk", F.row_number().over(date_window) + 1) \
            .select(F.col("starting_date_sk"),
                    F.col("tb_startyear").alias("starting_year"))
        return starting_date_dim

    def transform_ending_date_dim(self):
        date_window = Window.orderBy("tb_primarytitle")
        ending_date_dim = self.basics_data.withColumn("ending_date_sk", F.row_number().over(date_window) + 1) \
            .select(F.col("ending_date_sk"),
                    F.col("tb_endyear").alias("ending_year"))
        return ending_date_dim
