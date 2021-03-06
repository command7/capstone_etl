from Imdb_Etl.S3Manager import S3Manager
from Imdb_Etl.DynamoDbManager import DynamoDbManager
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import configparser
import psycopg2
from boto3 import resource


class ETLManager:
    def __init__(self):
        self.spark = None
        self.basics_data = None
        self.principals_data = None
        self.ratings_data = None
        self.names_data = None
        self.episodes_data = None
        self.redshift_connection = None
        self.initialize_redshift_connection()

        self.s3_manager = S3Manager()
        self.dynamo_db_manager = DynamoDbManager()
        self.initialize_spark_session()
        self.load_all_data()
        self.add_prefixes()
        self.show_all_data()
        self.start_transformations()
        self.write_data_to_redshift()
        self.stop_spark_cluster()

    def initialize_redshift_connection(self):
        conn_string = "dbname='imdb_data_warehouse' port='5439' user='root' password='rootStudent1!' host='redshift-test-cluster.cehyxlfhnwj9.us-east-1.redshift.amazonaws.com'"
        self.redshift_connection = psycopg2.connect(conn_string);

    def get_redshift_connection(self):
        return self.redshift_connection

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

        aws_access_key = conf_parser['Root']['AWS_ACCESS_KEY']
        aws_secret_key = conf_parser['Root']['AWS_SECRET_KEY']

        return aws_access_key, aws_secret_key

    def set_aws_credentials(self):
        aws_access_key, aws_secret_key = ETLManager.get_aws_credentials()
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

    def stop_spark_cluster(self):
        self.spark.sparkContext.stop()

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
                    F.col("tb_tconst").alias("media_id"),
                    F.col("tb_primaryTitle").alias("primary_title"),
                    F.col("tb_originalTitle").alias("original_title"),
                    F.col("tb_titleType").alias("media_type"),
                    F.col("tb_genre").alias("genre"))
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
        joined_df = joined_df.distinct()
        member_window = Window.orderBy('tp_tconst')
        media_member_dim = joined_df.withColumn("media_member_key",
                                                F.row_number().over(member_window) + member_dim_initial_sk) \
            .select(F.col("media_member_key"),
                    F.col("tp_tconst").alias("member_tconst"),
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

        member_bridge_join_condition = [media_member_bridge.tp_nconst == media_member_dim.member_id,
                                        media_member_bridge.tb_tconst == media_member_dim.member_tconst]
        media_member_bridge = media_member_bridge.join(media_member_dim,
                                                       member_bridge_join_condition,
                                                       how="inner") \
            .sort(F.asc("tb_tconst"),
                  F.asc("media_member_key")) \
            .select(F.col("tb_tconst").alias("tconst_merge_key"),
                    F.col("tb_tconst").alias("media_member_group_key"),
                    F.col("media_member_key"))

        bridge_window = Window.orderBy(media_member_bridge.media_member_group_key)
        media_member_bridge = media_member_bridge \
            .withColumn("rank", F.dense_rank().over(bridge_window) + member_bridge_initial_sk) \
            .select(F.col("rank").alias("media_member_group_key"),
                    F.col("tconst_merge_key"),
                    F.col("media_member_key"))

        last_member_bridge_starting_sk = media_member_bridge \
            .sort(F.desc("media_member_group_key")) \
            .first().media_member_group_key
        last_media_member_starting_sk = media_member_dim \
            .sort(F.desc("media_member_key")) \
            .first().media_member_key
        self.dynamo_db_manager.update_media_member_starting_sk(last_media_member_starting_sk)
        self.dynamo_db_manager.update_member_bridge_starting_sk(last_member_bridge_starting_sk)

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

        last_series_details_sk = series_details_dim \
            .sort(F.desc("series_details_sk")) \
            .first().series_details_sk
        self.dynamo_db_manager.update_series_details_starting_sk(last_series_details_sk)

        return series_details_dim

    def transform_starting_date_dim(self):
        initial_sk = self.dynamo_db_manager.get_starting_year_starting_sk()

        starting_date_dim = self.basics_data.select("tb_startyear").distinct().na.drop()
        date_window = Window.orderBy(starting_date_dim.tb_startyear)

        starting_date_dim = starting_date_dim.withColumn("starting_date_sk", F.row_number().over(date_window)
                                                         + initial_sk) \
            .select(F.col("starting_date_sk"),
                    F.col("tb_startyear").alias("starting_year"))

        last_starting_date_sk = starting_date_dim \
            .sort(F.desc("starting_date_sk")) \
            .first().starting_date_sk
        self.dynamo_db_manager.update_starting_year_starting_sk(last_starting_date_sk)
        return starting_date_dim

    def transform_ending_date_dim(self):
        initial_sk = self.dynamo_db_manager.get_ending_year_starting_sk()

        ending_date_dim = self.basics_data.select("tb_endyear").distinct().na.drop()
        ending_date_window = Window.orderBy(ending_date_dim.tb_endyear)

        ending_date_dim = ending_date_dim.withColumn("ending_date_sk", F.row_number().over(ending_date_window)
                                                     + initial_sk) \
            .select(F.col("ending_date_sk"),
                    F.col("tb_endyear").alias("ending_year"))
        try:
            last_ending_date_sk = ending_date_dim \
                .sort(F.desc("ending_date_sk")) \
                .first().ending_date_sk
            self.dynamo_db_manager.update_ending_year_starting_sk(last_ending_date_sk)
        except:
            self.dynamo_db_manager.update_ending_year_starting_sk(None)

        return ending_date_dim

    @staticmethod
    def left_inner_join_two_dataframes(left_data_frame,
                                       right_date_frame):
        return left_data_frame.join(right_date_frame,
                                    left_data_frame.media_member_key ==
                                    right_date_frame.media_member_key,
                                    how="left")

    def transform_fact_table(self,
                             transformed_media_details_dim,
                             transformed_starting_date_dim,
                             transformed_ending_date_dim,
                             transformed_series_details_dim,
                             transformed_media_member_dim,
                             transformed_media_member_bridge):

        bridge_join_condition = [
            transformed_media_member_bridge.tconst_merge_key == transformed_media_member_dim.member_tconst,
            transformed_media_member_bridge.media_member_key == transformed_media_member_dim.media_member_key]
        bridge_joined = transformed_media_member_bridge.join(transformed_media_member_dim,
                                                             bridge_join_condition,
                                                             how="left")
        bridge_join_fact_condition = [self.principals_data.tp_tconst == bridge_joined.member_tconst,
                                      self.principals_data.tp_nconst == bridge_joined.member_id]
        fact_dim = self.basics_data.withColumn("runtime_hours", self.basics_data.tb_runtimeminutes / 60) \
            .withColumn("runtime_seconds", self.basics_data.tb_runtimeminutes * 60) \
            .join(self.ratings_data,
                  self.basics_data.tb_tconst == self.ratings_data.tr_tconst,
                  how="left") \
            .join(self.principals_data,
                  self.basics_data.tb_tconst == self.principals_data.tp_tconst,
                  how="left") \
            .join(transformed_media_details_dim,
                  self.basics_data.tb_tconst == transformed_media_details_dim.media_id,
                  how="left") \
            .join(transformed_series_details_dim,
                  self.basics_data.tb_tconst == transformed_series_details_dim.series_episode_id,
                  how="left") \
            .join(transformed_starting_date_dim,
                  self.basics_data.tb_startyear == transformed_starting_date_dim.starting_year,
                  how="left") \
            .join(transformed_ending_date_dim,
                  self.basics_data.tb_endyear == transformed_ending_date_dim.ending_year,
                  how="left") \
            .join(bridge_joined,
                  bridge_join_fact_condition,
                  how="left")

        fact_dim = fact_dim.withColumn("ending_date_sk", F.when(F.isnull(fact_dim.ending_date_sk),
                                                                1) \
                                       .otherwise(fact_dim.ending_date_sk)) \
            .withColumn("starting_date_sk", F.when(F.isnull(fact_dim.starting_date_sk),
                                                   1) \
                        .otherwise(fact_dim.starting_date_sk)) \
            .withColumn("series_details_sk", F.when(F.isnull(fact_dim.series_details_sk),
                                                    1) \
                        .otherwise(fact_dim.series_details_sk)) \
            .select(F.col("series_details_sk"),
                    F.col("starting_date_sk"),
                    F.col("ending_date_sk"),
                    F.col("media_details_sk"),
                    F.col("media_member_group_key"),
                    F.col("tb_isadult").alias("is_adult_picture"),
                    F.col("tb_runtimeminutes").alias("runtime_minutes"),
                    F.col("runtime_hours"),
                    F.col("runtime_seconds"),
                    F.col("tb_tconst")) \
            .distinct() \
            .join(self.ratings_data,
                  self.ratings_data.tr_tconst == fact_dim.tb_tconst,
                  how="left") \
            .drop("tr_tconst") \
            .drop("tb_tconst") \
            .withColumnRenamed("tr_averagerating", "average_rating") \
            .withColumnRenamed("tr_numvotes", "num_votes")

        return fact_dim

    @staticmethod
    def formulate_copy_command(table_name, s3_data_path):
        aws_access_key, aws_secret_key = ETLManager.get_aws_credentials()
        return f"COPY {table_name} FROM '{s3_data_path}' access_key_id '{aws_access_key}' secret_access_key '{aws_secret_key}' FORMAT CSV"

    def empty_temp_bucket(self):
        acc, sec = self.get_aws_credentials()
        s3_resource = resource("s3",
                               region_name='us-east-1',
                               aws_access_key_id=acc,
                               aws_secret_access_key=sec)

        temp_bucket = s3_resource.Bucket('imdbetltemp')

        temp_bucket.objects.all().delete()

    def write_data_to_redshift(self):
        redshift_cursor = self.get_redshift_connection().cursor()

        mdd_copy_command = ETLManager.formulate_copy_command('media_details_dim', 's3://imdbetltemp/mediadetailsdim')
        sdd_copy_command = ETLManager.formulate_copy_command('starting_date_dim', 's3://imdbetltemp/startingdatedim')
        edd_copy_command = ETLManager.formulate_copy_command('ending_date_dim', 's3://imdbetltemp/endingdatedim')
        serdd_copy_command = ETLManager.formulate_copy_command('series_details_dim',
                                                               's3://imdbetltemp/seriesdetailsdim')
        mmb_copy_command = ETLManager.formulate_copy_command('media_member_bridge',
                                                             's3://imdbetltemp/mediamemberbridge')
        mmd_copy_command = ETLManager.formulate_copy_command('media_member_dim', 's3://imdbetltemp/mediamemberdim')
        mf_copy_command = ETLManager.formulate_copy_command('media_fact', 's3://imdbetltemp/mediafact')

        redshift_cursor.execute(mdd_copy_command)
        redshift_cursor.execute(sdd_copy_command)
        redshift_cursor.execute(edd_copy_command)
        redshift_cursor.execute(serdd_copy_command)
        redshift_cursor.execute(mmb_copy_command)
        redshift_cursor.execute(mmd_copy_command)
        redshift_cursor.execute(mf_copy_command)

        self.get_redshift_connection().commit()
        self.get_redshift_connection().close()

    def start_transformations(self):
        media_details_dim = self.transform_media_details_dim()
        starting_date_dim = self.transform_starting_date_dim()
        ending_date_dim = self.transform_ending_date_dim()
        series_details_dim = self.transform_series_details_dim()
        media_member_dim, media_member_bridge = self.transform_bridge_dimensions()
        media_fact = self.transform_fact_table(media_details_dim,
                                               starting_date_dim,
                                               ending_date_dim,
                                               series_details_dim,
                                               media_member_dim,
                                               media_member_bridge)
        media_fact = media_fact.withColumn('runtime_seconds', media_fact.runtime_seconds.cast(IntegerType()))
        media_fact = media_fact.withColumn('runtime_minutes', media_fact.runtime_minutes.cast(IntegerType()))
        media_member_dim = media_member_dim.drop("member_tconst")
        media_member_bridge = media_member_bridge.drop("tconst_merge_key")

        self.empty_temp_bucket()

        media_details_dim.write.csv("s3a://imdbetltemp/mediadetailsdim")
        starting_date_dim.write.csv("s3a://imdbetltemp/startingdatedim")
        ending_date_dim.write.csv("s3a://imdbetltemp/endingdatedim")
        series_details_dim.write.csv("s3a://imdbetltemp/seriesdetailsdim")
        media_member_bridge.write.csv("s3a://imdbetltemp/mediamemberbridge")
        media_member_dim.write.csv("s3a://imdbetltemp/mediamemberdim")
        media_fact.write.csv("s3a://imdbetltemp/mediafact")
