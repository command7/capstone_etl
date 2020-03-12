from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.config("spark.jars.packages",
                                         "org.apache.hadoop:hadoop-aws:2.7.6") \
    .appName("test application").getOrCreate()



spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA2ITTLRCLOTSBZJW6")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

basics_data = spark.read.parquet("s3a://imdbtitlebasics/processing/*/*/*/*/*.parquet")
basics_data = basics_data.select([F.col(c).alias("tb_" + c) for c in basics_data.columns])


w = Window.orderBy('tb_primaryTitle')
media_details_dim = basics_data.withColumn("media_details_sk", F.row_number().over(w) + 1) \
            .select(F.col("media_details_sk"),
                    F.col("tb_primaryTitle").alias("primary_title"),
                    F.col("tb_originalTitle").alias("original_title"))
                    # F.col("genre"))
media_details_dim.show()
# media_details_dim.write.parquet("s3a://imdbtitlebasics/testing/test.parquet", mode="overwrite")

"""
aws emr add-steps --cluster-id j-1WZGCEK8TVGVX --steps Name=imdbetlapp,Jar=command-runner.jar,Args=[spark-submit,--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--py-files,s3://imdbetlapp/Imdb_Etl.zip,--files,s3://imdbetlapp/Imdb_Etl/aws_config.cfg,s3://imdbetlapp/run_etl.py],ActionOnFailure=CONTINUE
"""