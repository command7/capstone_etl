from Imdb_Etl.BucketManager import BucketManager


class TitleBasicsManager(BucketManager):
    def __init__(self, initiation_time):
        super().__init__(initiation_time)
        self.set_path_to_process("s3a://imdbtitlebasicsz/processing/*/*/*/*/*.parquet")
        self.set_bucket_name(BucketManager.get_bucket_name_from_config("title_basics"))
        self.set_s3_resource()
        self.parse_bucket()
        self.change_files_to_processing_status()