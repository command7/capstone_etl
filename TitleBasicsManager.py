from BucketManager import BucketManager
from datetime import datetime, timezone


class TitleBasicsManager(BucketManager):
    def __init__(self, initiation_time):
        super().__init__(initiation_time)
        self.set_path_to_progress("s3a://imdbtitlebasics/precessing/*.parquet")
        self.set_bucket_name(BucketManager.get_bucket_name_from_config("title_basics"))
        self.set_s3_resource()
        self.parse_bucket()
        self.change_files_to_processing_status()


if __name__ == "__main__":
    test = TitleBasicsManager()