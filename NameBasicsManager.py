from BucketManager import BucketManager
from datetime import datetime, timezone


class NameBasicsManager(BucketManager):
    def __init__(self, initiation_time):
        super().__init__(initiation_time)
        self.set_bucket_name(self.get_bucket_name_from_config("name_basics"))
        self.set_s3_resource()
        self.parse_bucket()
        self.change_files_to_processing_status()


if __name__ == "__main__":
    test = NameBasicsManager()