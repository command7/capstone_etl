from BucketManager import BucketManager
from datetime import datetime, timezone


class TitlePrincipalsManager(BucketManager):
    def __init__(self, initiation_time):
        super().__init__(datetime.now(timezone.utc))
        self.set_bucket_name(self.get_bucket_name_from_config("title_principals"))
        self.set_s3_resource()
        self.parse_bucket()
        self.change_files_to_processing_status()


if __name__ == "__main__":
    test = TitlePrincipalsManager()