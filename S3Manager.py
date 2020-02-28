import configparser
import os
import boto3
from datetime import datetime, timezone
from TitlePrincipalsManager import TitlePrincipalsManager
from TitleBasicsManager import TitleBasicsManager
from TitleRatingsManager import TitleRatingsManager
from NameBasicsManager import NameBasicsManager


class S3Manager:

    def __init__(self):
        self.initiation_time = datetime.now(timezone.utc)
        self.title_basics_manager = TitleBasicsManager(self.initiation_time)
        self.title_principals_manager = TitlePrincipalsManager(self.initiation_time)
        self.title_ratings_manager = TitleRatingsManager(self.initiation_time)
        self.name_basics_manager = NameBasicsManager(self.initiation_time)


if __name__ == "__main__":
    s3_manager = S3Manager()
    # s3_manager.list_all_contents()


