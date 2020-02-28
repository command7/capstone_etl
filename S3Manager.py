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

    def get_title_basics_manager(self):
        return self.title_basics_manager

    def get_title_principals_manager(self):
        return self.title_principals_manager

    def get_title_ratings_manager(self):
        return self.title_ratings_manager

    def get_name_basics_manager(self):
        return self.name_basics_manager

    def list_all_basic_files(self):
        self.get_title_basics_manager().list_all_files()

    def list_all_principal_files(self):
        self.get_title_principals_manager().list_all_files()

    def list_all_rating_files(self):
        self.get_title_ratings_manager().list_all_files()

    def list_all_name_files(self):
        self.get_name_basics_manager().list_all_files()

    def list_all_files(self):
        print("Title Basics\n")
        self.list_all_basic_files()

        print("\nTitlePrincipals\n")
        self.list_all_principal_files()

        print("\nTitle Ratings\n")
        self.list_all_rating_files()

        print("\nName Basics\n")
        self.list_all_name_files()

if __name__ == "__main__":
    s3_manager = S3Manager()
    s3_manager.list_all_files()


