from Imdb_Etl.S3Manager import S3Manager


class ETLManager:
    def __init__(self):
        self.s3_manager = S3Manager()