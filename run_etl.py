from Imdb_Etl.ETLManager import ETLManager


def main():
    test = ETLManager()
    test.stop_spark_cluster()


main()
