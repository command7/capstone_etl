from Imdb_Etl.ETLManager import ETLManager


def main():
    test = ETLManager()
    test.show_all_data()
    test.start_transformations()
    test.stop_spark_cluster()


main()
