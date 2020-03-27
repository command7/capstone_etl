from Imdb_Etl.ETLManager import ETLManager


def main():
    test = ETLManager()
    test.show_all_data()
    test.transform_series_details_dim()
    test.stop_spark_cluster()


main()
