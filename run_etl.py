from Imdb_Etl.ETLManager import ETLManager


def main():
    test = ETLManager()
    test.show_all_data()
    test.transform_media_details_dim()


main()
