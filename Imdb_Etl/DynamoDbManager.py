import boto3
import configparser
from boto3.dynamodb.conditions import Key


class DynamoDbManager:
    def __init__(self):
        db_access_key, db_secret_key = DynamoDbManager.get_db_credentials()
        self.db_resource = boto3.resource("dynamodb",
                                          region_name='us-east-1',
                                          aws_access_key_id=db_access_key,
                                          aws_secret_access_key=db_secret_key)
        self.db_table = self.db_resource.Table("imdb_etl_stats")

    def get_sk_count(self, attribute_name):
        response = self.db_table.query(KeyConditionExpression=Key('stat_name').eq(attribute_name))
        return response["Items"][0]["sk_value"]

    def increment_sk_count(self, attribute_name):
        response = self.db_table.update_item(Key={"stat_name": attribute_name},
                                             UpdateExpression="set sk_value = sk_value + :val",
                                             ExpressionAttributeValues={
                                                 ':val': 1
                                             })

    def get_media_details_starting_sk(self):
        current_value = self.get_sk_count("media_details_starting_sk")
        self.increment_sk_count("media_details_starting_sk")
        return current_value

    def get_media_type_starting_sk(self):
        current_value = self.get_sk_count("media_type_starting_sk")
        self.increment_sk_count("media_type_starting_sk")
        return current_value

    def get_series_details_starting_sk(self):
        current_value = self.get_sk_count("series_details_starting_sk")
        self.increment_sk_count("series_details_starting_sk")
        return current_value

    @staticmethod
    def get_db_credentials():
        conf_parser = configparser.ConfigParser()
        conf_parser.read_file(open("aws_config.cfg", "r"))

        db_access_key = conf_parser['DynamoDbCredentials']['AWS_ACCESS_KEY']
        db_secret_key = conf_parser['DynamoDbCredentials']['AWS_SECRET_KEY']

        return db_access_key, db_secret_key


if __name__ == "__main__":
    test = DynamoDbManager()
    print(test.get_media_details_starting_sk())
    print(test.get_media_type_starting_sk())
    print(test.get_series_details_starting_sk())
