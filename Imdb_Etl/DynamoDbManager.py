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

    def increment_sk_count(self, attribute_name, value_to_change):
        response = self.db_table.update_item(Key={"stat_name": attribute_name},
                                             UpdateExpression="set sk_value = :val",
                                             ExpressionAttributeValues={
                                                 ':val': value_to_change
                                             })

    def get_media_details_starting_sk(self):
        return self.get_sk_count("media_details_starting_sk")

    def get_media_member_starting_sk(self):
        return self.get_sk_count("media_member_starting_sk")

    def get_member_bridge_starting_sk(self):
        return self.get_sk_count("member_bridge_starting_sk")

    def get_media_type_starting_sk(self):
        return self.get_sk_count("media_type_starting_sk")

    def get_series_details_starting_sk(self):
        return self.get_sk_count("series_details_starting_sk")

    def get_starting_year_starting_sk(self):
        return self.get_sk_count("starting_date_sk")

    def get_ending_year_starting_sk(self):
        return self.get_sk_count("ending_date_sk")

    def update_media_details_starting_sk(self, value_to_change):
        self.increment_sk_count("media_details_starting_sk",
                                value_to_change)

    def update_media_member_starting_sk(self, value_to_change):
        self.increment_sk_count("media_member_starting_sk",
                                value_to_change)

    def update_member_bridge_starting_sk(self, value_to_change):
        self.increment_sk_count("member_bridge_starting_sk",
                                value_to_change)

    def update_media_type_starting_sk(self, value_to_change):
        self.increment_sk_count("media_type_starting_sk",
                                value_to_change)

    def update_series_details_starting_sk(self, value_to_change):
        self.increment_sk_count("series_details_starting_sk",
                                value_to_change)

    def update_starting_year_starting_sk(self, value_to_change):
        self.increment_sk_count("starting_date_sk",
                                value_to_change)

    def update_ending_year_starting_sk(self, value_to_change):
        self.increment_sk_count("ending_date_sk",
                                value_to_change)

    def reset_sk_counts(self):
        response = self.db_table.update_item(Key={"stat_name": "media_details_starting_sk"},
                                             UpdateExpression="set sk_value = :val",
                                             ExpressionAttributeValues={
                                                 ':val': 1
                                             })
        response = self.db_table.update_item(Key={"stat_name": "media_type_starting_sk"},
                                             UpdateExpression="set sk_value = :val",
                                             ExpressionAttributeValues={
                                                 ':val': 1
                                             })
        response = self.db_table.update_item(Key={"stat_name": "series_details_starting_sk"},
                                             UpdateExpression="set sk_value = :val",
                                             ExpressionAttributeValues={
                                                 ':val': 1
                                             })
        response = self.db_table.update_item(Key={"stat_name": "media_member_starting_sk"},
                                             UpdateExpression="set sk_value = :val",
                                             ExpressionAttributeValues={
                                                 ':val': 1
                                             })
        response = self.db_table.update_item(Key={"stat_name": "member_bridge_starting_sk"},
                                             UpdateExpression="set sk_value = :val",
                                             ExpressionAttributeValues={
                                                 ':val': 1
                                             })
        response = self.db_table.update_item(Key={"stat_name": "starting_date_sk"},
                                            UpdateExpression="set sk_value = :val",
                                            ExpressionAttributeValues={
                                                ':val': 1
                                            })
        response = self.db_table.update_item(Key={"stat_name": "ending_date_sk"},
                                            UpdateExpression="set sk_value = :val",
                                            ExpressionAttributeValues={
                                                ':val': 1
                                            })

    @staticmethod
    def get_db_credentials():
        conf_parser = configparser.ConfigParser()
        conf_parser.read_file(open("aws_config.cfg", "r"))

        db_access_key = conf_parser['DynamoDbCredentials']['AWS_ACCESS_KEY']
        db_secret_key = conf_parser['DynamoDbCredentials']['AWS_SECRET_KEY']

        return db_access_key, db_secret_key
