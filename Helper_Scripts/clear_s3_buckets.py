import configparser
import boto3

conf_parser = configparser.ConfigParser()
conf_parser.read_file(open("../aws_config.cfg", "r"))

s3_access_key = conf_parser['S3Credentials']['AWS_ACCESS_KEY']
s3_secret_key = conf_parser['S3Credentials']['AWS_SECRET_KEY']

s3_resource = boto3.resource("s3",
                             region_name='us-east-1',
                             aws_access_key_id=s3_access_key,
                             aws_secret_access_key=s3_secret_key)

title_basics_bucket = s3_resource.Bucket('imdbtitlebasics')
title_principals_bucket = s3_resource.Bucket('imdbtitleprincipals')
title_ratings_bucket = s3_resource.Bucket('imdbtitleratings')
name_basics_bucket = s3_resource.Bucket('imdbnamebasics')

title_basics_bucket.objects.all().delete()
title_principals_bucket.objects.all().delete()
title_ratings_bucket.objects.all().delete()
name_basics_bucket.objects.all().delete()
