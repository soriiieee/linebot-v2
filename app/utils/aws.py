import boto3
import os
import sys
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError



class S3Connector:
    def __init__(self, environment,aws_config):
        self.environment = environment
        session = boto3.session.Session(
            aws_access_key_id=aws_config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=aws_config['AWS_SECRET_ACCESS_KEY'],
            region_name=aws_config['AWS_REGION'])
        self.s3_recourse = boto3.resource('s3',
                                          aws_access_key_id=aws_config['AWS_ACCESS_KEY_ID'],
                                          aws_secret_access_key=aws_config['AWS_SECRET_ACCESS_KEY'],
                                          region_name=aws_config['AWS_REGION'])
        
        self.client = session.client("s3")
        if self.environment == 'DEVELOPMENT':
            self.bucket = 'development-resme'
        elif self.environment == 'PRODUCTION':
            self.bucket = 'production-resme'

    def put_content_to_s3(self, s3_path, content, backup_key=None, backup_strategy='file'):
        """
        Function to put string content to s3 for a given s3 path and region
        Arguments:
            s3_path {str} -- s3 path
            content {str} -- string content to be put into s3
        Keyword Arguments:
            s3_client {boto3.client.s3} (default: {None'})-- boto3 client for s3 , new client will be created if not provided
            s3_resource {boto3.resource.s3} (default: {None'}) -- boto3 resource for s3 , new resource will be created if not provided
            region_name {str}(default: {'us-east-1'}) -- AWS region
            backup_key {str} (default: {None}) -- backup key name, if provided and if file already exists in given s3 path a backup of existing content will be taken in same s3 path appedning the sepcified backup_key at the end of file name
        Returns:
            return_object {dict} -- return_object['data'] {str} -- error message if return_object['success'] is False
        """
        return_object = {
            'success': True,
            'data': ''
        }
        try:
            # if backup_key is specified take backup of old content
            # if backup_key:
            #     results = self.client.list_objects(Bucket=self.bucket, Prefix=s3_path)
            #     if 'Contents' in results:
            #         old_content = self.s3_recourse.Object(self.bucket, s3_path).get()['Body'].read().decode('utf-8')
            #         s3_client_.put_object(Body=old_content, Bucket=bucket,
            #                               Key=rename_file(key, backup_key, backup_strategy))
            # put current content to s3
            s3_put_response = self.client.put_object(Body=content, Bucket=self.bucket, Key=s3_path)
            if s3_put_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception('Unable to put data to s3: {0}'.format(s3_put_response))

        except Exception as e:
            return_object['success'] = False
            exception_message = "message: {0}\nline no:{1}\n".format(str(e), sys.exc_info()[2].tb_lineno)
            return_object['data'] = exception_message
        finally:
            return return_object

    def file_upload(self, target_file, object_path, file_name):
        self.client.upload_file(Filename=target_file, Key='/'.join((object_path, file_name)),
                                Bucket=self.bucket)

    def check_exists(self, key):
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            return int(e.response['Error']['Code']) != 404
        return True

    def download(self, file, object_path, download_file):
        self.client.download_file(Filename=file, Key='/'.join((object_path, download_file)), Bucket=self.bucket)

    def read_file(self, file, object_path):
        if object_path:
            key_path = '/'.join((object_path, file))
        else:
            key_path = file
        s3_object = self.s3_recourse.Object(self.bucket, key_path)
        return s3_object.get()["Body"].read().decode('utf-8')

    def list_objects(self, folder):
        bucket = self.s3_recourse.Bucket(self.bucket)
        object_list = bucket.objects.filter(Prefix=folder)
        file_list = [x.key for x in object_list]
        return file_list


class DynamodbConnector:
    def __init__(self, aws_config):
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=aws_config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=aws_config['AWS_SECRET_ACCESS_KEY'],
            region_name=aws_config['AWS_REGION']
        )

    def get_sample_record(self, instrument_code, previous_sample, genre, tempo, beat):
        pk = self.get_sample_record_pk(previous_sample=previous_sample, instrument_code=instrument_code,
                                       genre=genre, tempo=str(tempo), beat=beat)
        table = self.dynamodb.Table(f'PRODUCTION-MUSIC_ENGINE-sample')
        response = table.query(
            KeyConditionExpression=Key('PK').eq(pk)
        )
        data = response['Items']
        return data

    def get_sample_record_pk(self, previous_sample='', instrument_code='', genre='', tempo='', beat=''):
        return get_str_md5('+'.join([previous_sample, str(instrument_code), str(genre), str(tempo), str(beat)]))

