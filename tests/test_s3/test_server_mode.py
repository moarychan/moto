import boto3
import os
import pytest


# python server.py s3 -p 7000

@pytest.fixture(scope='function')
def credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


def test_operation(credentials):
    mock_s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        endpoint_url='http://127.0.0.1:7000',
    )
    mock_s3.create_bucket(Bucket='my-bucket')


# Raw use
def test_my_model_save_with_raw_use(credentials):
    conn = boto3.resource(service_name='s3',
                          region_name='us-west-1',
                          endpoint_url='http://127.0.0.1:7000')
    conn.create_bucket(Bucket='my-bucket')
    s3 = boto3.client(service_name='s3',
                      region_name='use-east-1',
                      endpoint_url='http://127.0.0.1:7000')
    s3.put_object(Bucket='my-bucket', Key='steve', Body='hello')
    assert conn.Object('my-bucket', 'steve').get()['Body'].read().decode() == 'hello'
