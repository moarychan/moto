# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
import datetime
import pytz
import copy
import tempfile
import threading
import sys

from moto.core import ACCOUNT_ID, BaseBackend, BaseModel
from moto.core.utils import iso_8601_datetime_without_milliseconds_s3, rfc_1123_datetime
from .exceptions import (
    BlobAlreadyExists,
    MissingBlob,
    InvalidBucketName,
)
from ..settings import get_s3_default_key_buffer_size

MAX_BUCKET_NAME_LENGTH = 63
MIN_BUCKET_NAME_LENGTH = 3
DEFAULT_TEXT_ENCODING = sys.getdefaultencoding()


class FakeBlob(BaseModel):
    def __init__(
            self,
            container_name,
            name,
            path,
            value,
            etag=None,
            content_md5=None,
            x_ms_request_server_encrypted=None,
            x_ms_encryption_key_sha256=None,
            x_ms_encryption_scope=None,
            x_ms_version_id=None,
            # x_ms_client_request_id=None,
            x_ms_version="2015-02-21",
            x_ms_date=None,
            blob_content_type=None,
            blob_content_disposition=None,
            blob_type=None,
            x_ms_meta_m1=None,
            x_ms_meta_m2=None,
            content_type=None,
            content_length=None,
            last_modified=None,
            max_buffer_size=None,
            lock_mode=None,
            lock_legal_status="OFF",
            lock_until=None,
    ):
        self.container_name = container_name
        self.name = name
        self.path = path

        self.etag = etag
        self.x_ms_version = x_ms_version
        self.x_ms_date = x_ms_date
        self.blob_content_type = blob_content_type
        self.blob_content_disposition = blob_content_disposition
        self.blob_type = blob_type
        self.x_ms_meta_m1 = x_ms_meta_m1
        self.x_ms_meta_m2 = x_ms_meta_m2
        self.content_type = content_type
        self.content_length = content_length

        self.last_modified = datetime.datetime.utcnow()
        self._max_buffer_size = (
            max_buffer_size if max_buffer_size else get_s3_default_key_buffer_size()
        )
        self._value_buffer = tempfile.SpooledTemporaryFile(self._max_buffer_size)
        self.value = value

        self.lock = threading.Lock()
        self.lock_mode = lock_mode
        self.lock_legal_status = lock_legal_status
        self.lock_until = lock_until

    @property
    def value(self):
        self.lock.acquire()
        self._value_buffer.seek(0)
        r = self._value_buffer.read()
        r = copy.copy(r)
        self.lock.release()
        return r

    @value.setter
    def value(self, new_value):
        self._value_buffer.seek(0)
        self._value_buffer.truncate()

        # Hack for working around moto's own unit tests; this probably won't
        # actually get hit in normal use.
        if isinstance(new_value, str):
            new_value = new_value.encode(DEFAULT_TEXT_ENCODING)
        self._value_buffer.write(new_value)
        self.content_length = len(new_value)

    def copy(self, new_name=None, new_is_versioned=None):
        r = copy.deepcopy(self)
        if new_name is not None:
            r.name = new_name
        if new_is_versioned is not None:
            r._is_versioned = new_is_versioned
            r.refresh_version()
        return r

    def append_to_value(self, value):
        self.contentsize += len(value)
        self._value_buffer.seek(0, os.SEEK_END)
        self._value_buffer.write(value)
        self.last_modified = datetime.datetime.utcnow()

    @property
    def last_modified_RFC1123(self):
        return rfc_1123_datetime(self.last_modified)

    @property
    def response_dict(self):
        res = {
            "container_name": self.container_name,
            "name": self.name,
            "path": self.path,
            "last_modified": self.last_modified_RFC1123,
            "content_length": str(self.size),
        }
        return res

    @property
    def size(self):
        return self.content_length


class FakeContainer:
    def __init__(self, name):
        self.name = name
        self.cors = []
        self.creation_date = datetime.datetime.now(tz=pytz.utc)
        self.object_lock_enabled = False
        self.default_lock_mode = ""
        self.default_lock_days = 0
        self.default_lock_years = 0

    @property
    def creation_date_ISO8601(self):
        return iso_8601_datetime_without_milliseconds_s3(self.creation_date)

    def set_cors(self, rules):
        self.cors = []

        if len(rules) > 100:
            raise MalformedXML()

        for rule in rules:
            assert isinstance(rule["AllowedMethod"], list) or isinstance(
                rule["AllowedMethod"], str
            )
            assert isinstance(rule["AllowedOrigin"], list) or isinstance(
                rule["AllowedOrigin"], str
            )
            assert isinstance(rule.get("AllowedHeader", []), list) or isinstance(
                rule.get("AllowedHeader", ""), str
            )
            assert isinstance(rule.get("ExposedHeader", []), list) or isinstance(
                rule.get("ExposedHeader", ""), str
            )
            assert isinstance(rule.get("MaxAgeSeconds", "0"), str)

            if isinstance(rule["AllowedMethod"], str):
                methods = [rule["AllowedMethod"]]
            else:
                methods = rule["AllowedMethod"]

            for method in methods:
                if method not in ["GET", "PUT", "HEAD", "POST", "DELETE"]:
                    raise InvalidRequest(method)

            self.cors.append(
                CorsRule(
                    rule["AllowedMethod"],
                    rule["AllowedOrigin"],
                    rule.get("AllowedHeader"),
                    rule.get("ExposedHeader"),
                    rule.get("MaxAgeSecond"),
                )
            )

    def delete_cors(self):
        self.cors = []

    @staticmethod
    def template_name_type():
        return "ContainerName"

    @staticmethod
    def template_type():
        # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-s3-bucket.html
        return "Microsoft.Storage/storageAccounts/blobServices/containers"

    @classmethod
    def create_from_template_json(
            cls, resource_name, template_json, region_name
    ):
        raise NotImplementedError()

    @classmethod
    def update_from_cloudformation_json(
            cls, original_resource, new_resource_name, cloudformation_json, region_name,
    ):
        raise NotImplementedError()

    @classmethod
    def delete_from_template_json(
            cls, resource_name, cloudformation_json, region_name
    ):
        blob_backend.delete_container(resource_name)

    def to_config_dict(self):
        """Return the AWS Config JSON format of this S3 bucket.

        Note: The following features are not implemented and will need to be if you care about them:
        - Bucket Accelerate Configuration
        """
        config_dict = {
            "resourceId": self.name,
            "resourceName": self.name
        }

        return config_dict

    @property
    def has_default_lock(self):
        if not self.object_lock_enabled:
            return False

        if self.default_lock_mode:
            return True

        return False

    def default_retention(self):
        now = datetime.datetime.utcnow()
        now += datetime.timedelta(self.default_lock_days)
        now += datetime.timedelta(self.default_lock_years * 365)
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")


class BlobBackend(BaseBackend):
    def __init__(self):
        self.containers = {}

    def create_container(self, container_name):
        if container_name in self.containers:
            raise BlobAlreadyExists(container=container_name)
        if not MIN_BUCKET_NAME_LENGTH <= len(container_name) <= MAX_BUCKET_NAME_LENGTH:
            raise InvalidBucketName()
        new_container = FakeContainer(name=container_name)
        self.containers[container_name] = new_container
        return new_container

    def list_containers(self):
        return self.containers.values()

    def put_blob(
            self,
            container_name,
            blob_name,
            path,
            value,
            lock_mode=None,
            lock_legal_status="OFF",
            lock_until=None,
    ):
        if container_name not in self.containers:
            # raise MissingBucket(container=container_name)
            self.containers[container_name] = dict()

        if blob_name in self.containers[container_name]:
            raise BlobAlreadyExists(blob_name=blob_name)

        if not MIN_BUCKET_NAME_LENGTH <= len(blob_name) <= MAX_BUCKET_NAME_LENGTH:
            raise InvalidBucketName()

        new_blob = FakeBlob(
            container_name,
            blob_name,
            path,
            value,
            lock_mode=lock_mode,
            lock_legal_status=lock_legal_status,
            lock_until=lock_until,
        )
        blobs = self.containers[container_name]
        blobs[path + blob_name] = new_blob
        return new_blob

    def get_blob(self, container_name, path, blob_name):
        try:
            blob = self.containers[container_name][path + blob_name]
            if blob:
                return blob
            else:
                return None
        except KeyError:
            raise MissingBlob(bucket=path + blob_name)


blob_backend = BlobBackend()
