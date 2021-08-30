from __future__ import unicode_literals


from urllib.parse import (
    parse_qs,
    urlparse
)
from moto.packages.httpretty.core import HTTPrettyRequest
from moto.core.responses import _TemplateEnvironmentMixin, ActionAuthenticatorMixin
from moto.core.utils import path_url
from moto.s3bucket_path.utils import parts_name_from_url

from .exceptions import (
    BlobAlreadyExists, BlobClientError
)
from .models import (
    blob_backend,
    FakeBlob,
)

ACTION_MAP = {
    "BLOB": {
        "GET": {
            "uploads": "ListBucketMultipartUploads",
            "DEFAULT": "ListBucket",
        },
        "PUT": {
            "lifecycle": "PutLifecycleConfiguration",
            "DEFAULT": "CreateBucket",
        },
    },
}


class ResponseObject(_TemplateEnvironmentMixin, ActionAuthenticatorMixin):
    def __init__(self, backend):
        super(ResponseObject, self).__init__()
        self.backend = backend
        self.method = ""
        self.path = ""
        self.data = {}
        self.headers = {}

    @property
    def should_autoescape(self):
        return True

    def blob_response(self, request, full_url, headers):
        self.method = request.method
        self.path = self._get_path(request)
        self.headers = request.headers
        if "host" not in self.headers:
            self.headers["host"] = urlparse(full_url).netloc
        try:
            response = self._blob_response(request, full_url, headers)
        except BlobClientError as s3error:
            response = s3error.code, {}, s3error.description

        return self._send_response(response)

    @staticmethod
    def _send_response(response):
        if isinstance(response, str):
            return 200, {}, response.encode("utf-8")
        else:
            status_code, headers, response_content = response
            if not isinstance(response_content, bytes):
                response_content = response_content.encode("utf-8")

            return status_code, headers, response_content

    def _blob_response(self, request, full_url, headers):
        querystring = self._get_querystring(full_url)
        method = request.method

        parts_name = parts_name_from_url(full_url)
        container_name = parts_name[0]
        if not container_name:
            # If no bucket specified, list all buckets
            raise NotImplementedError()

        blob_name_parts = parts_name[1:]
        blob_name = blob_name_parts[-1]
        path = ''
        if len(blob_name_parts) > 1:
            path = blob_name_parts[:-1]
        self.data["ContainerName"] = container_name
        self.data["BlobName"] = blob_name

        if hasattr(request, "body"):
            # Boto
            body = request.body
        else:
            # Flask server
            body = request.data
        if body is None:
            body = b""
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        body = "{0}".format(body).encode("utf-8")

        if method == "GET":
            return self._blob_response_get(
                request, container_name, path, blob_name, querystring)
        elif method == "PUT":
            return self._blob_response_put(
                request, body, container_name, path, blob_name, querystring
            )
        elif method == 'HEAD':
            return self._blob_response_head(
                request, container_name, path, blob_name, querystring)
        else:
            raise NotImplementedError(
                "Method {0} has not been implemented in the S3 backend yet".format(
                    method
                )
            )

    @staticmethod
    def _validate_required_headers(request):
        if not request.headers.get("Content-Length"):
            return 411, {}, "Content-Length required"

        if not request.headers.get("Authorization"):
            return 400, {}, "Authorization required"

        if not request.headers.get("Date") and not request.headers.get("x-ms-date"):
            return 400, {}, "Date or x-ms-date required"

        if not request.headers.get("x-ms-version"):
            return 400, {}, "x-ms-version required"

        if not request.headers.get("Content-Length"):
            return 400, {}, "Content-Length required"

        return None

    def _blob_response_put(
        self, request, body, container_name, path, blob_name, querystring
    ):
        self._set_action("BLOB", "PUT", querystring)
        # self._authenticate_and_authorize_s3_action()

        resp = self._validate_required_headers(request)
        if resp:
            return resp

        self._set_action("BLOB", "PUT", querystring)
        # self._authenticate_and_authorize_s3_action()
        try:
            new_blob = self.backend.put_blob(container_name=container_name, path=path, blob_name=blob_name, value=body)
        except BlobAlreadyExists:
            return 400, {}, "Bucket already exists"

        if request.headers.get("x-amz-bucket-object-lock-enabled", "") == "True":
            new_blob.object_lock_enabled = True
            # new_blob.versioning_status = "Enabled"

        x_ms_client_request_id = None
        if 'HTTP_X_MS_CLIENT_REQUEST_ID' in request.headers.environ.keys():
            x_ms_client_request_id = request.headers.environ['HTTP_X_MS_CLIENT_REQUEST_ID']

        return 201, {
           "ETag": "0x8D968986D69CE76",
            "last-modified": new_blob.last_modified_RFC1123,
            "Content-MD5": "PiWWCnnbxptnTNTsZ6csYg==",
            "x-ms-request-server-encrypted": "true",
            # "x-ms-encryption-key-sha256": request.headers["x-ms-encryption-key-sha256"],
            "x-ms-client-request-id": x_ms_client_request_id,
            "x-ms-encryption-scope": None,
            "x-ms-version-id": None
        }, 'Created'

    def _blob_response_get(self, request, container_name, path, bucket_name, querystring):
        self._set_action("BLOB", "GET", querystring)
        # self._authenticate_and_authorize_s3_action()

        resp = self._validate_required_headers(request)
        if resp:
            return resp

        bucket = self.backend.get_blob(container_name, path, bucket_name)

        # template = self.response_template(S3_BUCKET_GET_RESPONSE)
        return (
            200,
            {},
            bucket.value
        )

    def _blob_response_head(self, request, container_name, path, blob_name, querystring):
        # self._authenticate_and_authorize_s3_action()

        resp = self._validate_required_headers(request)
        if resp:
            return resp

        blob = self.backend.get_blob(container_name, path, blob_name)
        if blob:
            return (
                200,
                {},
                ''
            )
        else:
            return 404, {}, ""

    @staticmethod
    def _get_querystring(full_url):
        parsed_url = urlparse(full_url)
        querystring = parse_qs(parsed_url.query, keep_blank_values=True)
        return querystring

    def _set_action(self, action_resource_type, method, querystring):
        action_set = False
        for action_in_querystring, action in ACTION_MAP[action_resource_type][
            method
        ].items():
            if action_in_querystring in querystring:
                self.data["Action"] = action
                action_set = True
        if not action_set:
            self.data["Action"] = ACTION_MAP[action_resource_type][method]["DEFAULT"]

    @staticmethod
    def _get_path(request):
        if isinstance(request, HTTPrettyRequest):
            path = request.path
        else:
            path = (
                request.full_path
                if hasattr(request, "full_path")
                else path_url(request.url)
            )
        return path


BlobResponseInstance = ResponseObject(blob_backend)
