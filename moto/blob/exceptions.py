from __future__ import unicode_literals

from moto.core.exceptions import RESTError

ERROR_WITH_BUCKET_NAME = """{% extends 'single_error' %}
{% block extra %}<BucketName>{{ blob }}</BucketName>{% endblock %}
"""

ERROR_WITH_KEY_NAME = """{% extends 'single_error' %}
{% block extra %}<KeyName>{{ key_name }}</KeyName>{% endblock %}
"""

ERROR_WITH_ARGUMENT = """{% extends 'single_error' %}
{% block extra %}<ArgumentName>{{ name }}</ArgumentName>
<ArgumentValue>{{ value }}</ArgumentValue>{% endblock %}
"""


class BlobClientError(RESTError):
    # S3 API uses <RequestID> as the XML tag in response messages
    request_id_tag_name = "RequestID"

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("template", "single_error")
        self.templates["blob_error"] = ERROR_WITH_BUCKET_NAME
        super(BlobClientError, self).__init__(*args, **kwargs)


class InvalidArgumentError(BlobClientError):
    code = 400

    def __init__(self, message, name, value, *args, **kwargs):
        kwargs.setdefault("template", "argument_error")
        kwargs["name"] = name
        kwargs["value"] = value
        self.templates["argument_error"] = ERROR_WITH_ARGUMENT
        super(InvalidArgumentError, self).__init__(
            "InvalidArgument", message, *args, **kwargs
        )


class BlobError(BlobClientError):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("template", "blob_error")
        self.templates["blob_error"] = ERROR_WITH_BUCKET_NAME
        super(BlobError, self).__init__(*args, **kwargs)


class BlobAlreadyExists(BlobError):
    code = 409

    def __init__(self, *args, **kwargs):
        super(BlobAlreadyExists, self).__init__(
            "BucketAlreadyExists",
            (
                "The requested blob name is not available. The blob "
                "namespace is shared by all users of the system. Please "
                "select a different name and try again"
            ),
            *args,
            **kwargs
        )


class MissingBlob(BlobError):
    code = 404

    def __init__(self, *args, **kwargs):
        super(MissingBlob, self).__init__(
            "NoSuchBucket", "The specified blob does not exist", *args, **kwargs
        )


class InvalidBucketName(BlobClientError):
    code = 400

    def __init__(self, *args, **kwargs):
        super(InvalidBucketName, self).__init__(
            "InvalidBucketName", "The specified bucket is not valid.", *args, **kwargs
        )

