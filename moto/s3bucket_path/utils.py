from __future__ import unicode_literals
from urllib.parse import urlparse


def parts_name_from_url(url):
    path = urlparse(url).path.lstrip("/")

    parts = path.lstrip("/").split("/")
    if len(parts) == 0 or parts[0] == "":
        return None
    return parts


def container_name_from_url(url):
    parts = parts_name_from_url(url)
    if len(parts) == 0 or parts[0] == "":
        return None
    return parts[0]


def blob_name_path_from_url(url):
    parts = parts_name_from_url(url)
    if len(parts) == 0 or parts[0] == "":
        return None
    return parts[1:]


def bucket_name_from_url(url):
    path = urlparse(url).path.lstrip("/")

    parts = path.lstrip("/").split("/")
    if len(parts) == 0 or parts[0] == "":
        return None
    return parts[0]


def parse_key_name(path):
    return "/".join(path.split("/")[2:])


def is_delete_keys(request, path, bucket_name):
    return (
        path == "/" + bucket_name + "/?delete"
        or path == "/" + bucket_name + "?delete"
        or (
            path == "/" + bucket_name
            and getattr(request, "query_string", "") == "delete"
        )
    )
