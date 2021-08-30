from __future__ import unicode_literals

from .responses import BlobResponseInstance

url_bases = [
    # mapping to localhost
    "http?://(*).blob.core.windows.net"
]

url_paths = {
    # subdomain key of path-based bucket
    "{0}/(?P<bucket_name_path>[^/]+)/(?P<key_name>.+)": BlobResponseInstance.blob_response
}
