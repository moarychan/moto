from __future__ import unicode_literals

import importlib


def lazy_load(module_name, element):
    def f(*args, **kwargs):
        module = importlib.import_module(module_name, "moto")
        return getattr(module, element)(*args, **kwargs)

    return f


mock_s3 = lazy_load(".s3", "mock_s3")

# import logging
# logging.getLogger('boto').setLevel(logging.CRITICAL)

__title__ = "moto"
__version__ = "2.2.5.dev"


# try:
#     # Need to monkey-patch botocore requests back to underlying urllib3 classes
#     from botocore.awsrequest import (
#         HTTPSConnectionPool,
#         HTTPConnectionPool,
#         HTTPConnection,
#         VerifiedHTTPSConnection,
#     )
# except ImportError:
#     pass
# else:
#     HTTPSConnectionPool.ConnectionCls = VerifiedHTTPSConnection
#     HTTPConnectionPool.ConnectionCls = HTTPConnection
