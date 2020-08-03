import os
CREDENTIAL_CLOUD = {
    "AUTH_URL": os.environ.get('CDN_CLOUD_URL'),
    "THRESHOLD": os.environ.get('CDN_CLOUD_THRESHOLD', 5),
    "MAX_RETRY": os.environ.get('CDN_CLOUD_MAX_RETRY', 2),
    "RETRY_DELAY": os.environ.get('CDN_CLOUD_RETRY_DELAY', 2),
}
