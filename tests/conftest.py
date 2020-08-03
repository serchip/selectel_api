import pytest

from ..selectel_storage import SelectelCDNApi
from ..settings import CREDENTIAL_CLOUD


@pytest.fixture()
async def resource():
    client = SelectelCDNApi(CREDENTIAL_CLOUD['AUTH_URL'], threshold=CREDENTIAL_CLOUD['THRESHOLD'],
                            max_retry=CREDENTIAL_CLOUD['MAX_RETRY'], retry_delay=CREDENTIAL_CLOUD['RETRY_DELAY']
                            )
    yield client
