import pytest


@pytest.mark.parametrize("dataLayer", [
    ('EXT'), ('TRN'), ('LOD'), ('BSE'), ('SUM')])
def test_getLogicalSchemaDataLayer(conf, dataLayer):

    dl = CONF.getLogicalSchemaDataLayer(dataLayer)

    assert dl is not None
