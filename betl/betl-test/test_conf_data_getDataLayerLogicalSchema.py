import pytest


@pytest.mark.parametrize("dataLayer", [
    ('EXT'), ('TRN'), ('LOD'), ('BSE'), ('SUM')])
def test_getDataLayerLogicalSchema(conf, dataLayer):

    dl = conf.DATA.getDataLayerLogicalSchema(dataLayer)

    assert dl is not None
