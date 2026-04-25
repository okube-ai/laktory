from laktory.constants import SUPPORTED_BACKENDS


def test_constants():
    assert "terraform" in SUPPORTED_BACKENDS
    assert "tofu" not in SUPPORTED_BACKENDS
