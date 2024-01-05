from laktory.models import Catalog


def test_model():
    cat = Catalog(
        name="lakehouse",
    )

    assert cat.name == "lakehouse"
    assert cat.full_name == "lakehouse"


if __name__ == "__main__":
    test_model()
