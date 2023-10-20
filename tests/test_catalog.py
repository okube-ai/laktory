from datetime import datetime

from laktory.models import Catalog


def test_model():
    cat = Catalog(
        name="lakehouse",
    )

    assert cat.name == "lakehouse"
    assert cat.full_name == "lakehouse"


def test_create():
    # Timestamp is included in catalog name to prevent conflicts when running
    # multiple tests in parallel
    catalog_name = "laktory_testing_" + str(datetime.now().timestamp()).replace(".", "")

    cat = Catalog(name=catalog_name)
    cat.create(if_not_exists=True)
    assert cat.exists()
    cat.delete(force=True)
    assert not cat.exists()


if __name__ == "__main__":
    test_model()
    test_create()
