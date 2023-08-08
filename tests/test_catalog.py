from laktory.models import Catalog


def test_model():
    cat = Catalog(
        name="lakehouse",
    )

    assert cat.name == "lakehouse"
    assert cat.full_name == "lakehouse"


def test_create():
    cat = Catalog(name="laktory_testing",)
    cat.create(if_not_exists=True)
    assert cat.exists()
    cat.delete(force=True)
    assert not cat.exists()


if __name__ == "__main__":
    test_model()
    test_create()
