import pytest

from indexclient.indexclient.client import Document, recursive_sort


def create_document(
    did=None,
    hashes=None,
    size=None,
    file_name=None,
    acl=None,
    authz=None,
    urls=None,
    urls_metadata=None,
):
    return Document(
        None,
        did,
        json={
            "hashes": hashes or {},
            "size": size or 1,
            "file_name": file_name or "file.txt",
            "urls": urls or ["one", "two", "three"],
            "acl": acl or ["1", "2", "3"],
            "authz": authz or "/gen3/programs/1/projects/2/3",
            "urls_metadata": urls_metadata
            or {"one": {"1": "one"}, "two": {"2": "two"}, "three": {"3": "three"}},
        },
    )


def test_equals():
    doc1 = create_document()
    doc2 = create_document()
    assert doc1 == doc2

    # Out of order values are still equal because the contents are the same.
    doc1.acl = ["4", "5"]
    doc2.acl = ["5", "4"]
    assert doc1 == doc2


def test_not_equals():
    doc1 = create_document(acl=["1", "2"])
    doc2 = create_document(acl=["2", "3"])
    assert doc1 != doc2


def test_less_than():
    doc1 = create_document(did="11111111-1111-1111-1111-111111111111")
    doc2 = create_document(did="11111111-1111-1111-1111-111111111112")
    assert doc1 < doc2

    # reverse order docs
    docs = [
        create_document(did="11111111-1111-1111-1111-11111111111" + str(suffix))
        for suffix in range(10, 0, -1)
    ]
    # sorted() sorts it in order from lowest to highest did because of __lt__/__gt__
    assert sorted(docs) == docs[::-1]


def test_greater_than():
    doc1 = create_document(did="11111111-1111-1111-1111-111111111111")
    doc2 = create_document(did="11111111-1111-1111-1111-111111111112")
    assert doc2 > doc1

    # reverse order docs
    docs = [
        create_document(did="11111111-1111-1111-1111-11111111111" + str(suffix))
        for suffix in range(10, 0, -1)
    ]
    # sorted() sorts it in order from lowest to highest did because of __lt__/__gt__
    assert sorted(docs) == docs[::-1]


@pytest.mark.parametrize(
    "given, expected",
    [
        (1, 1),
        ("one", "one"),
        ([1, 4, 3, 2], [1, 2, 3, 4]),
        ({"dict": [1, 4, 3, 2]}, {"dict": [1, 2, 3, 4]}),
        ({"one": {"two": [1, 4, 3, 2]}}, {"one": {"two": [1, 2, 3, 4]}}),
    ],
)
def test_recursive_sort(given, expected):
    assert recursive_sort(given) == expected
