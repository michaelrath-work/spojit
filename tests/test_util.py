import pytest

from spojit.util import date_time_to_timestamp
from spojit.util import resource_similarity


def test_date_time_to_timestamp():
    assert 1497967200 == date_time_to_timestamp("2017-06-20T16:00:00Z")


def test_resource_similarity():
    assert pytest.approx(0.5, 0.01) == resource_similarity(
        ["a", "b", "c", "d"], ["a", "c", "e"]
    )

    assert pytest.approx(0.5, 0.01) == resource_similarity(
        ["a", "c", "e"], ["a", "b", "c", "d"]
    )

    assert pytest.approx(0.0, 0.01) == resource_similarity(["a", "a"], ["b"])

    assert pytest.approx(0.5, 0.01) == resource_similarity(["a", "a"], ["b", "a"])
