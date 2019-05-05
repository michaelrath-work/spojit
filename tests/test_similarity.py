from collections import defaultdict

from spojit.io import IssueToOtherSimilarityHandler, IssueToCodeSimilarityHandler


class I2OHandler(IssueToOtherSimilarityHandler):
    def __init__(self, i_2_o: dict):
        self.i_2_o = i_2_o

    def get_sim(self, issue_id: str, other_id: str):
        issue = self.i_2_o.get(issue_id, None)
        return issue.get(other_id, None) if issue else None

    def __str__(self):
        n = len(self.i_2_o.keys())
        f = next(iter(self.i_2_o.items())) if n > 0 else None
        return "I2OHandler with {} entries, 1st: {}".format(n, f)


def _objects_2_double_lookup(key1: str, key2: str, objects: list):
    mapping = defaultdict(defaultdict)

    for e in objects:
        mapping[e[key1]].update({e[key2]: e["v"]})

    return mapping


class I2CHandler(IssueToCodeSimilarityHandler):
    def __init__(self, i_2_f: dict):
        self.i_2_f = i_2_f

    def get_sim(self, issue_id: str, commit_hash: str, file_path: str):
        issue = self.i_2_f.get(issue_id, None)
        commit = issue.get(commit_hash, None) if issue else None
        return commit.get(file_path, None) if commit else None

    def __str__(self):
        n = len(self.i_2_f.keys())
        f = next(iter(self.i_2_f.items())) if n > 0 else None
        return "I2CHandler with {} entries, 1st: {}".format(n, f)


def test_similarity_issue_to_commit():
    issue_to_commit = [
        {"c": "aaa", "i": "I-1", "v": 100},
        {"c": "bbb", "i": "I-1", "v": 200},
        {"c": "ccc", "i": "I-2", "v": 300},
    ]

    h = I2OHandler(
        _objects_2_double_lookup(key1="i", key2="c", objects=issue_to_commit)
    )

    assert 100 == h.get_sim("I-1", "aaa")
    assert 200 == h.get_sim("I-1", "bbb")
    assert h.get_sim("aaa", "I-1") is None
    assert h.get_sim("I-X", "xxx") is None


def test_similarity_issue_to_code():
    issue_to_code = {
        "I-1": {
            "aaa": {"path/to/file1.java": 80, "path/to/file2.java": 95},
            "bbb": {"file_x.java": 50},
        },
        "I-2": {"aaa": {"file_y.java": 33}},
    }

    h = I2CHandler(issue_to_code)

    assert 50 == h.get_sim("I-1", "bbb", "file_x.java")
    assert 33 == h.get_sim("I-2", "aaa", "file_y.java")
    assert h.get_sim("I-1", "bbb", "INVALID_FILE") is None
    assert h.get_sim("I-1", "INVALID_COMMIT", "file_x") is None
    assert h.get_sim("INVALID_ISSUE", "bbb", "file_x") is None
