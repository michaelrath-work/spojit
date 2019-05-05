import operator

from collections import defaultdict


def _split_csv_line(line: str):
    return line.split(",")


def _check_header(line: str):
    expected = "inst#,actual,predicted,error,prediction,commit_hash,issue_id"

    if line.strip() != expected:
        raise ValueError("Invalid header! Expected: {}".format(expected))


def _label_class(label: str):
    known_labels = ["non_linked", "linked"]

    for l in known_labels:
        if l in label:
            return l

    return None


def _to_prediction(fields: list) -> dict:
    if len(fields) != 7:
        raise ValueError("Invalid number of fields (7 expected): {}".format(fields))

    idx, actual_class, predicted_class, _, prediction, commit_hash, issue_id = fields

    actual_class = _label_class(actual_class)
    predicted_class = _label_class(predicted_class)
    prediction = float(prediction)

    if predicted_class == "linked":
        prob_linked = prediction
    elif predicted_class == "non_linked":
        prob_linked = 1.0 - prediction

    result = {
        "idx": idx,
        "actual_class": actual_class,
        "predicted_class": predicted_class,
        "probability_linked": prob_linked,
        "commit_hash": commit_hash,
        "issue_id": issue_id,
    }

    return result


def read_prediction_file(fn: str):
    with open(fn, "r") as fp:
        header = next(fp)
        _check_header(header)

        for line in filter(lambda x: len(x) > 0, map(lambda x: x.strip(), fp)):
            p = _to_prediction(_split_csv_line(line))
            yield p


def group_predictions_by_commit_hash(predictions: list) -> dict:
    commit_to_predictions = defaultdict(list)

    for p in predictions:
        commit_to_predictions[p["commit_hash"]].append(p)

    return commit_to_predictions


def order_by_probability(commit_to_predictions: dict) -> dict:
    return {
        k: sorted(v, key=operator.itemgetter("probability_linked"), reverse=True)
        for k, v in commit_to_predictions.items()
    }
