import datetime


def date_time_to_timestamp(arg: str) -> int:
    """convert date time in iso8601 int time stamp

    >>> date_time_to_timestamp('2017-06-20T16:00:00Z')
    1497967200

    :param arg: date string
    :return: seconds since epoch
    """

    if not arg:
        raise ValueError("invalid input")

    d = datetime.datetime.strptime(arg, "%Y-%m-%dT%H:%M:%SZ")
    return int(d.timestamp())


def resource_similarity(first, second) -> float:
    """Calculate resource similarity between two lists

    calculates the resource coverage by dividing the number of occurrences of
    the larger lists elements in the smaller one by the total number of elements
    in the larger list

    >>> resource_similarity(['a', 'b', 'c', 'd'], ['a', 'c', 'e'])
    0.5
    >>> resource_similarity(['a', 'c', 'e'], ['a', 'b', 'c', 'd'])
    0.5
    >>> resource_similarity(['a', 'a'], ['b'])
    0.0
    >>> resource_similarity(['a', 'a'], ['b', 'a'])
    0.5

    :param first: 1st list
    :param second: 2nd list
    :return: similarity
    """
    l1 = first if isinstance(first, set) else {a for a in first}
    l2 = second if isinstance(second, set) else {a for a in second}

    l3 = l1.intersection(l2)

    len_l1 = len(l1)
    len_l2 = len(l2)

    if len_l1 > len_l2:
        return len(l3) / len_l1 if len_l1 > 0 else 0

    return len(l3) / len_l2 if len_l2 > 0 else 0
