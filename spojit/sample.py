import abc
import textwrap
import typing

from . import weka

###############################################################################
#
###############################################################################


class TrainingSample:
    __slots__ = [
        "date_time",
        "f00_commit_hash",
        "f01_issue_id",
        "f99_is_linked",
        "f03_commit_user_id",
        "f04_issue_user_id",
        "f06_is_commit_within_issue_life_time",
        "f07_09_previous_linked_commit",
        # f10:
        "f11_num_commits_to_this_issues",
        "f12_num_open_issues_at_commit_time",
        "f13_num_open_issues_from_committer_at_commit_time",
        "f14_time_difference_issue_creation_and_commit",
        "f15_time_difference_commit_and_issue_resolve",
        # f16: close
        "f17_19_next_linked_commit",
        # text similarities
        "f30_max_similarity_code_to_issue_lsi",
        "f31_max_similarity_code_to_issue_vsm_with_ngram",
        "f32_similarity_commit_to_issue_vsm_with_ngram",
        # extra
        "f40_max_trace_similarity",
    ]

    def __init__(self):
        self.date_time = None  # type: str

        self.f00_commit_hash = None  # type: str
        self.f01_issue_id = None  # type: str

        self.f99_is_linked = False  # target value

        self.f03_commit_user_id = None  # type: int
        self.f04_issue_user_id = None  # type: int

        self.f06_is_commit_within_issue_life_time = None  # type: bool

        self.f07_09_previous_linked_commit = {
            # time difference to a previous commit, that was linked to this issue [seconds], or None
            "time_difference": None,
            # resource to a previous commit, that was linked to this issue [percent], or None
            "resource_overlap": None,
            # committer user id
            "committer_user_id": None,
        }

        # number of linked commits to this issue at that point in time
        self.f11_num_commits_to_this_issues = 0  # type: int

        #
        self.f12_num_open_issues_at_commit_time = 0  # type: int

        #
        self.f13_num_open_issues_from_committer_at_commit_time = 0  # type: int

        self.f14_time_difference_issue_creation_and_commit = None

        self.f15_time_difference_commit_and_issue_resolve = None

        # f16: close

        self.f17_19_next_linked_commit = {
            # time difference to a next commit, that was linked to this issue [seconds], or None
            "time_difference": None,
            # resource to a next commit, that was linked to this issue [percent], or None
            "resource_overlap": None,
            # committer user id
            "committer_user_id": None,
        }

        self.f30_max_similarity_code_to_issue_lsi = None
        self.f31_max_similarity_code_to_issue_vsm_with_ngram = None
        self.f32_similarity_commit_to_issue_vsm_with_ngram = None

        self.f40_max_trace_similarity = None


###############################################################################
#
###############################################################################


class BaseSampleCollector(abc.ABC):
    @abc.abstractmethod
    def add(self, sample: TrainingSample):
        pass


class SampleCollector(BaseSampleCollector):
    def __init__(self):
        self.samples = []  # type: typing.List[TrainingSample]

    def add(self, sample: TrainingSample):
        self.samples.append(sample)


###############################################################################
#
###############################################################################

# shortcuts
NOMINAL_TRUE_FALSE_VALUES = ["t", "f"]
NOMINAL_CLASS = ["linked", "non_linked"]

_weka_att_num = weka.create_numeric_attribute
_weka_att_nom = weka.create_nominal_attribute
_weka_att_str = weka.create_string_attribute


# definition of temporal 'closeness' to a previous commit
CLOSENESS_PREVIOUS_LINKED_COMMIT_SEC = 2 * 24 * 3600

# definition of temporal 'closeness' to a previous commit
CLOSENESS_NEXT_LINKED_COMMIT_SEC = 2 * 24 * 3600

# definition of temporal 'closeness' between issue resolution and commit
CLOSENESS_TO_ISSUE_RESOLUTION_SEC = 2.5 * 24 * 3600

SPOJIT_ATTRIBUTES = [
    _weka_att_num("sample_id", "XX just for orientation"),
    _weka_att_str("commit_hash", "00: just for orientation"),
    _weka_att_str("issue_id", "01: just for orientation"),
    _weka_att_num("commit_user_id", "03: committer of the change set"),
    _weka_att_num("issue_user_id", "04: assignee of the issues"),
    _weka_att_nom(
        "assignee_is_committer",
        NOMINAL_TRUE_FALSE_VALUES,
        "05: are committer and assignee the same person",
    ),
    _weka_att_nom(
        "is_commit_within_issue_lifetime",
        NOMINAL_TRUE_FALSE_VALUES,
        "06: issue and commit existed at same time, i.e",
    ),
    _weka_att_num(
        "previous_commit_time_difference",
        "07: time difference in seconds to a previous commit\n"
        "    that was linked to the same issue [seconds]",
    ),
    _weka_att_nom(
        "previous_commit_is_close",
        NOMINAL_TRUE_FALSE_VALUES,
        "08: Was the previous to the same issue 'close'",
    ),
    _weka_att_num(
        "previous_commit_resource_overlap",
        "09: resource overlap to previous commit\n"
        "    that was linked to the same issue",
    ),
    _weka_att_nom(
        "previous_commit_is_same_user",
        NOMINAL_TRUE_FALSE_VALUES,
        "10: the committer of the previous commit linked\n"
        "    to this issue is the current committer",
    ),
    _weka_att_num(
        "number_existing_links_to_issue_at_commit_time",
        "11: number of existing links to the issue at commit time\n"
        "    (excluding this commit, if it is linked)",
    ),
    _weka_att_num(
        "number_of_open_issues_at_commit_time",
        "12: number of open issues at commit time",
    ),
    _weka_att_num(
        "number_of_open_issues_at_commit_time_for_committer",
        "13: number of open issues at commit time assigned to committer",
    ),
    _weka_att_num(
        "time_diff_issue_creation_and_commit",
        textwrap.dedent(
            """\
                  14: seconds between issue creation and commit time [seconds]

                         Issue life time: A ... B
                         Commit time    : X

                         -A------X------------B-----> time
                          |<---->|
                            X - A"""
        ),
    ),
    _weka_att_num(
        "time_diff_commit_and_issue_resolve",
        textwrap.dedent(
            """\
                  15: seconds between the commit time and resolve of the issue

                        Issue life time: A ... B
                        Commit time    : X

                        -A------X------------B-----> time
                                |<---------->|
                                     B - X"""
        ),
    ),
    _weka_att_nom(
        "is_committed_close_to_issue_resolve",
        NOMINAL_TRUE_FALSE_VALUES,
        textwrap.dedent(
            """\
                  16: commit occurred close to issue resolution

                        Issue life time: A ... B
                        Commit time    : X

                        -A-----------X------B------X---------> time
                                     |<---->|<---->|

                            abs(B-X) < tolerance"""
        ),
    ),
    _weka_att_num(
        "next_commit_time_difference",
        "17: time difference in seconds to a next commit\n"
        "    that was linked to the same issue [seconds]",
    ),
    _weka_att_nom(
        "next_commit_is_close",
        NOMINAL_TRUE_FALSE_VALUES,
        "18: Is the next commit to the same issue 'close'",
    ),
    _weka_att_num(
        "next_commit_resource_overlap",
        "19: resource overlap to next commit\n" "    that was linked to the same issue",
    ),
    _weka_att_nom(
        "next_commit_is_same_user",
        NOMINAL_TRUE_FALSE_VALUES,
        "20: the committer of the previous commit linked\n"
        "    to this issue is the current committer",
    ),
    _weka_att_num(
        "max_sim_code_to_issue_lsi",
        "30: maximum similarity of the source code files of  commit\n"
        "    to the issue using LSI",
    ),
    _weka_att_num(
        "max_sim_code_to_issue_vsm_ngram",
        "31: maximum similarity of the source code files of  commit\n"
        "    to the issue using VSM with NGram",
    ),
    _weka_att_num(
        "sim_commit_to_issue_vsm_ngram",
        "32: similarity of commit message to issue using VSM with NGram",
    ),
    _weka_att_num("max_trace_similarity", "40: Trace similarity metric"),
    _weka_att_nom(
        "class", NOMINAL_CLASS, "99: Ground truth. is this commit linked to the issue"
    ),
]


def _same_users(user_id_1, user_id_2):
    if user_id_1 is None or user_id_2 is None:
        return None

    return user_id_1 == user_id_2


def _to_nominal_bool(value):
    if value is None:
        return None

    return "t" if value else "f"


def sample_to_weka_instance(sample: TrainingSample) -> list:
    # NOTE: sample id will be remapped, later
    instance_values = [0]

    # 00 - 01
    instance_values.append(sample.f00_commit_hash)
    instance_values.append(sample.f01_issue_id)
    instance_values.append(sample.f03_commit_user_id)
    instance_values.append(sample.f04_issue_user_id)
    instance_values.append(
        _to_nominal_bool(
            _same_users(sample.f03_commit_user_id, sample.f04_issue_user_id)
        )
    )

    # 06
    instance_values.append(
        _to_nominal_bool(sample.f06_is_commit_within_issue_life_time)
    )

    # # # # Previous commit
    # 07 + 08
    time_diff = sample.f07_09_previous_linked_commit["time_difference"]
    if time_diff is None:
        instance_values += [None, None]
    else:
        instance_values.append(time_diff)
        instance_values.append(
            _to_nominal_bool(time_diff < CLOSENESS_PREVIOUS_LINKED_COMMIT_SEC)
        )

    # 09
    instance_values.append(sample.f07_09_previous_linked_commit["resource_overlap"])

    # 10
    instance_values.append(
        _to_nominal_bool(
            _same_users(
                sample.f03_commit_user_id,
                sample.f07_09_previous_linked_commit["committer_user_id"],
            )
        )
    )

    # 11 - 13
    instance_values.append(sample.f11_num_commits_to_this_issues)
    instance_values.append(sample.f12_num_open_issues_at_commit_time)
    instance_values.append(sample.f13_num_open_issues_from_committer_at_commit_time)

    # 14
    instance_values.append(sample.f14_time_difference_issue_creation_and_commit)

    # 15
    instance_values.append(sample.f15_time_difference_commit_and_issue_resolve)

    # 16
    if sample.f15_time_difference_commit_and_issue_resolve is None:
        instance_values.append("f")
    else:
        is_close = (
            abs(sample.f15_time_difference_commit_and_issue_resolve)
            < CLOSENESS_TO_ISSUE_RESOLUTION_SEC
        )
        instance_values.append("t" if is_close else "f")

    # # # # Next commit
    # 17 + 18
    time_diff = sample.f17_19_next_linked_commit["time_difference"]
    if time_diff is None:
        instance_values += [None, None]
    else:
        instance_values.append(time_diff)
        instance_values.append(
            _to_nominal_bool(time_diff < CLOSENESS_NEXT_LINKED_COMMIT_SEC)
        )

    # 19
    instance_values.append(sample.f17_19_next_linked_commit["resource_overlap"])

    # 20
    instance_values.append(
        _to_nominal_bool(
            _same_users(
                sample.f03_commit_user_id,
                sample.f17_19_next_linked_commit["committer_user_id"],
            )
        )
    )

    # # # # TEXT SIMILARITIES
    instance_values += [
        sample.f30_max_similarity_code_to_issue_lsi,
        sample.f31_max_similarity_code_to_issue_vsm_with_ngram,
        sample.f32_similarity_commit_to_issue_vsm_with_ngram,
    ]

    # # # # Extra metrics
    instance_values.append(sample.f40_max_trace_similarity)

    # target value
    instance_values.append("linked" if sample.f99_is_linked else "non_linked")

    assert len(instance_values) == len(SPOJIT_ATTRIBUTES)
    return instance_values


def create_weka_dataset(
    relation_name, samples: typing.List[TrainingSample]
) -> weka.Instances:
    dataset = weka.create_dataset(relation_name, SPOJIT_ATTRIBUTES)

    for s in samples:
        inst = sample_to_weka_instance(s)
        # add sample numbering
        inst[0] = len(dataset.values) + 1

        dataset = weka.add_instance(dataset, inst)

    return dataset
