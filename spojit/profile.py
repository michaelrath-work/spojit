import collections
import copy
import itertools
import typing

from . import event as event_pkg
from . import io
from .sample import BaseSampleCollector, TrainingSample
from . import util
from .artifact_filter import DefaultFilter


###############################################################################
#
###############################################################################


class IssueLifeTimeManager:
    """Keep track of issue creation and resolving
    """

    def __init__(self):
        self.resolved_issues = []
        self.current_open_issues = []

    def create_issue(self, issue_id):
        if issue_id not in self.current_open_issues:
            self.current_open_issues.append(issue_id)

    def resolve_issue(self, issue_id):
        self.current_open_issues = [
            e for e in self.current_open_issues if e != issue_id
        ]

        if issue_id not in self.resolved_issues:
            self.resolved_issues.append(issue_id)


###############################################################################
#
###############################################################################


DataSource = collections.namedtuple(
    "DataSource", ["issues", "issue_to_change_set", "change_sets"]
)


###############################################################################
#
###############################################################################


def _filter_change_sets(change_sets: list, artifact_filter) -> list:
    """Filter the change sets with given artifact filter
    """
    result = []
    for commit in change_sets:
        mod_commit = copy.deepcopy(commit)
        # grab only source files
        mod_commit["file_path"] = list(
            filter(artifact_filter.is_source_file, commit["file_path"])
        )

        if artifact_filter.is_relevant_commit(mod_commit):
            result.append(mod_commit)

    return result


def _filter_data_source(data_source: DataSource, artifact_filter) -> DataSource:
    """Filter a data source and create a new one
    """

    def add_mapped_type(issue: dict) -> dict:
        issue2 = copy.deepcopy(issue)
        issue2["mapped_type"] = artifact_filter.issue_type_to_category(issue)
        return issue2

    # filter the issues
    issues = list(
        map(
            add_mapped_type,
            filter(artifact_filter.is_relevant_issue, data_source.issues),
        )
    )
    used_issue_ids = {issue["id"] for issue in issues}

    # filter change sets
    change_sets = _filter_change_sets(data_source.change_sets, artifact_filter)
    used_change_set_ids = {c["commit_hash"] for c in change_sets}

    # filter the change sets
    issue_to_change_set = {}
    for issue_id, commits in data_source.issue_to_change_set.items():
        if issue_id not in used_issue_ids:
            continue

        valid_ids = used_change_set_ids.intersection(commits)
        if valid_ids:
            issue_to_change_set[issue_id] = list(valid_ids)

    return DataSource(
        issues=issues, issue_to_change_set=issue_to_change_set, change_sets=change_sets
    )


def _all_linked_changed_sets(ds: DataSource) -> set:
    return {commit for commits in ds.issue_to_change_set.values() for commit in commits}


_TO_PREDICT_KEY = "__to_predict__"


def _mark_as_to_predict(d: dict) -> dict:
    """Add a special key to mark new commits
    """
    r = copy.deepcopy(d)
    r[_TO_PREDICT_KEY] = True
    return r


def _is_marked_as_to_predict(d: dict) -> bool:
    return d.get(_TO_PREDICT_KEY, False)


def _prepare_data_source(
    data_source: DataSource, new_change_sets: typing.List[typing.Dict]
):
    history_commit_hashes = {c["commit_hash"] for c in data_source.change_sets}
    new_commit_hashes = {c["commit_hash"] for c in new_change_sets}
    overlap = history_commit_hashes.intersection(new_commit_hashes)
    if len(overlap):
        raise ValueError("Commit hashes appear in history and new")

    # mark the new change sets and add them to a temporary data source
    # NOTE: no deep copy is needed, this is done in the filter
    marked_change_sets = list(map(_mark_as_to_predict, new_change_sets))
    tmp_data_source = DataSource(
        issues=data_source.issues,
        issue_to_change_set=data_source.issue_to_change_set,
        change_sets=data_source.change_sets + marked_change_sets,
    )

    # apply the filter
    return _filter_data_source(tmp_data_source, DefaultFilter())


DataSourceType = collections.namedtuple(
    "DataSource", ["issues", "issue_to_change_set", "change_sets"]
)


class DataSource(DataSourceType):
    __slots__ = ()

    def __str__(self):
        n_i = len(self.issues)
        first_i = self.issues[0] if n_i > 0 else None

        n_cs = len(self.change_sets)
        first_cs = self.change_sets[0] if n_cs > 0 else None

        n_i2cs = len(self.issue_to_change_set)
        first_i2cs = (
            next(iter(self.issue_to_change_set.items())) if n_i2cs > 0 else None
        )

        return """DataSource with
  {:5d} issues, 1st: {}
  {:5d} change sets, 1st: {}
  {:5d} links, 1st: {}
""".format(
            n_i, first_i, n_cs, first_cs, n_i2cs, first_i2cs
        )


class ProfileGenerator:
    """Create data for training, testing, and prediction for a classifier fro a given issue setup (i.e. profile)

    Input arguments:
        'data source': reflects the known/mined artifacts from ITS and VCS

        'change_sets_to_predict': a list of change sets from VCS, with 'unknown linking'.
            There are two scenarios:
            (a) Classic train/test split: the list of change_sets to predict is e.g. 20% of the newest
                commits from _mined_ VCS artifacts. So this is used to evaluate the classifier performance.
                For these commit, some known links may exist between issues from the mined 'data source' and
                these commits. These links are than used to evaluate the classifier prediction performance

            (b) The list of change sets, or usually just one, is a new one commit about to be added to VCS.
                So no links from issues 'data source' to these commits. This is _actual_ prediction for
                production usage

            Note: the list can be empty
    """

    # study showed, that some commits are linked to already resolved issues. But the time window is limited
    # We allow this, up to this difference
    MAX_DIFFERENCE_BETWEEN_RESOLVED_AND_COMMITTED_SEC = 1.5 * 24 * 3600

    NO_SIMILARITY = 0.0

    def __init__(
        self,
        data_source: DataSource,
        change_sets_to_predict: typing.List[typing.Dict],
        text_similarity_handler: io.TextSimilarityHandler,
        trace_handler: io.TraceSimilarityHandler,
        issue_types: typing.List[str],
    ):
        self.data_source = _prepare_data_source(data_source, change_sets_to_predict)

        self.esc = event_pkg.EventStreamCreator()
        self.allowed_issue_types = issue_types[:]

        assert text_similarity_handler is not None
        self.text_similarity_handler = text_similarity_handler
        assert trace_handler is not None
        self.trace_handler = trace_handler

        self.issue_manager = IssueLifeTimeManager()

        self.project_users = io.build_project_users(self.data_source)
        self.unknown_jira_user_id = len(self.project_users)
        self.unknown_git_user_id = self.unknown_jira_user_id + 1

        self.issue_id_to_issue = {}
        self.commit_hash_to_change_set = {}
        self.commit_hash_to_issue_ids = {}

        # hashes of all linked change sets without any constraints
        # (i.e. no filtering on issue type, resolution, etc)
        self.linked_change_sets_unconstrained = _all_linked_changed_sets(data_source)

    # noinspection PyUnusedLocal
    def run(
        self,
        data_source_collector: BaseSampleCollector,
        to_predict_collector: BaseSampleCollector,
    ):
        """Sample the profiles

        :param data_source_collector: collector for samples created from 'data source'
        :param to_predict_collector: collector for samples created for 'change_sets_to_predict'
        """

        self.esc.generate(self.data_source)

        # transform data
        self.issue_id_to_issue = self.build_issue_lookup()
        self.commit_hash_to_change_set = self.build_change_set_lookup()
        self.commit_hash_to_issue_ids = self.build_change_set_issue_inverse_lookup()

        num_events = len(self.esc.event_stream)
        for event_nr, event in enumerate(self.esc.event_stream):
            # if event_nr % 500 == 0:
            #     self.logger.info('%8d samples (%6d/%6d events)' % (self.num_samples_created, event_nr, num_events))

            if not self.is_relevant_event(event):
                continue

            ##################
            # JIRA EVENT
            if isinstance(event, event_pkg.JiraEvent):
                if event.action == "create":
                    self.issue_manager.create_issue(event.issue_id)

                elif event.action == "resolve":
                    self.issue_manager.resolve_issue(event.issue_id)

                else:
                    raise RuntimeError("Unknown event action %s" % event.action)

            ##################
            # GIT EVENT
            if isinstance(event, event_pkg.GitEvent):
                samples = self.create_samples_for_change_set(event.commit_hash)
                commit = self.commit_hash_to_change_set[event.commit_hash]

                def add_samples(collector: BaseSampleCollector):
                    for s in samples:
                        collector.add(s)

                if _is_marked_as_to_predict(commit):
                    add_samples(to_predict_collector)
                else:
                    add_samples(data_source_collector)

    def create_samples_for_change_set(self, commit_hash: str) -> list:
        """Create all samples for a given commit

        :param commit_hash:
        :return:
        """
        commit = self.commit_hash_to_change_set.get(commit_hash, None)
        if not commit:
            raise ValueError('Could not find commit "{}"'.format(commit_hash))

        samples = []
        is_training = not _is_marked_as_to_predict(commit)

        if is_training:
            # check if issue is linked: if so, only build these links
            linked_issue_ids = self.commit_hash_to_issue_ids.get(commit_hash, None)
            if linked_issue_ids:
                for issue_id in linked_issue_ids:
                    issue = self.issue_id_to_issue.get(issue_id, None)

                    if (
                        issue is not None
                        and issue["mapped_type"] in self.allowed_issue_types
                    ):
                        sample = self.create_training_sample(commit, issue)
                        sample.f06_is_commit_within_issue_life_time = False

                        if issue_id in self.issue_manager.current_open_issues:
                            sample.f06_is_commit_within_issue_life_time = True

                        samples.append(sample)

                # commit is linked, no further processing required
                return samples

        # samples for all active issues
        for issue_id in self.issue_manager.current_open_issues:
            issue = self.issue_id_to_issue.get(issue_id, None)
            if not issue:
                continue

            sample = self.create_training_sample(commit, issue)
            sample.f06_is_commit_within_issue_life_time = True
            samples.append(sample)

        # samples for previous issues:
        commit_time_stamp = util.date_time_to_timestamp(commit["committed_date"])

        for issue_id in self.issue_manager.resolved_issues:
            issue = self.issue_id_to_issue.get(issue_id, None)
            if not issue:
                continue

            # only consider issues resolved a given time before commit
            resolved_time_stamp = util.date_time_to_timestamp(issue["resolved_date"])
            diff_sec = abs(commit_time_stamp - resolved_time_stamp)
            if (
                diff_sec
                < ProfileGenerator.MAX_DIFFERENCE_BETWEEN_RESOLVED_AND_COMMITTED_SEC
            ):
                sample = self.create_training_sample(commit, issue)
                sample.f06_is_commit_within_issue_life_time = False
                samples.append(sample)

        return samples

    def build_issue_lookup(self) -> dict:
        """Build mapping from issue id to issue

        Additionally enhance data by user id
        Note:
            Only allowed issues are added

        :return: mapping issue id to issue
        """
        result = {}

        for raw_issue in self.data_source.issues:
            if raw_issue["mapped_type"] not in self.allowed_issue_types:
                continue

            issue = copy.deepcopy(raw_issue)

            user_id = self.project_users.get_link_id(issue["assignee_username"], "jira")
            if user_id is None:
                user_id = self.unknown_jira_user_id
            issue["user_id"] = user_id

            result[issue["id"]] = issue

        return result

    def build_change_set_lookup(self) -> dict:
        """Build mapping from commit hash to commit

        Additionally enhance data by user id

        :return: mapping
        """
        result = {}
        for raw_commit in self.data_source.change_sets:
            commit = copy.deepcopy(raw_commit)
            # commit['committed_date_ts'] = util.date_time_to_timestamp(commit['committed_date'])

            user_id = self.project_users.get_link_id(commit["author_email"], "git")
            if user_id is None:
                user_id = self.unknown_git_user_id

            commit["user_id"] = user_id
            result[commit["commit_hash"]] = commit

        return result

    def build_change_set_issue_inverse_lookup(self) -> dict:
        """Build lookup from commit hash to linked issues ids

        :return:
        """
        hash_to_issue_ids = {}

        for issue_id, data in self.data_source.issue_to_change_set.items():
            for commit_hash in data:
                if commit_hash in hash_to_issue_ids:
                    issues = hash_to_issue_ids[commit_hash]

                    if issue_id not in issues:
                        # append
                        hash_to_issue_ids[commit_hash] = issues + [issue_id]
                else:
                    # first add
                    hash_to_issue_ids[commit_hash] = [issue_id]

        return hash_to_issue_ids

    def is_relevant_event(self, event) -> bool:
        """Event exclusion

        :param event: Jira or Git event
        :return:
        """

        if isinstance(event, event_pkg.GitEvent):
            linked_issues = self.commit_hash_to_issue_ids.get(event.commit_hash, None)
            if linked_issues is None:
                # a non-linked commit -> that's what we want ...

                # FIXME ... wait a minute. We made an 'error' during filtering of our initial data
                #   we only use links to issues with a certain state, e.g. 'status = Done' and
                #   'resolution = False'. Using the export data, we assume commits that are not
                #   linked in the exported data are not linked at all. *BUT THIS IS A WRONG ASSUMPTION*
                #
                #   There are many linked commits to issues (even if they have 'resolution = Won't Fix'.
                #   So, we have to exclude these commits. Otherwise we would train on already linked commits
                #
                #   Example:
                #   (1) Commit abcde1234 is linked to Issue SECSY-666, which is 'unresolved'
                #   (2) Commit abcde1234 will appear as unlinked in the exported data set
                #   (3) We would predict on abcde1234 and propose Issues for it -> Error
                if event.commit_hash in self.linked_change_sets_unconstrained:
                    # Oops, the commit is linked after-all
                    return False

                # true non linked commit
                return True

            for issue_id in linked_issues:
                issue = self.issue_id_to_issue.get(issue_id, None)
                if issue and (issue["mapped_type"] in self.allowed_issue_types):
                    # commit is linked to an interesting issue type
                    return True

            # commit is linked to issue type, we are not interested in
            return False

        if isinstance(event, event_pkg.JiraEvent):
            issue = self.issue_id_to_issue.get(event.issue_id, None)
            if issue is None:
                return False

            return issue["mapped_type"] in self.allowed_issue_types

        return False

    def create_training_sample(self, commit: dict, issue: dict) -> TrainingSample:
        """Create sample from a given commit and issue combination

        :param commit:
        :param issue:
        :return:
        """
        commit_hash = commit["commit_hash"]  # type: str
        issue_id = issue["id"]  # type: str

        sample = TrainingSample()

        linked_issue_ids = self.commit_hash_to_issue_ids.get(commit_hash, None)
        if linked_issue_ids and issue_id in linked_issue_ids:
            sample.f99_is_linked = True

        sample.f00_commit_hash = commit_hash
        sample.f01_issue_id = issue_id
        sample.date_time = commit["committed_date"]

        sample.f03_commit_user_id = commit["user_id"]
        sample.f04_issue_user_id = issue["user_id"]

        sample.f11_num_commits_to_this_issues = self.get_number_of_links_to_issue_before(
            issue_id, commit["committed_date"]
        )

        sample.f12_num_open_issues_at_commit_time = len(
            self.issue_manager.current_open_issues
        )

        sample.f13_num_open_issues_from_committer_at_commit_time = self.get_number_of_currently_open_issues_assigned_to_user(
            sample.f03_commit_user_id
        )

        # sanity:
        assert (
            sample.f13_num_open_issues_from_committer_at_commit_time
            <= sample.f12_num_open_issues_at_commit_time
        )

        t1 = util.date_time_to_timestamp(issue["created_date"])
        t2 = util.date_time_to_timestamp(commit["committed_date"])

        sample.f14_time_difference_issue_creation_and_commit = t2 - t1

        resolved_date = issue.get("resolved_date", None)
        if resolved_date:
            t3 = util.date_time_to_timestamp(resolved_date)
            sample.f15_time_difference_commit_and_issue_resolve = t3 - t2
        else:
            sample.f15_time_difference_commit_and_issue_resolve = None

        self.assign_text_similarities(commit, issue, sample)

        self.assign_trace_metric(commit, issue, sample)

        # backward / forward in history
        self.check_previous_and_next_commits(commit, issue, sample)

        return sample

    def check_previous_and_next_commits(
        self, commit: dict, issue: dict, sample: TrainingSample
    ):
        """Check if linked commits to the issues existed

                         issue
        ---[-----X------------X-------------X--------]------> time
                 ^                          ^
                 |                          |
                C_prev        C             C_next

        Search for (if existent) C_prev and C_next, which are linked to issue.
        C is the current commit.

        :param commit: commit
        :param issue: issue
        :param sample:
        :return:
        """
        linked_commit_hashes = self.data_source.issue_to_change_set.get(issue["id"], [])
        if not linked_commit_hashes:
            return

        linked_commits = []
        for linked_commit_hashes in linked_commit_hashes:
            linked_commit = self.commit_hash_to_change_set.get(
                linked_commit_hashes, None
            )
            if linked_commit:
                linked_commits.append(linked_commit)

        if len(linked_commits) == 0:
            return

        sorted_linked_commits = sorted(
            linked_commits, key=lambda c: c["committed_date"]
        )
        committed_date = commit["committed_date"]

        # Backward: get all commits linked to the issue _before_ this commit
        previous_commits = list(
            itertools.takewhile(
                lambda c: c["committed_date"] < committed_date, sorted_linked_commits
            )
        )

        # Forward: get all commits linked to the issue _after_ this commit
        next_commits = list(
            itertools.dropwhile(
                lambda c: c["committed_date"] <= committed_date, sorted_linked_commits
            )
        )

        if len(previous_commits) > 0:
            previous_commit = previous_commits[-1]  # the last of the previous

            # timings
            t1 = util.date_time_to_timestamp(commit["committed_date"])
            t2 = util.date_time_to_timestamp(previous_commit["committed_date"])
            sample.f07_09_previous_linked_commit["time_difference"] = t1 - t2

            # resource overlap
            overlap = util.resource_similarity(
                commit["file_path"], previous_commit["file_path"]
            )
            sample.f07_09_previous_linked_commit["resource_overlap"] = overlap

            # person
            sample.f07_09_previous_linked_commit["committer_user_id"] = previous_commit[
                "user_id"
            ]

        if len(next_commits) > 0:
            next_commit = next_commits[0]  # the first of the next

            t1 = util.date_time_to_timestamp(commit["committed_date"])
            t2 = util.date_time_to_timestamp(next_commit["committed_date"])
            sample.f17_19_next_linked_commit["time_difference"] = t2 - t1

            # resource overlap
            overlap = util.resource_similarity(
                commit["file_path"], next_commit["file_path"]
            )
            sample.f17_19_next_linked_commit["resource_overlap"] = overlap

            # person
            sample.f17_19_next_linked_commit["committer_user_id"] = next_commit[
                "user_id"
            ]

    def get_number_of_links_to_issue_before(
        self, issue_id: str, date_time_before: str
    ) -> int:
        """Get the number of commits linked to a given issues before a given point in time

        :param issue_id:
        :param date_time_before:
        :return:
        """
        assert isinstance(date_time_before, str)

        count = 0

        linked_commit_hashes = self.data_source.issue_to_change_set.get(issue_id, [])
        for commit_hash in linked_commit_hashes:
            commit = self.commit_hash_to_change_set.get(commit_hash)
            if commit and commit["committed_date"] < date_time_before:
                count += 1

        return count

    def get_number_of_currently_open_issues_assigned_to_user(self, user_id: int) -> int:
        """Count the number of currently open issues for a given user

        :param user_id:
        :return:
        """
        assert user_id is not None

        count = 0
        for issue_id in self.issue_manager.current_open_issues:
            issue = self.issue_id_to_issue.get(issue_id, None)
            if issue and user_id == issue["user_id"]:
                count += 1

        return count

    def assign_text_similarities(
        self, commit: dict, issue: dict, sample: TrainingSample
    ):
        committed_file_paths = commit["file_path"]
        issue_id = issue["id"]

        # TEXT TO ISSUE
        sample.f30_max_similarity_code_to_issue_lsi = _get_maximum_text_similarity(
            self.text_similarity_handler.code_to_issue_lsi,
            issue_id,
            commit["commit_hash"],
            committed_file_paths,
            default=ProfileGenerator.NO_SIMILARITY,
        )

        sample.f31_max_similarity_code_to_issue_vsm_with_ngram = _get_maximum_text_similarity(
            self.text_similarity_handler.code_to_issue_vsm_ngram,
            issue_id,
            commit["commit_hash"],
            committed_file_paths,
            default=ProfileGenerator.NO_SIMILARITY,
        )

        # COMMIT TO ISSUE
        value = self.text_similarity_handler.commit_to_issue_vsm_ngram.get_sim(
            issue_id, commit["commit_hash"]
        )

        sample.f32_similarity_commit_to_issue_vsm_with_ngram = (
            value if value is not None else ProfileGenerator.NO_SIMILARITY
        )

    def assign_trace_metric(self, commit: dict, issue: dict, sample: TrainingSample):
        handler = self.trace_handler.trace_metric
        assert handler is not None

        sample.f40_max_trace_similarity = _get_maximum_text_similarity(
            handler,
            issue["id"],
            commit["commit_hash"],
            commit["file_path"],
            default=ProfileGenerator.NO_SIMILARITY,
        )


def _get_maximum_text_similarity(
    handler, issue_id: str, commit_hash: str, file_paths: list, default: float
) -> float:
    """Get the maximum file similarity

    If no handler is available, None is returned

    :param handler: text similarity handler
    :param issue_id: issue id
    :param commit_hash: commit hash
    :param file_paths: list of file paths in the commit
    :param default: default value
    :return: max value
    """
    assert handler is not None

    def get_sim(file_path: str) -> float:
        return handler.get_sim(issue_id, commit_hash, file_path)

    sim_values = map(get_sim, file_paths)
    return max((val for val in sim_values if val is not None), default=default)
