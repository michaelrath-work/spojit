import collections
import itertools

from . import util


JiraEvent = collections.namedtuple(
    "JiraEvent",
    [
        "date_time",  # time
        "time_stamp",  # time stamp: [seconds]
        "issue_id",
        "action",  # 'open', 'resolve'
    ],
)

GitEvent = collections.namedtuple(
    "GitEvent",
    ["date_time", "time_stamp", "commit_hash"],  # time  # time stamp: [seconds]
)


_DATE_TIME_FAR_IN_FUTURE = "2030-12-31T23:59:59Z"


def _new_issue_created_event(offset: int, issue: dict) -> JiraEvent:
    created_date = issue["created_date"]
    time_stamp = util.date_time_to_timestamp(created_date) + offset
    return JiraEvent(
        issue_id=issue["id"],
        action="create",
        date_time=created_date,
        time_stamp=time_stamp,
    )


def _new_issue_resolved_event(offset: int, issue: dict) -> JiraEvent:
    resolved_date = issue.get("resolved_date", None)
    if not resolved_date:
        # issue is not yet resolved -> we emulate a dte far in furture
        # to basically keep it open indefinitely
        resolved_date = _DATE_TIME_FAR_IN_FUTURE

    time_stamp = util.date_time_to_timestamp(resolved_date) + offset
    return JiraEvent(
        issue_id=issue["id"],
        action="resolve",
        date_time=resolved_date,
        time_stamp=time_stamp,
    )


def _new_git_event(offset: int, commit: dict) -> GitEvent:
    committed_date = commit["committed_date"]
    time_stamp = util.date_time_to_timestamp(committed_date) + offset
    return GitEvent(
        commit_hash=commit["commit_hash"],
        date_time=committed_date,
        time_stamp=time_stamp,
    )


class EventStreamCreator:
    """Create time ordered stream of development events
    An event is:
        * creation of an issue
        * committing a commit
        * closing/resolving an issue
    """

    TIME_STAMP_OFFSET = -946684800  # year 2000 time stamp

    def __init__(self):
        self.jira_create_offset_seconds = 0
        self.jira_resolve_offset_seconds = 30 * 60  # TODO: re-check this
        self.git_commit_offset_seconds = 0

        self.event_stream = []

    def generate(self, data_source):
        jira_events = self.generate_jira_events(data_source)
        git_events = self.generate_git_events(data_source)

        self.event_stream = list(itertools.chain(jira_events, git_events))
        self.event_stream.sort(key=lambda e: e.time_stamp)

    def generate_jira_events(self, data_source) -> list:
        create_offset = (
            self.jira_create_offset_seconds + EventStreamCreator.TIME_STAMP_OFFSET
        )
        resolve_offset = (
            self.jira_resolve_offset_seconds + EventStreamCreator.TIME_STAMP_OFFSET
        )

        for issue in data_source.issues:
            yield _new_issue_created_event(create_offset, issue)
            yield _new_issue_resolved_event(resolve_offset, issue)

    def generate_git_events(self, data_source) -> list:
        offset = self.git_commit_offset_seconds + EventStreamCreator.TIME_STAMP_OFFSET

        for commit in data_source.change_sets:
            yield _new_git_event(offset, commit)
