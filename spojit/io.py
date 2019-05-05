import abc
import collections
import json
import os
import operator
import typing
import warnings
import zipfile

from . import user_model

###############################################################################
#
###############################################################################


class JsonImport:
    """Simple importer of exported Json files

    ## Data Format

    * change_sets
        * List of objects (dicts)

        [
            {
                'author': str,
                'author_email': str,
                'commit_hash': str,
                'committed_date': str,
                'file_path': [ str ]
            },
            ...
        ]

    * issue_to_change_set
        * mapping string <issue_id> to list of commit hashes (str)

        {
            <issue_id_1>: [ str ],
            <issue_id_2>: [ str ],
        }

    * issue_to_issue
        * list of links

        [
            {
                'source_issue_id': str,
                'target_issue_id': str,
                'semantics': str
            },
            ...
        ]

     * issues
        * List of objects (dicts)

        [
            {
                'assignee': str,
                'assignee_username': str,
                'created_date': str,
                'id': str,
                'mapped_type': str,
                'resolved_data': str,
                'type': str
            },
            ...
        ]

    """

    def __init__(self, project_name, import_texts=False):
        self.project_name = project_name
        self.import_texts = import_texts
        self.zip_file = None  # type: zipfile.ZipFile
        self.issues = []
        self.issue_to_issue = []
        self.issue_to_change_set = collections.defaultdict(list)
        self.change_sets = []
        pass

    def run(self, source_directory: str):
        fn = os.path.join(source_directory, self.project_name + ".zip")

        with zipfile.ZipFile(fn, "r") as self.zip_file:
            self.import_issues()
            # self.import_issue_to_issue()
            self.import_change_set_links()
            self.import_change_sets()

    def import_issues(self):
        import_fields = [
            "id",
            "created_date",
            "resolved_date",
            "type",
            "status",
            "resolution",
            "assignee",
            "assignee_username",
        ]

        if self.import_texts:
            import_fields += ["summary", "description"]

        for file_name in self.zip_file.namelist():
            if file_name.startswith("_"):
                continue

            with self.zip_file.open(file_name) as fp:
                content = str(fp.read(), "utf-8")
                obj = json.loads(content)
                issue = {k: obj[k] for k in import_fields}
                self.issues.append(issue)

    def import_issue_to_issue(self):
        warnings.warn("This function is possibly not needed", DeprecationWarning)

        with self.zip_file.open("_issue_links.json") as fp:
            content = str(fp.read(), "utf-8")
            self.issue_to_issue = json.loads(content)

    def import_change_set_links(self):
        with self.zip_file.open("_issue_to_change_set.json") as fp:
            content = str(fp.read(), "utf-8")
            obj = json.loads(content)
            for issue_id, content in obj.items():
                commit_hashes = [e["commit_hash"] for e in content]
                self.issue_to_change_set[issue_id] = commit_hashes

        pass

    def import_change_sets(self):
        import_keys = ["file_path", "author", "author_email", "committed_date"]

        if self.import_texts:
            import_keys += ["message"]

        with self.zip_file.open("_change_set_to_code.json") as fp:
            content = str(fp.read(), "utf-8")
            obj = json.loads(content)
            for commit_hash, content in obj.items():
                commit = {k: content[k] for k in import_keys}
                commit.update({"commit_hash": commit_hash})
                self.change_sets.append(commit)


###############################################################################
#
###############################################################################


class IssueToOtherSimilarityHandler(abc.ABC):
    @abc.abstractmethod
    def get_sim(self, issue_id: str, other_id: str) -> typing.Optional[float]:
        """Query similarity from issue id to another object

        Another object might be:
            * commit_hash
            * ...

        :param issue_id: issue identifier, e.g. SPOJIT-123
        :param other_id: other object, e.g. 'abc5de6f...' [commit_hash]
        :return: similarity
        """
        pass


class IssueToCodeSimilarityHandler(abc.ABC):
    @abc.abstractmethod
    def get_sim(self, issue_id: str, commit_hash: str, file_path: str) -> typing.Optional[float]:
        """Query similarity from issue id to file path of a specific change set

        :param issue_id: issue identifier, e.g. SPOJIT-123
        :param commit_hash: change set, e.g. 'abc5de6f...' [commit_hash]
        :param file_path: file path, e.g. 'src/foo.java'[filepath]
        :return: similarity
        """
        pass


class _NoneI2OHandler(IssueToOtherSimilarityHandler):
    """Empty similarity handler always returning None
    """

    def __init__(self):
        pass

    def get_sim(self, issue_id: str, other_id: str) -> typing.Optional[float]:
        return None


class _NoneI2CHandler(IssueToCodeSimilarityHandler):
    """Empty similarity handler always returning None
    """
    def get_sim(self, issue_id: str, commit_hash: str, file_path: str) -> typing.Optional[float]:
        return None


def _assure_i2o_handler_exists(h) -> IssueToOtherSimilarityHandler:
    if not h:
        h = _NoneI2OHandler()

    assert isinstance(h, IssueToOtherSimilarityHandler)
    return h


def _assure_i2c_handler_exists(h) -> IssueToCodeSimilarityHandler:
    if not h:
        h = _NoneI2CHandler()

    assert isinstance(h, IssueToCodeSimilarityHandler)
    return h


class TextSimilarityHandler:
    """Handle different text similarities
    """

    def __init__(
        self,
        issue_to_code_lsi=None,
        issue_to_code_vsm_ngram=None,
        issue_to_commit_vsm_ngram=None,
    ):
        # File: LSI_<project>_<profile>_code_%d.p
        self.code_to_issue_lsi = _assure_i2c_handler_exists(issue_to_code_lsi)

        # File: VSMWithNGram_<project>_<profile>_code_%d.p
        self.code_to_issue_vsm_ngram = _assure_i2c_handler_exists(issue_to_code_vsm_ngram)

        # File: VSMWithNGram_<project>_<profile>_commit_%d.p
        self.commit_to_issue_vsm_ngram = _assure_i2o_handler_exists(
            issue_to_commit_vsm_ngram
        )

    def __str__(self):
        return """TextSimilarityHandler
  ISSUE->CODE      : {}
  ISSUE->CHANGE SET: {}
  LSI              : {}""".format(
            self.code_to_issue_vsm_ngram,
            self.commit_to_issue_vsm_ngram,
            self.code_to_issue_lsi,
        )


###############################################################################
#
###############################################################################


class TraceSimilarityHandler:
    """Special Jacob metric

    IssueId -> file_path
    """

    def __init__(self, issue_to_file=None):
        # File: ts_<project>_<profile>_code_%d.p
        self.trace_metric = _assure_i2c_handler_exists(issue_to_file)

    def __str__(self):
        return """TraceSimilarityHandler
  TRACE-METRIC: {}""".format(
            self.trace_metric
        )


###############################################################################
#
###############################################################################


def _flatten(d: dict) -> list:
    """Transform dict of sets in flat list

    :param d: dict of sets
    :return: list of tuples
    """
    return [(key, v) for key, values in d.items() for v in values]


def build_project_users(project_data) -> user_model.ProjectUsers:
    jira_raw = collections.defaultdict(set)
    for issue in project_data.issues:
        assignee = issue["assignee"]
        if assignee:
            jira_raw[issue["assignee"]].update([issue["assignee_username"]])

    git_raw = collections.defaultdict(set)
    for commit in project_data.change_sets:
        git_raw[commit["author"]].update([commit["author_email"]])

    # flatten and IMPORTANT sort, to get stable input and thus ids
    # TODO: however, the same developers should ALWAYS get the same id
    #       e.g. what if project data is filtered, and once there are
    #       10 developers in the list and later 20. So the keys are no longer
    #       uniques. Maybe use UUIDS
    jira_users = sorted(_flatten(jira_raw), key=operator.itemgetter(0))
    git_users = sorted(_flatten(git_raw), key=operator.itemgetter(0))

    model = user_model.ProjectUsers()
    model.run(jira_users, git_users)

    return model
