import abc
import more_itertools
import re
import typing


class ArtifactFilter(abc.ABC):
    @abc.abstractmethod
    def issue_type_to_category(self, issue: dict) -> typing.Optional[str]:
        pass

    @abc.abstractmethod
    def is_relevant_issue(self, issue: dict) -> bool:
        """Check if issue is relevant for Link completion algorithm
        :param issue:
        :return:
        """
        pass

    @abc.abstractmethod
    def is_relevant_commit(self, commit: dict) -> bool:
        """Check if commit is relevant for Link completion algorithm
        :param commit:
        :return:
        """
        pass


class DefaultFilter(ArtifactFilter):
    """Default artifact filtering
    """

    ISSUE_TYPE_MAPPING = {
        "Bug": "bug",
        "Feature Request": "feature",
        "New Feature": "feature",
        "Enhancement": "improvement",
        "Improvement": "improvement",
        "Sub-task": "task",
        "Task": "task",
    }
    """Mapping from JIRA issue type to our description model"""

    SOURCE_FILE_EXTENSIONS = [
        ".java",
        ".as",
        ".h",
        ".hpp",
        ".hh",
        ".c",
        ".cpp",
        ".cc",
        ".groovy",
        ".scala",
    ]
    """File extensions recognized as 'source' files"""

    TEST_FILE_PATH_PATTERNS = [
        "^test/.*",
        ".*/tests?/.*",
        ".*[tT]esting.*",
        ".*Test.java",
        ".*/[tT]est.*",
        ".*-test/.*",
        # https://maven.apache.org/guides/introduction/introduction-to-the-standard-directory-layout.html
        "^src/test/.*",
        ".*/src/test.*" "^tests/src/.*",
        "^tests.*",
    ]
    """path patterns to distinguish 'real' source file from test files"""

    EXAMPLE_FILE_PATH_PATTERNS = [
        "^documentation/.*",  # drools
        "^examples/.*",  # seam2
        "^examples-.*",  # seam2, e.g. examples-ee6/
        ".*example.*",  # _generic_
        "^src/examples",  # groovy
        "^tutorial/.*",  # pig
    ]
    """path patterns recognized as example/documentation"""

    def __init__(self):
        pass

    def issue_type_to_category(self, issue: dict):
        return DefaultFilter.ISSUE_TYPE_MAPPING.get(issue["type"], None)

    def is_relevant_issue(self, issue: dict):
        # - issue has accepted type
        # and is considered as actively processed or implemented
        mapped_type = self.issue_type_to_category(issue)

        # TODO: Rethink the status/resolution handling. Both change over
        #       time, which is currently not directly supported. E.g.
        #       ISSUE-1 starts as 'Open' and thus is inherently relevant
        #       but later is set to 'Wont fix', which should render it
        #       irrelevant. Currently both values cannot be queried.

        return (
            mapped_type is not None
            and issue["resolution"]
            in [None, "Done", "Fixed"]  # actively developed  # implemented
            and issue["status"]
            in [
                "Open",
                "Reopened",  # actively developed
                "Resolved",
                "Closed",  # implemented
            ]
        )

    def is_relevant_commit(self, commit: dict):
        # commit contains at least one source file
        return (
            more_itertools.ilen(
                filter(DefaultFilter.is_source_file, commit["file_path"])
            )
            > 0
        )

    @staticmethod
    def has_source_file_extension(fn: str) -> bool:
        return any(
            map(lambda ext: fn.endswith(ext), DefaultFilter.SOURCE_FILE_EXTENSIONS)
        )

    @staticmethod
    def is_source_file(file_path: str) -> bool:
        """Check whether a file represents a 'valid' source file, i.e. one that contributes to the project

        :param file_path: path to check
        :return:
        """
        # TODO: RETHINK this

        if not DefaultFilter.has_source_file_extension(file_path):
            return False

        # TODO: what about this?
        # return not DefaultFilter.is_test_file(file_path) and not DefaultFilter.is_example_file(file_path)
        return True

    @staticmethod
    def is_test_file(file_path: str) -> bool:
        """Check if file path represents a test file

        >>> DefaultFilter.is_test_file('src/main/java/org/jboss/weld/environment/se/discovery/NewSEWeldDeployment.java')
        False
        >>> DefaultFilter.is_test_file('tests/src/main/java/org/jboss/webbeans/mock/MockJmsServices.java')
        True
        >>> DefaultFilter.is_test_file('tests-arquillian/src/main/java/org/jboss/weld/mock/cluster/SwitchableMockEELifecycle.java')
        True
        >>> DefaultFilter.is_test_file('test/org/apache/foo.java')
        True

        Heuristic to check, whether a given file path represents a test file
        :param file_path: path to check
        :return: True, if this is a test file
        """
        if not DefaultFilter.has_source_file_extension(file_path):
            return False

        for pattern in DefaultFilter.TEST_FILE_PATH_PATTERNS:
            if re.search(pattern, file_path):
                return True

        return False

    @staticmethod
    def is_example_file(file_path: str) -> bool:
        """Check if file path represents an example file

        :param file_path: path to check
        :return: True, if this is an example file
        """
        if not DefaultFilter.has_source_file_extension(file_path):
            return False

        for pattern in DefaultFilter.EXAMPLE_FILE_PATH_PATTERNS:
            if re.search(pattern, file_path):
                return True

        return False
