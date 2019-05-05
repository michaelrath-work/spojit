import collections
import json
import typing


class Developer:
    """Developer/User class

    A user may have multiple names, e.g. 'Bruce Wayne', 'Master Wayne'
    and multiple users names, e.g. 'bwayne', 'batman', 'the-bat'
    but they all represent the same identity
    """

    def __init__(self, number, names, user_names):
        assert isinstance(names, set)
        assert isinstance(user_names, set)
        self.number = number

        self.names = names.copy()  # something like 'Bruce Wayne'
        self.user_names = user_names.copy()  # something like 'batman'

    def add_name(self, name):
        self.names.add(name)

    def add_user_name(self, user_name):
        self.user_names.add(user_name)

    def to_json(self):
        return {
            "number": self.number,
            "names": [n for n in self.names],
            "user_names": [n for n in self.user_names],
        }

    @staticmethod
    def from_json(obj):
        return Developer(obj["number"], set(obj["names"]), set(obj["user_names"]))

    def __str__(self):
        return "{}: names = {}, user_names = {}".format(
            self.number, repr(self.names), repr(self.user_names)
        )


class DeveloperTeam:
    """A list, aka team of developers
    """

    def __init__(self, team_name: str):
        self.members = []  # type: typing.List[Developer]
        self.team_name = team_name

    def add_or_update_developer(self, name, user_name) -> typing.Optional[Developer]:
        """
        Strategy:
            - first: check user_name, because this is often unique, e.g. email
            - second: check name

        :param name: name
        :param user_name: nickname
        :return:
        """
        if not name and not user_name:
            return None

        # check user name first
        matches = list(filter(lambda x: user_name in x.user_names, self.members))

        if matches:
            matches[0].add_name(name)  # register new name
            return matches[0]

        # check name second
        # TODO: maybe apply Levenshtein distance to sidestep errors
        matches = list(filter(lambda x: name in x.names, self.members))
        if matches:
            matches[0].add_user_name(user_name)  # register new user name
            return matches[0]

        # create a new developer
        dev = Developer(len(self.members), {name}, {user_name})
        self.members.append(dev)

        return dev

    def identify_by_user_name(self, user_name) -> typing.Optional[Developer]:
        if not user_name:
            return None

        matches = list(filter(lambda x: user_name in x.user_names, self.members))

        return matches[0] if matches else None

    def to_json(self):
        return [d.to_json() for d in self.members]

    def load_json(self, json_obj):
        self.members = [Developer.from_json(ele) for ele in json_obj]


LinkedDeveloper = collections.namedtuple("LinkedDeveloper", "jira_idx git_idx")


class ProjectUsers:
    """Manage several teams, notably JIRA and GIT
    """

    def __init__(self):
        # self.data_access = da
        self.jira_team = DeveloperTeam("jira")
        self.git_team = DeveloperTeam("git")
        self.team_links = []  # type: typing.List[LinkedDeveloper]
        self.project_name = "unnamed"

    def run(self, jira_users: list, git_users: list):
        for user_name, login in jira_users:
            self.jira_team.add_or_update_developer(user_name, login)

        for author, author_email in git_users:
            self.git_team.add_or_update_developer(author, author_email)

        self.link_teams()

    def get_stats(self):
        """Get statistics about project user

        :return: unique jira user, unique git users, linked users
        """
        unique_jira = sum(
            1 if (a is not None) and (b is None) else 0 for a, b in self.team_links
        )
        unique_git = sum(
            1 if (a is None) and (b is not None) else 0 for a, b in self.team_links
        )
        linked = sum(
            1 if (a is not None) and (b is not None) else 0 for a, b in self.team_links
        )
        return unique_jira, unique_git, linked

    def __str__(self):
        content = ""
        for jira_idx, git_idx in self.team_links:
            jira_dev = (
                self.jira_team.members[jira_idx] if jira_idx is not None else None
            )
            git_dev = self.git_team.members[git_idx] if git_idx is not None else None
            content += "%s\n%s\n\n" % (jira_dev, git_dev)

        return content

    def __len__(self):
        return len(self.team_links)

    def get_link_id(self, user_name, team_name):
        """Get a unique developer id which can be used to compare JIRA with git users

        :param user_name: the developer user name. For jira, this is the login name
                          and for git the email address
        :param team_name: either 'jira' or 'git
        :return:
        """
        for idx, linked_developer in enumerate(self.team_links):
            if team_name == "jira":
                if linked_developer.jira_idx is not None:
                    jira_dev = self.jira_team.members[linked_developer.jira_idx]
                    if user_name in jira_dev.user_names:
                        return idx

            elif team_name == "git":
                if linked_developer.git_idx is not None:
                    git_dev = self.git_team.members[linked_developer.git_idx]
                    if user_name in git_dev.user_names:
                        return idx
            else:
                raise ValueError("unknown team name '%s'" % team_name)

        return None

    def save(self, file_path: str, project_name: str):
        obj = {
            "project": project_name,
            "jira_users": self.jira_team.to_json(),
            "git_users": self.git_team.to_json(),
            "linked_users": [(ele.jira_idx, ele.git_idx) for ele in self.team_links],
        }

        with open(file_path, "w") as fp:
            json.dump(obj, fp, indent=2, sort_keys=True)

    def load(self, file_path: str):
        with open(file_path, "r") as fp:
            obj = json.load(fp)
            self.project_name = obj["project"]

            self.jira_team = DeveloperTeam("jira")
            self.jira_team.load_json(obj["jira_users"])

            self.git_team = DeveloperTeam("git")
            self.git_team.load_json(obj["git_users"])

            self.team_links = [
                LinkedDeveloper(jira_idx=a, git_idx=b) for a, b in obj["linked_users"]
            ]

    def link_teams(self):
        """Link the JIRA and Git teams

        Strategy: the matching is based on names only, because user_names are
                  email addresses in Git and nicknames in JIRA
        """
        remaining_git_ids = [dev.number for dev in self.git_team.members]

        for jira_dev in self.jira_team.members:
            matching_git_dev = None

            for idx in remaining_git_ids:
                git_dev = self.git_team.members[idx]

                for name in jira_dev.names:
                    # TODO: maybe use Levenshtein distance to side step typing errors
                    if name in git_dev.names:
                        matching_git_dev = git_dev
                        break

                if matching_git_dev:
                    remaining_git_ids.remove(idx)
                    break

            if matching_git_dev:
                self.team_links.append(
                    LinkedDeveloper(
                        jira_idx=jira_dev.number, git_idx=matching_git_dev.number
                    )
                )
            else:
                self.team_links.append(
                    LinkedDeveloper(jira_idx=jira_dev.number, git_idx=None)
                )

        for idx in remaining_git_ids:
            self.team_links.append(LinkedDeveloper(jira_idx=None, git_idx=idx))


#     def run_from_trace_ng_db(self, data_access: trace_ng_db.DataAccess):
#         self._build_jira(data_access)
#         self._build_git(data_access)
#         self.link_teams()
#
#     def _build_jira(self, data_access: trace_ng_db.DataAccess):
#         # language=sql
#         stmt = """
# SELECT DISTINCT issue.assignee AS name, issue.assignee_username AS email
# FROM issue
# WHERE       (issue.resolution = 'Done' OR issue.resolution = 'Fixed')
#         AND (issue.status = 'Resolved' OR issue.status = 'Closed')
#         """
#
#         for row in data_access.execute_sql(stmt):
#             if row[0] is not None and row[1] is not None:
#                 self.jira_team.add_or_update_developer(row[0], row[1])
#
#     def _build_git(self, data_access: trace_ng_db.DataAccess):
#         # language=sql
#         stmt = """
# SELECT DISTINCT change_set.author AS name,  change_set.author_email AS email
# FROM change_set
#         """
#         for row in self.data_access.execute_sql(stmt):
#             if row[0] is not None and row[1] is not None:
#                 self.git_team.add_or_update_developer(row[0], row[1])
