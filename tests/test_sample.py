# Sample Test passing with nose and pytest
import spojit.sample as smpl


def my_sample() -> smpl.TrainingSample:
    s = smpl.TrainingSample()
    s.f00_commit_hash = "#deadbeaf"
    s.f01_issue_id = "SPOJIT-123"
    s.f03_commit_user_id = 3
    s.f04_issue_user_id = 2

    s.f06_is_commit_within_issue_life_time = True

    s.f07_09_previous_linked_commit["time_difference"] = None
    s.f07_09_previous_linked_commit["resource_overlap"] = 0.3
    s.f07_09_previous_linked_commit["committer_user_id"] = 10

    # number of linked commits to this issue at that point in time
    s.f11_num_commits_to_this_issues = 0

    #
    s.f12_num_open_issues_at_commit_time = 0

    #
    s.f13_num_open_issues_from_committer_at_commit_time = 0

    s.f14_time_difference_issue_creation_and_commit = None

    s.f15_time_difference_commit_and_issue_resolve = None

    # f16: close

    s.f17_19_next_linked_commit["time_difference"] = None
    s.f17_19_next_linked_commit["resource_overlap"] = None
    s.f17_19_next_linked_commit["committer_user_id"] = 33

    s.f30_max_similarity_code_to_issue_lsi = None
    s.f31_max_similarity_code_to_issue_vsm_with_ngram = None
    s.f32_similarity_commit_to_issue_vsm_with_ngram = None

    s.f40_max_trace_similarity = None

    s.f99_is_linked = True

    return s


def test_sample_creation():
    s = my_sample()

    foo = smpl.sample_to_weka_instance(s)
    print(foo)
    # print(smpl.SPOJIT_ATTRIBUTES)
    assert len(foo) == len(smpl.SPOJIT_ATTRIBUTES)
