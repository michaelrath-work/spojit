from spojit import user_model


def test_developers():
    devs = user_model.DeveloperTeam("avengers")

    devs.add_or_update_developer("Tony Stark", "tony")
    devs.add_or_update_developer("Tony Stark", "iron man")

    devs.add_or_update_developer("Steven Rogers", "steve")
    devs.add_or_update_developer("Steven Rogers", "captain america")

    assert 2 == len(devs.members)
