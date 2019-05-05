import spojit.weka as weka


def test_instance_conversion():
    s1 = weka.instance_to_arff([None])
    assert "?" == s1

    s1 = weka.instance_to_arff([1])
    assert "1" == s1

    s1 = weka.instance_to_arff([3.145])
    assert "3.145000" == s1

    s1 = weka.instance_to_arff(["spojit"])
    assert "spojit" == s1

    s1 = weka.instance_to_arff(["spojit", None, 1, 6.66, None])
    assert "spojit,?,1,6.660000,?" == s1


def test_attribute_format_no_desc():
    att = weka.create_numeric_attribute("numeric_1", "")

    l = weka.attribute_to_arff(att)
    assert len(l) == 1
    assert l[0] == "@ATTRIBUTE numeric_1 numeric"


def test_attribute_format_desc():
    att = weka.create_string_attribute("string_1", "description")

    l = weka.attribute_to_arff(att)
    assert len(l) == 2
    assert l[0] == "% description"
    assert l[1] == "@ATTRIBUTE string_1 string"


def test_attribute_format_multiline_desc():
    att = weka.create_numeric_attribute("numeric_1", desc="first line\n   second line")
    l = weka.attribute_to_arff(att)
    assert len(l) == 3
    assert l[0] == "% first line"
    assert l[1] == "%    second line"
    assert l[2] == "@ATTRIBUTE numeric_1 numeric"


def test_attribute_nominal():
    att = weka.create_nominal_attribute(
        "nominal_att", values=["linked", "non_linked"], desc=None
    )
    l = weka.attribute_to_arff(att)
    assert len(l) == 1
    assert l[0] == "@ATTRIBUTE nominal_att {linked,non_linked}"


def test_dataset_to_arff():
    a1 = weka.create_numeric_attribute("numeric_att", "numeric description")
    a2 = weka.create_string_attribute("string_att", "string description")

    dataset = weka.create_dataset("my_relation", [a1, a2])

    dataset = weka.add_instance(dataset, [1, None])
    dataset = weka.add_instance(dataset, [1, "spojit"])

    arff_gen = weka.dataset_to_arff(dataset)
    l = list(arff_gen)
    assert len(l) == 11

    # s = '\n'.join(l)
    # assert s == 'michael'
