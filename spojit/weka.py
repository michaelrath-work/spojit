import collections
import functools
import typing


ATT_TYPE_NUMERIC = "numeric"
ATT_TYPE_NOMINAL = "nominal"
ATT_TYPE_STRING = "string"

SUPPORTED_ATT_TYPES = {ATT_TYPE_NOMINAL, ATT_TYPE_NUMERIC, ATT_TYPE_STRING}

Attribute = collections.namedtuple(
    "Attribute", ["name", "att_type", "description", "nominal_values"]
)


def create_numeric_attribute(name: str, desc) -> Attribute:
    return Attribute(
        name, att_type=ATT_TYPE_NUMERIC, description=desc, nominal_values=None
    )


def create_string_attribute(name: str, desc) -> Attribute:
    return Attribute(
        name, att_type=ATT_TYPE_STRING, description=desc, nominal_values=None
    )


def create_nominal_attribute(name: str, values: list, desc) -> Attribute:
    assert isinstance(values, list)
    return Attribute(
        name, att_type=ATT_TYPE_NOMINAL, description=desc, nominal_values=values[:]
    )


###############################################################################
#
###############################################################################


Instances = collections.namedtuple(
    "Instances", ["relation_name", "attributes", "values"]
)


def create_dataset(relation_name: str, attributes: list) -> Instances:
    assert isinstance(attributes, list)
    empty_list = []
    return Instances(
        relation_name=relation_name, attributes=attributes, values=empty_list[:]
    )


def add_instance(dataset: Instances, instance: list) -> Instances:
    assert len(instance) == len(dataset.attributes)
    return dataset._replace(values=dataset.values + [instance])


###############################################################################
# ARFF EXPORT
###############################################################################


def _none_to_questionmark(arg):
    return "?" if arg is None else arg


def _numeric_to_str(precision, arg):
    if isinstance(arg, float):
        return "{1:.{0}f}".format(precision, arg)
    elif isinstance(arg, int):
        return str(arg)
    return arg


def _thread_first(f1, f2, arg):
    """f1(f2(arg))"""
    return f2(f1(arg))


_value_conversion_func = functools.partial(
    _thread_first, functools.partial(_numeric_to_str, 6), _none_to_questionmark
)


def instance_to_arff(inst: list) -> str:
    """Convert an instance to a line in ARFF file

    :param inst: instance to convert
    :return: line
    """
    return ",".join(map(_value_conversion_func, inst))


def _format_attribute_description(attr: Attribute) -> typing.List[str]:
    if attr.description is None or len(attr.description) == 0:
        return []

    return ["% " + s for s in attr.description.split("\n")]


def attribute_to_arff(attr: Attribute) -> typing.List[str]:
    """Transform attribute to ARFF format

    :param attr: attribute to transform
    :return: lines describing the attribute
    """
    desc = _format_attribute_description(attr)
    att_type = attr.att_type
    if attr.att_type == ATT_TYPE_NOMINAL:
        values = ",".join(attr.nominal_values)
        att_type = "{{{}}}".format(values)
    l = "@ATTRIBUTE {} {}".format(attr.name, att_type)
    return desc + [l]


def dataset_to_arff(dataset: Instances) -> typing.Iterator[str]:
    """Transform dataset tp ARFF format

    yields one line at the time

    :param dataset: dataset to convert
    :return: generator over lines
    """
    # header
    yield '@RELATION "{}"'.format(dataset.relation_name)

    # attributes
    for attr in dataset.attributes:
        yield ""
        yield from attribute_to_arff(attr)

    # data
    yield from ("", "@DATA")

    for inst in dataset.values:
        yield instance_to_arff(inst)
