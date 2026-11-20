from .codec import MISSING, dumps, field
from .errors import InvalidDocument

OPS = {"eq", "ne", "lt", "le", "gt", "ge", "in", "exists", "contains"}


def validate_predicate(expression, depth=0):
    if expression is None:
        return
    if depth > 16 or not isinstance(expression, dict):
        raise InvalidDocument("invalid predicate structure")
    if set(expression) in ({"and"}, {"or"}):
        children = next(iter(expression.values()))
        if not isinstance(children, list) or not children or len(children) > 64:
            raise InvalidDocument("boolean predicates require 1..64 children")
        for child in children:
            validate_predicate(child, depth + 1)
        return
    if set(expression) == {"not"}:
        validate_predicate(expression["not"], depth + 1)
        return
    if set(expression) != {"field", "op", "value"} or not isinstance(expression["op"], str) or expression["op"] not in OPS:
        raise InvalidDocument("predicate must have field, op and value")
    field({}, expression["field"])
    dumps(expression["value"])
    if expression["op"] == "in" and not isinstance(expression["value"], list):
        raise InvalidDocument("in requires an array")
    if expression["op"] == "exists" and not isinstance(expression["value"], bool):
        raise InvalidDocument("exists requires a boolean")


def equal(left, right):
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    if isinstance(left, dict) or isinstance(right, dict):
        return (isinstance(left, dict) and isinstance(right, dict) and left.keys() == right.keys()
                and all(equal(left[key], right[key]) for key in left))
    if isinstance(left, list) or isinstance(right, list):
        return (isinstance(left, list) and isinstance(right, list) and len(left) == len(right)
                and all(equal(a, b) for a, b in zip(left, right)))
    return left == right


def matches(document, expression):
    if expression is None:
        return True
    if "and" in expression:
        return all(matches(document, item) for item in expression["and"])
    if "or" in expression:
        return any(matches(document, item) for item in expression["or"])
    if "not" in expression:
        return not matches(document, expression["not"])
    value = field(document, expression["field"])
    op, other = expression["op"], expression["value"]
    if op == "exists":
        return (value is not MISSING) == other
    if value is MISSING:
        return False
    if op == "eq":
        return equal(value, other)
    if op == "ne":
        return not equal(value, other)
    if op == "in":
        return any(equal(value, choice) for choice in other)
    if op == "contains":
        if isinstance(value, list):
            return any(equal(item, other) for item in value)
        return isinstance(value, str) and isinstance(other, str) and other in value
    if isinstance(value, bool) or isinstance(other, bool):
        return False
    numeric = isinstance(value, (int, float)) and isinstance(other, (int, float))
    if not numeric and not (isinstance(value, str) and isinstance(other, str)):
        return False
    return {"lt": lambda: value < other, "le": lambda: value <= other,
            "gt": lambda: value > other, "ge": lambda: value >= other}[op]()


def equality_terms(expression):
    if expression is None:
        return []
    if "and" in expression:
        return [term for child in expression["and"] for term in equality_terms(child)]
    if expression.get("op") == "eq":
        return [(expression["field"], expression["value"])]
    return []
