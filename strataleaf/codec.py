import hashlib
import json
import math
import re
from .errors import InvalidDocument

NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
MISSING = object()


def name(value):
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise InvalidDocument("names must match [a-z][a-z0-9_]{0,62}")
    return value


def key(value):
    if not isinstance(value, str) or not value or len(value.encode()) > 512:
        raise InvalidDocument("document key must be a nonempty string of at most 512 bytes")
    return value


def validate_json(value, depth=0):
    if depth > 32:
        raise InvalidDocument("document nesting exceeds 32 levels")
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float) and math.isfinite(value):
        return
    if isinstance(value, list):
        for item in value:
            validate_json(item, depth + 1)
        return
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        for item in value.values():
            validate_json(item, depth + 1)
        return
    raise InvalidDocument("only finite JSON values and string object keys are supported")


def dumps(value):
    validate_json(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def loads(value):
    return json.loads(value)


def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def field(document, path):
    if not isinstance(path, str) or not path or any(not p for p in path.split(".")):
        raise InvalidDocument("field path must have nonempty dot-separated components")
    value = document
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return MISSING
        value = value[part]
    return value


def scalar_token(value):
    if value is MISSING or isinstance(value, (dict, list)):
        return None
    if isinstance(value, bool):
        return "b:" + str(int(value))
    if isinstance(value, (int, float)):
        return "n:" + str(value) if isinstance(value, int) else "n:" + str(int(value)) if value.is_integer() else "n:" + repr(value)
    return "v:" + dumps(value)
