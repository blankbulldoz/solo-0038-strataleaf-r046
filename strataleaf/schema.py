from .codec import MISSING, dumps, field, loads
from .errors import InvalidDocument

TYPES = {"string": str, "integer": int, "number": (int, float), "boolean": bool, "object": dict, "array": list}


def normalize_schema(schema):
    if not isinstance(schema, dict) or set(schema) - {"fields", "required", "additional"}:
        raise InvalidDocument("schema accepts fields, required and additional")
    fields = schema.get("fields", {})
    required = schema.get("required", [])
    if not isinstance(fields, dict) or not isinstance(required, list):
        raise InvalidDocument("fields must be an object and required an array")
    for path, kind in fields.items():
        field({}, path)
        if not isinstance(kind, str) or kind not in TYPES:
            raise InvalidDocument("unsupported field type: " + str(kind))
    if any(not isinstance(x, str) or x not in fields for x in required):
        raise InvalidDocument("required fields must have declared types")
    additional = schema.get("additional", True)
    if not isinstance(additional, bool) or len(set(required)) != len(required):
        raise InvalidDocument("invalid schema options")
    return {"fields": fields, "required": sorted(required), "additional": additional}


def validate_document(document, schema):
    if not isinstance(document, dict):
        raise InvalidDocument("a document must be a JSON object")
    if len(dumps(document).encode()) > 1024 * 1024:
        raise InvalidDocument("document exceeds 1 MiB")
    for path in schema["required"]:
        if field(document, path) is MISSING:
            raise InvalidDocument("missing required field: " + path)
    for path, kind in schema["fields"].items():
        value = field(document, path)
        if value is MISSING:
            continue
        accepted = isinstance(value, TYPES[kind])
        if kind in {"integer", "number"} and isinstance(value, bool):
            accepted = False
        if not accepted:
            raise InvalidDocument("field " + path + " must be " + kind)
    if not schema["additional"]:
        roots = {path.split(".")[0] for path in schema["fields"]}
        if set(document) - roots:
            raise InvalidDocument("undeclared top-level fields: " + ",".join(sorted(set(document) - roots)))
    return document
