from .codec import dumps, loads
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import read_record
from .transaction import WriteBatch


def merge_patch(document, patch):
    if not isinstance(patch, dict):
        return loads(dumps(patch))
    result = dict(document) if isinstance(document, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = merge_patch(result.get(key), value)
    return result


def patch_document(store, collection, key, patch, expected=None):
    if not isinstance(patch, dict):
        raise InvalidDocument("document merge patch must be an object")
    current = read_record(store, collection, key)
    if current is None:
        raise NotFound("cannot patch a missing document")
    if expected is not None and expected != current.revision:
        raise Conflict("patch revision changed")
    document = merge_patch(current.document, patch)
    return WriteBatch(store).put(collection, key, document, expected=current.revision,
                                             expires_at=current.expires_at).commit()
