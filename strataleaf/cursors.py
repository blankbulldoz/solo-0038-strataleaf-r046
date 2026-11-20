import base64
import hashlib
import hmac
from .codec import dumps, loads
from .errors import InvalidCursor


def encode_cursor(store, payload):
    raw = dumps(payload).encode()
    signature = hmac.new(bytes.fromhex(store.get("cursor_secret")), raw, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(signature + raw).decode().rstrip("=")


def decode_cursor(store, token):
    try:
        if not isinstance(token, str) or len(token) > 8192:
            raise ValueError("invalid token length")
        data = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
        signature, raw = data[:32], data[32:]
        expected = hmac.new(bytes.fromhex(store.get("cursor_secret")), raw, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError("signature mismatch")
        payload = loads(raw)
        if not isinstance(payload, dict) or payload.get("database") != store.get("database_id"):
            raise ValueError("wrong database")
        return payload
    except (ValueError, TypeError, KeyError, UnicodeDecodeError) as exc:
        raise InvalidCursor("invalid or foreign continuation cursor") from exc
