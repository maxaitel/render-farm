from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from ipaddress import ip_address


SCRYPT_N = 2**14
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_KEY_LENGTH = 32


def hash_password(password: str) -> str:
    if len(password) < 12:
        raise ValueError("Passwords must be at least 12 characters long.")

    salt = secrets.token_bytes(16)
    derived = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        dklen=SCRYPT_KEY_LENGTH,
    )
    return "$".join(
        [
            "scrypt",
            str(SCRYPT_N),
            str(SCRYPT_R),
            str(SCRYPT_P),
            base64.urlsafe_b64encode(salt).decode("ascii"),
            base64.urlsafe_b64encode(derived).decode("ascii"),
        ]
    )


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, n_raw, r_raw, p_raw, salt_raw, hash_raw = encoded.split("$", 5)
    except ValueError:
        return False

    if algorithm != "scrypt":
        return False

    try:
        salt = base64.urlsafe_b64decode(salt_raw.encode("ascii"))
        expected = base64.urlsafe_b64decode(hash_raw.encode("ascii"))
        derived = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=int(n_raw),
            r=int(r_raw),
            p=int(p_raw),
            dklen=len(expected),
        )
    except Exception:
        return False

    return hmac.compare_digest(derived, expected)


def new_session_token() -> str:
    return secrets.token_urlsafe(48)


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def is_private_ip(value: str | None) -> bool:
    if not value:
        return False

    candidate = value.strip()
    if not candidate:
        return False
    if candidate in {"localhost", "127.0.0.1", "::1"}:
        return True

    try:
        parsed = ip_address(candidate)
    except ValueError:
        return False
    return parsed.is_private or parsed.is_loopback or parsed.is_link_local
