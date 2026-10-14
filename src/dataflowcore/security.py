"""Bounded service credentials with overlapping rotation and read-only access."""

import hashlib
import hmac
import re
from contextvars import ContextVar

from .contracts import Invalid

request_actor = ContextVar("dataflow_request_actor", default=None)


class Credentials:
    def __init__(self, admin, worker, viewer=(), insecure=False):
        self.insecure = insecure
        self.roles = {"admin": tuple(admin), "worker": tuple(worker), "viewer": tuple(viewer)}
        tokens = [token for values in self.roles.values() for token in values]
        if not insecure and (
            not self.roles["admin"]
            or not self.roles["worker"]
            or any(not isinstance(t, str) or not re.fullmatch(r"[!-~]{24,256}", t) for t in tokens)
            or len(tokens) != len(set(tokens))
            or len(tokens) > 24
        ):
            raise Invalid("configure distinct 24..256 character tokens; at most 24 credentials")

    def authenticate(self, header, worker_route=False):
        if self.insecure:
            return {"role": "worker" if worker_route else "admin", "principal": "insecure"}
        for role, tokens in self.roles.items():
            for token in tokens:
                if hmac.compare_digest(header.encode(), ("Bearer " + token).encode()):
                    if worker_route != (role == "worker"):
                        return None
                    return {
                        "role": role,
                        "principal": hashlib.sha256(token.encode()).hexdigest()[:16],
                    }
        return None


def token_list(value):
    return tuple(t.strip() for t in value.split(",") if t.strip())
