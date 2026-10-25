"""Independently verified HTTP dispatch to asynchronous native query operations."""

import re
from urllib.parse import parse_qs

from colvenna_isle.contracts.validation import ContractError, validate
from colvenna_isle.control.http import decode_json, principal_from_event
from colvenna_isle.control.registry import ControlError

IDENTITY_HEADERS = {
    "principal_id": "x-colvenna-principal-id",
    "key_id": "x-colvenna-key-id",
    "spaces": "x-colvenna-spaces",
    "scopes": "x-colvenna-scopes",
    "generation": "x-colvenna-generation",
    "expires_at": "x-colvenna-expires-at",
}
ROUTE = re.compile(
    r"/v1/spaces/([^/]+)/(catalog|queries)(?:/([^/]+)(?:/(cancel|export))?)?"
)


def verified_principal(headers, keys):
    """Require overwritten Gateway context AND independently authenticated key."""
    context = {name: headers.get(header) for name, header in IDENTITY_HEADERS.items()}
    if any(not isinstance(value, str) or not value for value in context.values()):
        raise ControlError(403, "FORBIDDEN")
    principal = principal_from_event({"requestContext": {"authorizer": {"lambda": context}}})
    if keys.authenticate(headers.get("authorization", "")) != principal:
        raise ControlError(403, "FORBIDDEN")
    return principal


class Service:
    def __init__(self, keys, registry, backend=None):
        self.keys = keys
        self.registry = registry
        self.backend = backend

    def handle(self, method, path, headers, body, query=""):
        principal = verified_principal(headers, self.keys)
        match = ROUTE.fullmatch(path)
        if not match:
            raise ControlError(404, "ROUTE_NOT_FOUND")
        space, resource, query_id, action = match.groups()
        try:
            validate("identifier", space)
            if query_id:
                validate("identifier", query_id)
        except ContractError:
            raise ControlError(400, "INVALID_IDENTIFIER") from None
        principal.authorize(space, "query:read")
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        operation = (method, resource, bool(query_id), action)
        allowed = {
            ("GET", "catalog", False, None),
            ("POST", "queries", False, None),
            ("GET", "queries", True, None),
            ("POST", "queries", True, "cancel"),
            ("GET", "queries", True, "export"),
            ("GET", "queries", False, None),
        }
        if operation not in allowed:
            raise ControlError(404, "ROUTE_NOT_FOUND")
        if self.backend is None:
            raise ControlError(503, "QUERY_EXECUTOR_UNAVAILABLE")
        try:
            options = parse_qs(query, keep_blank_values=True, strict_parsing=True, max_num_fields=4)
            fields = {"token"} if action == "export" else {"page_token", "page_size", "view"} if query_id else set()
            if set(options) - fields or any(len(values) != 1 or not values[0] for values in options.values()):
                raise ValueError("Unexpected query option")
            options = {name: values[0] for name, values in options.items()}
            if "page_size" in options:
                if not options["page_size"].isascii() or not options["page_size"].isdecimal() \
                        or not 1 <= int(options["page_size"]) <= 10000:
                    raise ValueError("Page quota")
            if "view" in options and options["view"] not in {"metadata", "result", "sql"}:
                raise ValueError("Invalid view")
            if action == "export" and "token" not in options:
                raise ValueError("Export token required")
            if method == "GET" and body:
                raise ValueError("GET body")
            request = decode_json(body) if method == "POST" and body else {}
            if method == "POST":
                validate("query_request" if not query_id else "empty_request", request)
        except (ValueError, ContractError):
            raise ControlError(400, "INVALID_QUERY_REQUEST") from None
        if resource == "catalog":
            return 200, self.backend.catalog(principal, configuration)
        if method == "POST" and not query_id:
            return 202, self.backend.submit(principal, configuration, request)
        if action == "cancel":
            return 200, self.backend.cancel(principal, configuration, query_id)
        if action == "export":
            return 200, self.backend.export(principal, configuration, query_id, options["token"])
        if not query_id:
            return 200, self.backend.history(principal, configuration)
        return 200, self.backend.status(principal, configuration, query_id, options)
