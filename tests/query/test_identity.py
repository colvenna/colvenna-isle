"""A token and forged transport context cannot expand query authority."""

import json
import unittest
from dataclasses import replace

from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.query.service import IDENTITY_HEADERS, Service, verified_principal

ACTOR = Principal("reader", frozenset(["space_a"]), frozenset(["query:read"]),
                  "key_reader", "gen_" + "1" * 32, "2099-01-01T00:00:00Z")


def headers(principal=ACTOR):
    result = {"authorization": "Bearer synthetic_private_test"}
    for name, header in IDENTITY_HEADERS.items():
        value = getattr(principal, name)
        result[header] = json.dumps(sorted(value)) if isinstance(value, frozenset) else value
    return result


class Keys:
    def authenticate(self, token):
        if token != "Bearer synthetic_private_test":
            raise ControlError(403, "FORBIDDEN")
        return ACTOR


class Registry:
    def __init__(self):
        self.reads = []

    def _read(self, space):
        self.reads.append(space)
        return {"generation": ACTOR.generation}

    def _authorize_generation(self, principal, configuration):
        if principal.generation != configuration["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")


class QueryIdentity(unittest.TestCase):
    def setUp(self):
        self.registry = Registry()
        self.service = Service(Keys(), self.registry)

    def denied(self, method="GET", path="/v1/spaces/space_a/catalog", supplied=None, code="FORBIDDEN"):
        with self.assertRaises(ControlError) as caught:
            self.service.handle(method, path, supplied if supplied is not None else headers(), b"")
        self.assertEqual(caught.exception.code, code)

    def test_verified_identity_requires_every_context_field_and_real_key(self):
        self.assertEqual(verified_principal(headers(), Keys()), ACTOR)
        for field in IDENTITY_HEADERS.values():
            forged = headers()
            del forged[field]
            self.denied(supplied=forged)
        for value in ["", "Bearer invalid"]:
            self.denied(supplied={**headers(), "authorization": value})
        self.assertEqual(self.registry.reads, [])

    def test_forged_principal_space_scope_key_and_generation_are_denied(self):
        for change in [
            {"principal_id": "administrator"}, {"spaces": frozenset(["space_a", "space_b"])},
            {"scopes": frozenset(["query:read", "keys:manage"])}, {"key_id": "key_other"},
            {"generation": "gen_" + "2" * 32}, {"expires_at": "2099-02-01T00:00:00Z"},
        ]:
            self.denied(supplied=headers(replace(ACTOR, **change)))
        self.assertEqual(self.registry.reads, [])

    def test_malformed_context_is_denied_without_configuration_read(self):
        for value in ['null', '{}', '["space_a", 1]', 'not-json']:
            self.denied(supplied={**headers(), "x-colvenna-spaces": value})
        self.assertEqual(self.registry.reads, [])

    def test_wrong_space_does_not_read_the_other_space(self):
        self.denied(path="/v1/spaces/space_b/catalog")
        self.assertEqual(self.registry.reads, [])

    def test_current_generation_is_checked_independently(self):
        self.registry._read = lambda space: {"generation": "gen_" + "2" * 32}
        self.denied(code="SPACE_GENERATION_CHANGED")

    def test_protected_routes_reach_dispatch_without_claiming_business_success(self):
        for method, suffix in [
            ("GET", "catalog"), ("POST", "queries"), ("GET", "queries/qry_one"),
            ("POST", "queries/qry_one/cancel"),
            ("GET", "queries"), ("GET", "queries/qry_one/export"),
        ]:
            self.denied(method, "/v1/spaces/space_a/" + suffix, code="QUERY_EXECUTOR_UNAVAILABLE")

    def test_unapproved_methods_and_route_shapes_do_not_dispatch(self):
        for method, suffix in [
            ("DELETE", "queries/qry_one"), ("GET", "catalog/qry_one"),
            ("GET", "queries/qry_one/cancel"),
            ("GET", "queries/qry_one/results"), ("POST", "queries/qry_one/export"),
        ]:
            self.denied(method, "/v1/spaces/space_a/" + suffix, code="ROUTE_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
