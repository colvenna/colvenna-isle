## Change

Describe the product or design change and its effect.

## Validation

- [ ] `python3 scripts/check_docs.py`
- [ ] Service suites (`COLVENNA_ACCOUNT=123456789012`, see CONTRIBUTING.md); native suites through `scripts/build_product.py` when writer/query code changed.
- [ ] `python3 -m unittest discover -s tests/ggshield_scan` when `scripts/ggshield_scan.py` changed; the ggshield hook and the secret scan workflow pass (CONTRIBUTING.md#secret-scanning).
- [ ] Release copy, API contract, and engineering design remain consistent.
- [ ] Architecture artifacts/evidence refreshed when the diagram changed.
- [ ] Runtime/cloud claims are supported or explicitly marked as unverified.
