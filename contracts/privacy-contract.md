# Privacy Contract

Privacy modes:

- `local_only`
- `redacted`
- `safe_summary`

Rules:

- Privacy is enforced in the Context SDK layer.
- Canonical and view products remain source-factual and provenance-rich.
- `local_only` fields must not be returned in `redacted` or `safe_summary` mode.
- Pseudonymization must be stable within one response bundle.
