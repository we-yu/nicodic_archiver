# nicodic_archiver

Scrape and store Nicodic BBS comments (batch, periodic, and optional web UI).

## Operator tooling (targets + saved archive)

For day-to-day maintenance by a single operator—listing and inspecting the
**target registry**, adding canonical targets, temporarily **deactivating**
rows, and listing / inspecting / **exporting** saved articles—use the bounded
`operator` command:

```text
python main.py operator help
```

Full workflows, env notes (`NICODIC_DB_PATH`, target DB path), and the
intentional **out-of-scope** boundary are documented in
[`docs/OPERATOR.md`](docs/OPERATOR.md).

Optional shell forwarder (run from repo root): `tools/nico-operator.sh`.

## Other CLI

Run `python main.py` with no arguments to see scrape, batch, web, telemetry
CSV, and legacy inspect/export commands. Telemetry remains a support layer;
the operator guide above does not replace those entries.
