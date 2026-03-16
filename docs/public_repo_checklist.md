# Public repo checklist

## Before making the repo public

- choose and confirm the final license,
- replace any local-path references,
- make source URLs explicit,
- include one end-to-end example command sequence,
- add badges only after CI is stable,
- pin a minimal supported Python version,
- make output file names stable,
- add a changelog once functionality becomes real.

## Data transparency

- describe every source and its grain,
- document which data are lagged,
- identify optional vs required sources,
- log any known breaks in source history.

## Research transparency

- keep bridge metrics simple and interpretable,
- label every proxy clearly,
- separate measured variables from inferred variables,
- state what the repo does not identify causally.

## Release candidates

### v0.1

- working fetchers for priority sources,
- auction-week panel,
- descriptive charts,
- tests.

### v0.2

- richer maturity splits,
- bank-capacity overlays,
- optional TRACE integration,
- issue templates and better docs.

### v1.0

- stable CLI,
- reproducible end-to-end pipeline,
- paper-quality outputs,
- clearly versioned schemas.
