# VIVO import

The generated Turtle describes the workflow, its stages, one execution,
resource observations, traces, software, containers, nodes, and provenance.

The collector also emits the established VIVO links:

- Felix Kummer's existing person URI to the Geoflow workflow via
  `rm:workflows`;
- FONDA B5 to the workflow via `rm:hasWorkflow`;
- the workflow run to the existing Kubernetes backend individual;
- Python and Shell as implementation languages;
- Florian Katerndahl and Dirk Pflugmacher as literal responsible-researcher
  names.

The Felix and B5 statements are written in the inverse direction expected by
their VIVO profile pages, making the workflow discoverable there without
creating duplicate people or subprojects.

## Import procedure

1. Validate the generated file with
   `python3 scripts/validate-repository.py`.
2. In VIVO, open **Site Admin > Add/Remove RDF data**.
3. Add the generated file from `metadata/generated`.
4. Open the workflow run, workflow, Felix Kummer, and B5 pages and verify the
   links and Energy and Carbon values.
5. Keep the adjacent `.metrics.json` file as the calculation audit; it is not
   uploaded to VIVO.

`metadata/examples/geoflow-example.ttl` is the corrected output from the
documented successful run. It is an example, not a file that should be
re-imported each time.

## Obsolete backend cleanup

An earlier import used a literal `Kubernetes` backend value. If it is still
present, use VIVO's **Remove RDF data** operation with:

```text
metadata/vivo/remove-obsolete-backend-literal.ttl
```

Only use this cleanup for the matching historical Geoflow run. New collector
output links directly to the existing Kubernetes backend resource.
