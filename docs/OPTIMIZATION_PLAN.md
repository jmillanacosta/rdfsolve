# Client-first grounding experiment

## Question

Does optional, source-labelled ontology evidence improve exact RDF answer F1 without increasing unnecessary model work?

`Client` owns discovery, typed records, field paths and source execution. `OntologyLookup` supplies an optional evidence overlay. MCP calls the client and returns concise observations. The model composes SELECT queries with retained paths and identities. Query preparation checks scope, ownership, binding dependencies and retained goal witnesses.

Ontology definitions explain terms. They do not prove that a dataset uses those terms correctly. A named parent supplies hierarchy context; it does not make a local instance a member of a different class. Class correspondence does not establish entity identity. Only exact IRIs or Bioregistry namespace/identifier correspondences permit an ontology alias to identify a returned source record.

## Conditions

- **Off:** the client uses mined endpoint metadata and supplied mappings.
- **On:** the same client also uses OLS labels, definitions, exact synonyms and direct named parents. Ontobee is an alternative provider, measured separately from the OLS ablation.
- Default is off. Opening a client makes no ontology requests.
- External evidence stays outside `MinedSchema`. Each term retains provider, requested IRI, resolved IRI, match basis and fetch time. Normalized provider responses are cached separately.
- The client enriches vocabulary in the requested schema region. Failed entity-name searches can use exact ontology aliases; candidates are verified in the configured dataset.
- Providers have timeouts, request and response limits. Unavailability is recorded separately from a completed no-match. A failed lookup cannot authorize a weaker query.

## Reproduction

Run `03_ontology_cache.ipynb` through the CPU notebook script to prepare the external evidence cache, then freeze it. Cache preparation sees the local schema and question concepts, never reference answer bindings. Inspect the saved provider trace and missing cache entries before running the model.

Run `04_ontology.ipynb` through `scripts/slurm_qwen_mcp.sh`. The notebook reads the case manifest, obtains independent reference answers through `Client.select`, and calls `rdfsolve.api.ask_rdf` for each condition. Use the same frozen RDF, schema, cache, model, temperature, seed and budgets. Alternate condition order across repeats. Snapshot the manifest and inputs with the run.

Start with two controls and vocabulary-dependent questions on the existing small AOPWiki and WikiPathways snapshots. The human-applicability sample must contain both matching and nonmatching roots. Preflight checks must show that dropping its applicability restriction changes the reference answer. Do not call an empty snapshot a successful answer to the live dataset question.

Three paired repeats are the default. Each run has 30 requests, 32,768 total output tokens, and 8,192 tokens per completion; temperature 0.2 and paired seeds 101 onward. Budget exhaustion scores zero. These are development cases, not a held-out benchmark. Changes after inspecting failures create a new experiment version; retain prior runs.

## Metrics

Compare sets of complete projected RDF tuples. Preserve URI/literal kind, lexical value, language, datatype and unbound positions. Reference queries are handwritten and kept outside the model context. These cases contain no projected blank nodes; a future blank-node case needs an explicit isomorphism policy.

Report per-case precision, recall, F1, exact answer match and completion. Failed or blocked runs score zero. These pilot references must be nonempty. A future empty-reference case should score an executed empty prediction as correct and preserve the separate completion check. Do not compare query strings or award correctness for a receipt alone.

Report macro F1 with paired on-minus-off deltas. Bootstrap by question, keeping repeated seeds together; with few questions the interval is descriptive and cannot establish general improvement. Report case-level regressions as well as gains.

Also record model requests, generated tokens, inclusive input tokens, cache reads, maximum request context, tool response bytes, source requests, ontology requests/cache misses, latency and stopping reason. Frozen-cache runs isolate evidence quality from provider latency. Separate live-provider probes measure availability and added requests. A cached ontology lookup is still evidence retrieval, not an endpoint query.

## Gates

1. Core tests prove schema immutability, exact identity verification, provider failure handling and shared-helper use for Ontobee.
2. MCP tests prove ontology evidence reaches preparation, while scope and ownership checks remain active. Keep at most six MCP test files and 2,000 lines total.
3. Run `uvx tox -e py,mcp,lint`; critical SDK tests must run in the cluster environment.
4. Run model experiments only through SLURM. Save notebook, source/input hashes, full host-side answers, call journals and source/ontology traces under `rdfsolve/logs/mcp-test`.
5. Examine the first failing stage before changing code. No extra critic, guessed alias list, relaxed scope check or automatic finalization on preparation.

## Experiment audit

The first human-only sample gave a perfect tuple score to a query that omitted the human restriction. Its raw scores and emitted query remain in the report. The sample now includes eight matching roots and eight nonmatching roots; both human cases declare a counterexample query. The notebook refuses to evaluate a snapshot that cannot distinguish that omission. Record tuple F1 and exact match separately from any query-semantic audit.

## Data identity

The CONSTRUCT reader now rejects relative IRIs lacking an explicit RDF base, with an anchored SELECT repair. The mining notebook explicitly excludes those values. This prevents RDFLib from inventing local file identities from endpoint identifiers. The experiment retains its original input copies; refreshed samples form a new input version even when the projected reference answers are unchanged.

Substring matches remain retrieval hints. Full normalized concept words are required by the current grounding gate; this prevents “taxon” from being approved by “taxonomy” alone. The gate remains a conservative lexical check. Receipt text reports the number of declared value restrictions so missing declarations are visible.

## Remaining work

- Integrate real mapping-stage metadata using existing mapping records.
- Evaluate sparse and conflicting ontology descriptions, provider coverage and unsupported local identifiers.
- Check the intended scope of initiating/outcome events separately from `has_key_event`.
- Improve the client's conservative lexical goal check where grounded semantics remain unresolved; do not call it a proof of natural-language correctness.
- Expand to pinned, independently reviewed non-federated questions after the pilot. Keep development and held-out questions separate.

## API references

[OLS API specification](https://www.ebi.ac.uk/ols4/v3/api-docs) defines term and search endpoints. Live probes require `queryFields=label,synonym` with `exact=true` for exact-name search. [Ontobee's SPARQL tutorial](https://ontobee.org/tutorial/sparql) describes its vocabulary graph; access uses the package's `SparqlHelper`. [SPARQL 1.1](https://www.w3.org/TR/sparql11-query/) governs query and optional-join semantics.
