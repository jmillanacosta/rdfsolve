# Optional ontology evidence: implementation and experiment

15 September 2026. Repository: `tgx-hpc:~/rdfsolve/rdfsolve-2`.

The preceding client-first work was committed as `1a92295f Use typed client operations for MCP retrieval`. This change adds ontology evidence in the client and tests its effect separately from source mining.

## Client API

```python
from rdfsolve.api import Client, OntologyLookup, ask_rdf

with Client.open("schema.json", ontology_grounding=True) as client:
    print(client.describe("measurement method", owners=["Key Event"]))
    records = client.find("human", kind="cellular organisms")
    print(client.trace())
    client.save_session("investigation.json")

answer = await ask_rdf(question, schema="schema.json", ontology_grounding=True)
```

The owner and class names above describe the AOPWiki example. The API accepts any configured source/schema. Use `OntologyLookup("ontobee", cache="ontology.json")` as the client argument to choose Ontobee. The agent API accepts `ontology_provider="ontobee"`. Frozen experiments use `ontology_cache=...` and `ontology_offline=True`.

`Client.vocabulary(iri)` returns labels, definitions, exact synonyms, direct named parents and lookup provenance. `describe`, field inspection and failed entity-name searches use this facility automatically when enabled. Mined schemas remain unchanged. Generated field paths and actual source identities determine the executable vocabulary.

An alias can identify a source record only after the client retrieves it and verifies an exact IRI or a Bioregistry namespace/identifier correspondence. A same-named source record with another identity is rejected. Related or broad synonyms do not become entity identities. Parent assertions supply explanatory context.

The separate `ontology.py` module owns provider access, bounded responses, timeouts, caching and status. Ontobee uses `SparqlHelper.select_with_fallback`. OLS uses its JSON API. MCP adds no tool or agent. Its existing observations show a bounded evidence summary; full provider traces remain in `client.session_metadata()["ontology"]` and the saved package journal.

## Provider checks

Both providers resolved `MMO_0000000` and `NCBITaxon_9606` from the cluster. Each check performed two term and two hierarchy operations, with zero source-dataset queries. OLS returned the measurement-method definition; Ontobee also returned several imported definitions and parent assertions for Homo sapiens. That variation is a reason to preserve provenance. Ontology descriptions alone do not establish a dataset-specific relationship's meaning.

OLS exact-name search requires `queryFields=label,synonym`; a broad search for “human” returned virus terms during investigation. The adapter also filters obsolete terms and validates the exact requested IRI. Nullable synonym metadata, provider failures, cached evidence and exhausted budgets have separate handling. A transient failure remains retryable through the client.

## Paired experiment

Seven development questions, three paired seeds (101–103), grounding off/on: 42 attempts. Model: Qwen3.6-35B-A3B Q8_0 through llama-server on SLURM. Temperature 0.2; 8,192 tokens per completion, 32,768 output tokens per attempt, 30 requests. Condition order alternates across seeds.

The runs use fixed local AOPWiki/WikiPathways RDF and a frozen OLS cache. The same question, data, seed and limits apply to each pair. Each run preserves source hashes, complete input copies, reference answers, calls, model usage and package traces. Handwritten reference queries execute through `Client.select` outside model context. Scoring compares complete RDF tuples, including term kind, datatype, language and unbound positions. Failures score zero.

The original sample's **raw tuple macro F1 was 0.190 off and 0.333 on**. The semantic audit rejects one apparent on-condition success: the seed-103 complex query omitted the human restriction, but received F1 = 1 because all sampled AOP roots matched humans. Thus seven raw exact matches on versus four off include one misleading success; six on-condition answers remain after that audit. The raw interval cannot establish improved query semantics.

| Question | Off mean tuple F1 | On mean tuple F1 |
|---|---:|---:|
| aop-identities | 0.333 | 0.667 |
| event-methods | 0.000 | 0.333 |
| event-sex | 0.000 | 0.000 |
| event-taxa | 0.000 | 0.000 |
| human-aops | 0.000 | 0.000 |
| human-events | 0.000 | 0.333 |
| wp-titles | 1.000 | 1.000 |

The `human-events` on score is the audited false positive. The standalone human-AOP case also used an insufficiently contrasting sample, although every attempt failed. The corrected notebook rejects a dataset on which removing the human restriction leaves the reference unchanged. The sample producer now retrieves eight matching roots and eight nonmatching roots.

Across 21 attempts per condition:

| Cost | Off | On |
|---|---:|---:|
| Model requests | 316 | 278 |
| Generated tokens | 198,598 | 171,158 |
| Inclusive input tokens | 2,806,031 | 2,430,328 |
| Cache-read tokens | 2,492,707 | 2,156,685 |
| Tool response bytes | 462,695 | 422,636 |
| Local source queries | 122 | 131 |

Costs include failures. Cache reads are part of inclusive input tokens. Lower total cost here is not evidence of consistently cheaper successful solving. Frozen-cache model runs made no ontology HTTP requests. The three jobs used identical input hashes; their only source difference was the CONSTRUCT reader fix, which local SELECT evaluation did not invoke.


These are a small development sample. The first human snapshot lacked contrasting roots. Its raw scores are retained as a diagnosed experimental failure; the refreshed benchmark requires contrasting roots. Unit fixtures separately reject misleading prose matches and wrong-owner joins. The bootstrap is descriptive; the questions are not a held-out benchmark.

## What remains difficult

- The model sometimes searches for a class name as an entity or uses a retained handle as bare SPARQL syntax. Several attempts exhaust generation limits during repair.
- The client requires too much literal word overlap for some goal checks. A query selecting the expected PATO biological-sex field was rejected because its goal also said “applicability.” The ontology definition was available. This is a grounding-check limitation.
- “Applicable taxon” can lead to the ontology term for the discipline of taxonomy. Its definition does not establish the intended applicability relation. The corrected client keeps partial-word matches as weak retrieval hints. They cannot satisfy the grounding check. The corrected-sample run exposed the earlier bug: “taxon” inside “taxonomy” had received a score sufficient to approve the wrong field.
- The complex human/AOP/Key Event question still needs stronger package-backed grounding and simpler recovery. A missing original-question clause can escape a ledger built only from model-declared goals. Ontology descriptions do not repair that omission, and the false-positive sample concealed it.

The next experiment should improve the client’s field-selection and goal-repair interface while retaining owner, path, identity and scope checks. Keep ontology retrieval optional while measuring that change. A larger prompt or another reviewer would not establish the missing relationship evidence.

The final receipt now reports the number of declared value restrictions. A missing initial restriction can therefore be visible as “Recorded value restrictions: 0.” This is a factual trace of declared goals; it cannot recover a clause the model never declared.

## Data and diagram fixes

`SparqlHelper.construct_graph` previously allowed RDFLib to resolve relative endpoint IRIs against the local working directory. The reader now rejects unresolved relative identities with an anchored-SELECT repair. Explicit RDF bases remain supported. The mining notebooks explicitly exclude IRIs lacking a scheme and record that restriction in their source queries.

Refreshed samples contain zero parser-generated `file:` terms. AOPWiki-small changed from 1,962 to 1,872 triples, human-AOP from 5,024 to 4,908; WikiPathways remained at 9,812. All seven projected reference result sets are identical before and after refresh. Earlier model runs retain their original input copies; refreshed files are a new input version. The subsequent contrasting-root sample has 203 AOP identities. Removing the human restriction changes the AOP answer from 8 to 203 and the event answer from 199 to 270, exposing 71 false event tuples. Both cases now have executable preflight counterexamples.

Model diagrams now come directly from generated-model links. The conflicting `pydantic-2-mermaid` dependency was removed. The native client notebook completed on SLURM with 19 lung-related AOPs and seven AOP/chemical associations; batched and individual path evaluation agreed. Mermaid rendering still depends on the notebook viewer.

## Verification and reproduction

- `uvx tox -e py,mcp,lint`: 473 passed in the full suite; 51 passed again in the dedicated MCP/SDK environment; lint and format pass. The 51 are included in the full count.
- Six MCP test files, 1,341 lines combined. Ontology behavior is covered by ten compact core tests plus two MCP integration cases.
- Mutation checks reject an unverified same-name entity, a wrong ontology IRI and substring-only semantic acceptance.
- Core imports and generated models work with all MCP/PydanticAI/OpenAI imports blocked. `uv pip check` reports compatible dependencies.
- Both SLURM scripts pass shell syntax checks; all notebook code parses.

The full suite emits existing warnings, principally RDFLib deprecations (1,198 in the full run, 33 in the separate MCP run). They remain visible.

```bash
RDFSOLVE_NOTEBOOK=03_ontology_cache.ipynb RDFSOLVE_RUN_KIND=ontology-cache \
  sbatch scripts/slurm_mcp_mine.sh
RDFSOLVE_NOTEBOOK=04_ontology.ipynb sbatch scripts/slurm_qwen_mcp.sh
```

Logs: `~/rdfsolve/logs/mcp-test/`. Main paired jobs: `112620`, `112621`, `112622`. Setup pilot `112616` was aborted after expensive generation without a prepared query; its attempted case remains a recorded failure. Completed pilot `112617` is separate from the three-seed cohort. Cache-build failures and their corrections remain recorded. Final source/data measurement-method smoke check `112625` scored zero in both conditions. The vocabulary gain did not repeat there. Contrasting-root preparation: `112626`; corrected human-event paired check `112627` scored 0 off and 0.084 on. The on query still omitted the human restriction and used the taxonomy field, yielding only 13 correct tuples out of 111 returned, against 199 expected. That failure led to the partial-word grounding fix. Final guarded rerun `112628` blocked in both conditions (F1 0/0), preserving the unresolved meaning instead of executing the taxonomy substitution.

Use each run’s `inputs/`, `source/`, `run-manifest.json`, `experiment-config.json` and `experiment-inputs.json` to reproduce that version. `ontology-confirmation.json` and `summarize_ontology.py` retain the combined calculation. Provider checks, snapshot verification and test logs are beside the runs.

## Primary references

[OLS API](https://www.ebi.ac.uk/ols4/v3/api-docs), [Ontobee SPARQL tutorial](https://ontobee.org/tutorial/sparql), [SPARQL 1.1](https://www.w3.org/TR/sparql11-query/), [Turtle IRI resolution](https://www.w3.org/TR/turtle/#sec-iri-references). RDF base resolution is legitimate when its base is established; the client’s conservative policy prevents accidental use of a local filesystem base for remote data.
