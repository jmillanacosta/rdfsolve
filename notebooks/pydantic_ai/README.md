# Ask AOPWiki with PydanticAI

Open `01_ask_aopwiki.ipynb`. Reuse the session from
`../pydantic_clients/01_mine_explore.ipynb` and set the path to your local AOPWiki
Turtle dump. The schema comes from that saved session; the answers come from the
local dump, not the live endpoint.

Install from the repository root:

```bash
uv pip install -e '.[notebooks,agents]'
```

Select the `rdfsolve (Python 3.12)` kernel. Copy `.env.example` to `.env` here
and fill in `OPENAI_API_KEY` and `ANTHROPIC_API_KEY`. Do not put keys in cells.
The adapter is optional; ordinary RDF clients do not import PydanticAI.

## What the experiment does

- Give each model a fresh client and the same tools, question, and output columns.
- Keep reference SPARQL and answers outside its prompts and tools.
- Run generated and reference queries on the same frozen local RDF graph.
- Save dataset and schema hashes, the reference revision, messages, queries,
  results, timing, token usage, failures, and excluded questions under `runs/`.

The starting models are GPT-5.4 mini and Claude Haiku 4.5. Change their IDs in
`.env` if needed. Runs are sequential: four questions per configured model, one
attempt each, at most eight model requests and twelve tool calls per attempt.
Each response is limited to 1,500 output tokens. Model usage limits are not a
currency budget; provider billing still applies. Candidate queries run in a
separate process with a 45-second timeout, a 4 GiB memory limit, and a 5,000-row
answer limit. Updates, federation, and dataset loading are rejected.

The four cases cover a count, a selected adverse outcome, chemical identifiers,
and chemicals for a selected pathway. The initial base-dump trial returned no
chemical cross-reference rows; that empty-answer case remains visible. Other
repository queries are listed as outside this small test, not silently counted
as successes. The reference checkout is pinned and stored locally, not vendored.

## Read the scores

Exact match compares complete RDF rows, including duplicates. Precision, recall,
and F1 compare distinct rows; counts use exact equality only. RDF identifiers,
language tags, and datatypes remain significant. This is conservative: equivalent
literal spellings may differ. Blank-node answers need a different comparison and
are not scored. Reference failures are unscorable; model failures count as failed
attempts. Do not average only successful attempts or treat empty-answer matches
as evidence of broad recall.

This is a smoke test, not a general accuracy estimate. Public reference queries
may have appeared in model training; we prevent runtime access, not prior exposure.
The reused schema can differ from the dump. Both its hash and the dump's hash are
saved so this choice is visible. No claim about the current live endpoint follows
from these results.

The adapter follows the [PydanticAI toolset interface](https://ai.pydantic.dev/toolsets/).
Model details: [GPT-5.4 mini](https://developers.openai.com/api/docs/models/gpt-5.4-mini)
and [Claude models](https://platform.claude.com/docs/en/models/overview).
Reference questions: [AOP-Wiki-Queries](https://github.com/marvinm2/AOP-Wiki-Queries).
