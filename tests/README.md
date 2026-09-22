# Tests

Use one small input/output test per module. Write it before new logic.
Patch network calls, clocks and external processes. Run the real transformation.
Use a representative input and one meaningful failure in the same test.
Compare useful output with expected values. Name the failing operation in the assertion.
Use temporary directories. Do not call public endpoints or write into the checkout.
Avoid parameter grids, constructor checks and assertions for each internal step.
Keep the graph → pipeline → release fixture shared across the three pipeline stages.
Run `uvx tox` after each change set.
