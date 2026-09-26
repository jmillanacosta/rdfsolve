Evaluation of question answering
================================

``rdfsolve.evaluation`` scores the answers of a system (for example a model with the MCP
tools) against reference answers, and gives the statistics to compare systems and to plan
an evaluation.

Scores
------

An answer and a reference are tables of RDF terms with the same column names.

Level ``term``
   A cell matches only the same RDF term. A plain literal and an ``xsd:string`` literal are
   the same term.

Level ``resource``
   A cell matches when both cells show the same resource. A resource is shown by its IRI,
   by a name (``rdfs:label``, ``dc:title``, ``dcterms:title``, ``skos:prefLabel``,
   ``schema:name``, ``foaf:name``, ``dcterms:alternative``, ``skos:altLabel`` and synonyms),
   by an identifier (``dc:identifier``, ``dcterms:identifier``, ``skos:notation``,
   ``schema:identifier``), by a web page (``foaf:page``, ``foaf:isPrimaryTopicOf``,
   ``schema:url``), or by the value of a key field of its class. A key field is found in
   the data: its literal values have at most 64 characters, and for at least 98% of its
   statements each resource has one value and each value belongs to one resource (for
   example a CAS number, or an HGNC identifier). The data resolves each value to the
   resources that have it as the value of one of these fields. Numbers match by value.
   Other values (descriptions, measurements, values of fields that are not keys) match only
   as terms.

Level ``linked``
   As ``resource``, and records linked by ``owl:sameAs`` or ``skos:exactMatch`` also match
   (for example the HGNC and the NCBI Gene records of one gene). This level is a
   sensitivity analysis: such links join records of different sources, not views of one
   record.

At each level, rows that are the same count once, and each answer row can match one
reference row (a maximum bipartite matching). Precision, recall and F1 follow from the
number of matched rows; an answer is an exact match when all rows match in both
directions. An answer that the system did not complete scores 0. At the resource level,
the scores also count the field changes, for example ``keid: label -> identifier``.

The resource level is the primary outcome when a question does not name the field of a
column: a key event given by its IRI, its identifier or its title is the same key event.
The term level shows how often the exact representation of the reference was chosen.

.. code-block:: python

   from rdfsolve.evaluation import score_levels

   scores = score_levels(reference_rows, answer_rows, ["ke", "title"], select)
   scores["resource"].f1, scores["resource"].exact, scores["resource"].substitutions

``select`` runs a SPARQL SELECT on the source and gives its bindings.

Statistics
----------

The question is the unit of analysis; attempts are repeated measurements of a question.

* Estimate of a condition: the mean over questions of the mean score per question.
* Interval: a two-stage bootstrap (questions with replacement, then attempts within each
  question), percentile interval. Pairs of conditions are resampled together, question by
  question.
* Test of two conditions: a sign-flip test on the differences per question, exact for up
  to 16 questions and Monte Carlo (100,000 sign patterns) above; the p values of several
  pairs are adjusted by the method of Holm.
* Repeatability: the intraclass correlation of attempts within questions, ICC(1), and the
  design effect ``1 + (k - 1) ICC`` of ``k`` attempts per question.

Planning
--------

The success of an attempt (exact match at the resource level) is modelled on the logit
scale: ``mu + u_q`` for condition A and ``mu + delta + u_q + w_q`` for condition B, with the
difficulty of a question ``u_q ~ N(0, sigma_u^2)`` and a question-dependent effect
``w_q ~ N(0, sigma_w^2)``. ``fit_components`` fits the model to a pilot by the posterior
mode under weakly informative priors (``mu, delta ~ N(0, 2.5^2)``; ``sigma_u`` and
``sigma_w`` half-normal with scale 2), with the marginal likelihood from Gauss-Hermite
quadrature. The priors keep the estimates finite when a small pilot separates the
conditions completely. ``power_table`` simulates studies with a number of questions and
attempts and gives the share in which the sign-flip test rejects at the chosen level; at
``delta = 0`` this share is the simulated type I error. ``minimum_detectable`` gives the
smallest difference in mean success rate with power 0.8, and ``sensitivity`` gives it for
other values of ``sigma_u``.
