"""Canonical GrapeRank constants — single source of truth.

Both `backend/graperank.py` and `indexer/graperank.py` are Python ports of the
NosFabrica/brainstorm_graperank_algorithm Java reference. Their per-observer
iteration loops differ (the backend version is tightly coupled to Postgres
BFS; the indexer version is a standalone class-based library), but the
NUMERIC CONSTANTS must be identical or the two code paths will produce
different scores for the same inputs.

This module is the one place those constants live. Each importer may rename
them locally (e.g. `backend/graperank.py` also applies env-var overrides for
ops tuning), but the defaults below are authoritative.

Reference: https://github.com/NosFabrica/brainstorm_graperank_algorithm
  - src/main/java/com/nosfabrica/graperank/grape/Constants.java
"""

# --- Ratings (signed edge weights) ---
RATING_FOLLOW = 1.0
RATING_MUTE = -0.1
RATING_REPORT = -0.1

# --- Confidences (how much to trust a rater's verdict) ---
CONFIDENCE_FOLLOW = 0.03                 # non-observer follow
CONFIDENCE_FOLLOW_OBSERVER = 0.5         # observer's own follow (high trust)
CONFIDENCE_MUTE = 0.5
CONFIDENCE_REPORT = 0.5

# --- Propagation parameters ---
ATTENUATION = 0.85                       # GLOBAL_ATTENUATION_FACTOR
RIGOR = 0.5                              # GLOBAL_RIGOR (confidence saturation rate)

# --- Convergence ---
CONVERGENCE_THRESHOLD = 0.0001           # min delta-influence to keep iterating
MAX_ITERATIONS = 100                     # hard safety cap; Java reference has none

# --- Classification cutoffs ---
CUTOFF_VALID_USER = 0.02                 # DEFAULT_CUTOFF_OF_VALID_USER
CUTOFF_TRUSTED_REPORTER = 0.1            # DEFAULT_CUTOFF_OF_TRUSTED_REPORTER
