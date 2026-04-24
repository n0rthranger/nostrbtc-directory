# GrapeRank unit tests
#
# Tests for both the standalone module (indexer/graperank.py) and
# a production-compatible algorithm replica.
# Includes cross-validation that both implementations produce identical results,
# and hand-computed numeric tests verified against the Java reference.
#
# Reference: https://github.com/NosFabrica/brainstorm_graperank_algorithm
# Licensed under AGPL-3.0 — see LICENSE file in project root.

import sys
import os
import math
import importlib.util
from collections import defaultdict

# Load indexer/graperank.py under a disambiguated name so a plain
# `import graperank` can never be affected by sys.path ordering.
_INDEXER_GR = os.path.join(os.path.dirname(__file__), '..', 'indexer', 'graperank.py')
_spec = importlib.util.spec_from_file_location("indexer_graperank", _INDEXER_GR)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

graperank = _mod.graperank
Rating = _mod.Rating
DEFAULT_ATTENUATION = _mod.DEFAULT_ATTENUATION
DEFAULT_RIGOR = _mod.DEFAULT_RIGOR
DEFAULT_PRECISION = _mod.DEFAULT_PRECISION
DEFAULT_FOLLOW_SCORE = _mod.DEFAULT_FOLLOW_SCORE
DEFAULT_MUTE_SCORE = _mod.DEFAULT_MUTE_SCORE
DEFAULT_REPORT_SCORE = _mod.DEFAULT_REPORT_SCORE
DEFAULT_OBSERVER_CONFIDENCE = _mod.DEFAULT_OBSERVER_CONFIDENCE
DEFAULT_FOLLOW_CONFIDENCE = _mod.DEFAULT_FOLLOW_CONFIDENCE
DEFAULT_MUTE_CONFIDENCE = _mod.DEFAULT_MUTE_CONFIDENCE
DEFAULT_REPORT_CONFIDENCE = _mod.DEFAULT_REPORT_CONFIDENCE
DEFAULT_CUTOFF_VALID_USER = _mod.DEFAULT_CUTOFF_VALID_USER
DEFAULT_MAX_ITERATIONS = _mod.DEFAULT_MAX_ITERATIONS


# ============================================================================
# Helpers
# ============================================================================

def make_follow(rater, ratee, observer=None):
    """Create a follow rating with reference-matching confidence."""
    c = DEFAULT_OBSERVER_CONFIDENCE if rater == observer else DEFAULT_FOLLOW_CONFIDENCE
    return Rating(rater, ratee, DEFAULT_FOLLOW_SCORE, c)


def make_mute(rater, ratee):
    """Create a mute rating: score=-0.1, confidence=0.5 (matches Java reference)."""
    return Rating(rater, ratee, DEFAULT_MUTE_SCORE, DEFAULT_MUTE_CONFIDENCE)


def make_report(rater, ratee):
    """Create a report rating: score=-0.1, confidence=0.5 (matches Java reference)."""
    return Rating(rater, ratee, DEFAULT_REPORT_SCORE, DEFAULT_REPORT_CONFIDENCE)


# ============================================================================
# Production-compatible algorithm replica
# ============================================================================
# These constants and functions exactly match the current nostrbtc.com scorer.
# If you change production scorer constants, update here too.

_PROD_RATING_FOLLOW = 1.0
_PROD_RATING_MUTE = -0.1
_PROD_RATING_REPORT = -0.1
_PROD_CONFIDENCE_FOLLOW = 0.03
_PROD_CONFIDENCE_FOLLOW_OBSERVER = 0.5
_PROD_CONFIDENCE_MUTE = 0.5
_PROD_CONFIDENCE_REPORT = 0.5
_PROD_ATTENUATION = 0.85
_PROD_RIGOR = 0.5
_PROD_CONVERGENCE_THRESHOLD = 0.0001
_PROD_CUTOFF_VALID_USER = 0.02
_PROD_MAX_ITERATIONS = 100


def _prod_input_to_confidence(total_input, rigor=_PROD_RIGOR):
    """confidence = 1 - rigor^input (same as production)"""
    decay_rate = -math.log(rigor) if rigor > 0 else 1.0
    return 1.0 - math.exp(-total_input * decay_rate)


def _prod_build_inputs(edges, observer, relevant_users):
    """Build {ratee: [(rater, rating, confidence), ...]} from edges."""
    inputs = defaultdict(list)
    for source, target, edge_type in edges:
        if source not in relevant_users:
            continue
        if edge_type == "follow":
            rating = _PROD_RATING_FOLLOW
            confidence = _PROD_CONFIDENCE_FOLLOW_OBSERVER if source == observer else _PROD_CONFIDENCE_FOLLOW
        elif edge_type == "mute":
            rating = _PROD_RATING_MUTE
            confidence = _PROD_CONFIDENCE_MUTE
        elif edge_type == "report":
            rating = _PROD_RATING_REPORT
            confidence = _PROD_CONFIDENCE_REPORT
        else:
            continue
        inputs[target].append((source, rating, confidence))
    return dict(inputs)


def _prod_iterate(inputs, scorecards):
    """Same ordered worklist iteration used by the current production scorer."""
    rater_to_ratees = defaultdict(set)
    for ratee, inp_list in inputs.items():
        for rater, _, _ in inp_list:
            rater_to_ratees[rater].add(ratee)

    worklist = set(scorecards.keys())
    rounds = 0
    while rounds < _PROD_MAX_ITERATIONS and worklist:
        next_worklist = set()
        for key in worklist:
            sc = scorecards.get(key)
            if sc is None or sc["observer"] == sc["observee"]:
                continue
            sum_w = 0.0
            sum_wxr = 0.0
            for rater, rating, conf in inputs.get(sc["observee"], []):
                rater_sc = scorecards.get(rater)
                if rater_sc is None:
                    continue
                w = conf * rater_sc["influence"] * _PROD_ATTENUATION
                sum_w += w
                sum_wxr += w * rating
            avg_score = sum_wxr / sum_w if sum_w != 0 else 0.0
            confidence = _prod_input_to_confidence(sum_w)
            new_influence = max(avg_score * confidence, 0.0)
            delta = abs(new_influence - sc["influence"])
            if delta > _PROD_CONVERGENCE_THRESHOLD:
                next_worklist.update(rater_to_ratees.get(key, ()))
            sc["averageScore"] = avg_score
            sc["input"] = sum_w
            sc["confidence"] = confidence
            sc["influence"] = new_influence
        rounds += 1
        worklist = next_worklist
    for sc in scorecards.values():
        sc["verified"] = sc["influence"] >= _PROD_CUTOFF_VALID_USER
    return scorecards, rounds


def prod_graperank(observer, edges):
    """End-to-end production GrapeRank for testing.

    edges: [(source, target, edge_type), ...]
    Returns {pubkey: influence_score}
    """
    all_users = set()
    for s, t, _ in edges:
        all_users.add(s)
        all_users.add(t)

    inputs = _prod_build_inputs(edges, observer, all_users)

    scorecards = {}
    for user in all_users:
        if user == observer:
            scorecards[user] = {
                "observer": observer, "observee": user,
                "averageScore": 1.0, "input": float("inf"),
                "confidence": 1.0, "influence": 1.0,
                "verified": True,
            }
        else:
            scorecards[user] = {
                "observer": observer, "observee": user,
                "averageScore": 0.0, "input": 0.0,
                "confidence": 0.0, "influence": 0.0,
                "verified": False,
            }

    scorecards, rounds = _prod_iterate(inputs, scorecards)
    return {pk: sc["influence"] for pk, sc in scorecards.items() if pk != observer}


# ============================================================================
# Helper: convert edge list to Rating list for cross-validation
# ============================================================================

def _edges_to_ratings(observer, edges):
    """Convert production-style edges to Rating objects for the standalone module."""
    ratings = []
    for source, target, edge_type in edges:
        if edge_type == "follow":
            ratings.append(make_follow(source, target, observer=observer))
        elif edge_type == "mute":
            ratings.append(make_mute(source, target))
        elif edge_type == "report":
            ratings.append(make_report(source, target))
    return ratings


def _edges_to_pubkeys(observer, edges):
    """Extract all non-observer pubkeys from edges."""
    pks = set()
    for s, t, _ in edges:
        pks.add(s)
        pks.add(t)
    pks.discard(observer)
    return list(pks)


# ============================================================================
# 1. Constants validation — verify module constants match Java reference
# ============================================================================

def test_constants_match_java_reference():
    """All module constants must match the Java Constants.java defaults."""
    assert DEFAULT_ATTENUATION == 0.85, f"ATTENUATION should be 0.85, got {DEFAULT_ATTENUATION}"
    assert DEFAULT_RIGOR == 0.5, f"RIGOR should be 0.5, got {DEFAULT_RIGOR}"
    assert DEFAULT_PRECISION == 0.0001, f"PRECISION should be 0.0001, got {DEFAULT_PRECISION}"
    assert DEFAULT_FOLLOW_SCORE == 1.0, f"FOLLOW_SCORE should be 1.0, got {DEFAULT_FOLLOW_SCORE}"
    assert DEFAULT_MUTE_SCORE == -0.1, f"MUTE_SCORE should be -0.1, got {DEFAULT_MUTE_SCORE}"
    assert DEFAULT_REPORT_SCORE == -0.1, f"REPORT_SCORE should be -0.1, got {DEFAULT_REPORT_SCORE}"
    assert DEFAULT_OBSERVER_CONFIDENCE == 0.5
    assert DEFAULT_FOLLOW_CONFIDENCE == 0.03
    assert DEFAULT_MUTE_CONFIDENCE == 0.5
    assert DEFAULT_REPORT_CONFIDENCE == 0.5
    assert DEFAULT_CUTOFF_VALID_USER == 0.02


def test_constants_match_production():
    """Module constants must match the production algorithm constants."""
    assert DEFAULT_ATTENUATION == _PROD_ATTENUATION
    assert DEFAULT_RIGOR == _PROD_RIGOR
    assert DEFAULT_PRECISION == _PROD_CONVERGENCE_THRESHOLD
    assert DEFAULT_FOLLOW_SCORE == _PROD_RATING_FOLLOW
    assert DEFAULT_MUTE_SCORE == _PROD_RATING_MUTE
    assert DEFAULT_REPORT_SCORE == _PROD_RATING_REPORT
    assert DEFAULT_OBSERVER_CONFIDENCE == _PROD_CONFIDENCE_FOLLOW_OBSERVER
    assert DEFAULT_FOLLOW_CONFIDENCE == _PROD_CONFIDENCE_FOLLOW
    assert DEFAULT_MUTE_CONFIDENCE == _PROD_CONFIDENCE_MUTE
    assert DEFAULT_REPORT_CONFIDENCE == _PROD_CONFIDENCE_REPORT


def test_constants_match_shared_source_of_truth():
    """indexer/graperank.py intentionally DUPLICATES the constants from
    shared/graperank_constants.py (so tests can cross-check independently).
    This test asserts the duplication is faithful — if someone bumps a
    number in shared/ without also bumping indexer/graperank.py, this test
    flags it before the backend and test-reference silently diverge."""
    _SHARED = os.path.join(os.path.dirname(__file__), '..', 'shared', 'graperank_constants.py')
    _sspec = importlib.util.spec_from_file_location("shared_grc", _SHARED)
    _smod = importlib.util.module_from_spec(_sspec)
    _sspec.loader.exec_module(_smod)

    assert DEFAULT_ATTENUATION == _smod.ATTENUATION
    assert DEFAULT_RIGOR == _smod.RIGOR
    assert DEFAULT_PRECISION == _smod.CONVERGENCE_THRESHOLD
    assert DEFAULT_MAX_ITERATIONS == _smod.MAX_ITERATIONS
    assert DEFAULT_FOLLOW_SCORE == _smod.RATING_FOLLOW
    assert DEFAULT_MUTE_SCORE == _smod.RATING_MUTE
    assert DEFAULT_REPORT_SCORE == _smod.RATING_REPORT
    assert DEFAULT_OBSERVER_CONFIDENCE == _smod.CONFIDENCE_FOLLOW_OBSERVER
    assert DEFAULT_FOLLOW_CONFIDENCE == _smod.CONFIDENCE_FOLLOW
    assert DEFAULT_MUTE_CONFIDENCE == _smod.CONFIDENCE_MUTE
    assert DEFAULT_REPORT_CONFIDENCE == _smod.CONFIDENCE_REPORT
    assert DEFAULT_CUTOFF_VALID_USER == _smod.CUTOFF_VALID_USER


# ============================================================================
# 2. Confidence formula — verify against Java convertInputToConfidence
# ============================================================================

def test_confidence_formula():
    """confidence = 1 - rigor^input = 1 - exp(-input * -ln(rigor))"""
    # With rigor=0.5: confidence(1.0) = 1 - 0.5^1 = 0.5
    assert abs(_prod_input_to_confidence(1.0) - 0.5) < 0.0001
    # confidence(2.0) = 1 - 0.5^2 = 0.75
    assert abs(_prod_input_to_confidence(2.0) - 0.75) < 0.0001
    # confidence(0.0) = 1 - 0.5^0 = 0
    assert abs(_prod_input_to_confidence(0.0) - 0.0) < 0.0001
    # confidence(10.0) should be close to 1.0
    assert _prod_input_to_confidence(10.0) > 0.999
    # confidence(0.425) — observer follow weight
    expected = 1.0 - math.exp(-0.425 * math.log(1.0 / 0.5))
    assert abs(_prod_input_to_confidence(0.425) - expected) < 0.0001


# ============================================================================
# 3. Hand-computed numeric tests — verified against Java formula
# ============================================================================

def test_numeric_single_follow():
    """Hand-computed: observer → A (single follow).

    Observer influence = 1.0
    Weight = confidence * influence * attenuation = 0.5 * 1.0 * 0.85 = 0.425
    sum_w = 0.425, sum_wxr = 0.425 (rating=1.0)
    avg = 1.0
    confidence = 1 - exp(-0.425 * ln(2)) = 1 - 0.5^0.425 ≈ 0.2551
    influence = 1.0 * 0.2551 = 0.2551
    """
    expected_influence = 1.0 - math.exp(-0.425 * math.log(1.0 / 0.5))
    # ~0.2551

    # Production implementation
    prod = prod_graperank("obs", [("obs", "A", "follow")])
    assert abs(prod["A"] - expected_influence) < 0.001, \
        f"Production: expected ~{expected_influence:.4f}, got {prod['A']:.4f}"

    # Standalone module
    standalone = graperank("obs", ["A"], [make_follow("obs", "A", observer="obs")])
    assert abs(standalone["A"] - expected_influence) < 0.001, \
        f"Standalone: expected ~{expected_influence:.4f}, got {standalone['A']:.4f}"


def test_numeric_two_hop_chain():
    """Hand-computed: observer → A → B (two-hop chain).

    A's influence ≈ 0.2551 (from test above)
    B's weight from A = 0.03 * 0.2551 * 0.85 ≈ 0.006505
    B's avg = 1.0
    B's confidence = 1 - exp(-0.006505 * ln(2)) ≈ 0.004502
    B's influence ≈ 0.004502
    """
    a_influence = 1.0 - math.exp(-0.425 * math.log(2.0))
    b_weight = 0.03 * a_influence * 0.85
    b_confidence = 1.0 - math.exp(-b_weight * math.log(2.0))
    b_expected = 1.0 * b_confidence  # avg=1.0

    edges = [("obs", "A", "follow"), ("A", "B", "follow")]
    prod = prod_graperank("obs", edges)
    assert abs(prod["A"] - a_influence) < 0.001
    assert abs(prod["B"] - b_expected) < 0.001, \
        f"B: expected ~{b_expected:.6f}, got {prod['B']:.6f}"

    ratings = _edges_to_ratings("obs", edges)
    standalone = graperank("obs", ["A", "B"], ratings)
    assert abs(standalone["B"] - b_expected) < 0.001, \
        f"Standalone B: expected ~{b_expected:.6f}, got {standalone['B']:.6f}"


def test_numeric_observer_follow_plus_mute():
    """Hand-computed: observer follows AND mutes A.

    Follow weight = 0.5 * 1.0 * 0.85 = 0.425, wxr = 0.425 * 1.0 = 0.425
    Mute weight   = 0.5 * 1.0 * 0.85 = 0.425, wxr = 0.425 * (-0.1) = -0.0425
    sum_w = 0.85, sum_wxr = 0.3825
    avg = 0.3825 / 0.85 = 0.45
    confidence = 1 - exp(-0.85 * ln(2)) ≈ 0.4466
    influence = 0.45 * 0.4466 ≈ 0.2010
    """
    sum_w = 0.85
    sum_wxr = 0.425 * 1.0 + 0.425 * (-0.1)
    avg = sum_wxr / sum_w
    conf = 1.0 - math.exp(-sum_w * math.log(2.0))
    expected = max(avg * conf, 0.0)

    prod = prod_graperank("obs", [("obs", "A", "follow"), ("obs", "A", "mute")])
    assert abs(prod["A"] - expected) < 0.001, \
        f"Expected ~{expected:.4f}, got {prod['A']:.4f}"

    ratings = [make_follow("obs", "A", observer="obs"), make_mute("obs", "A")]
    standalone = graperank("obs", ["A"], ratings)
    assert abs(standalone["A"] - expected) < 0.001


# ============================================================================
# 4. Cross-validation: standalone module vs production algorithm
# ============================================================================

def _cross_validate(observer, edges, tolerance=0.001):
    """Run both implementations on the same graph, assert scores match."""
    prod = prod_graperank(observer, edges)
    pubkeys = _edges_to_pubkeys(observer, edges)
    ratings = _edges_to_ratings(observer, edges)
    standalone = graperank(observer, pubkeys, ratings)

    for pk in pubkeys:
        p = prod.get(pk, 0.0)
        s = standalone.get(pk, 0.0)
        assert abs(p - s) < tolerance, \
            f"Mismatch for {pk}: production={p:.6f} standalone={s:.6f} (delta={abs(p-s):.6f})"


def test_cross_simple_chain():
    """Cross-validate: observer → A → B → C."""
    _cross_validate("obs", [
        ("obs", "A", "follow"),
        ("A", "B", "follow"),
        ("B", "C", "follow"),
    ])


def test_cross_fan_out():
    """Cross-validate: observer follows 5 users."""
    _cross_validate("obs", [
        ("obs", "A", "follow"),
        ("obs", "B", "follow"),
        ("obs", "C", "follow"),
        ("obs", "D", "follow"),
        ("obs", "E", "follow"),
    ])


def test_cross_diamond():
    """Cross-validate: observer → A, observer → B, A → target, B → target."""
    _cross_validate("obs", [
        ("obs", "A", "follow"),
        ("obs", "B", "follow"),
        ("A", "target", "follow"),
        ("B", "target", "follow"),
    ])


def test_cross_with_mutes():
    """Cross-validate: graph with mutes."""
    _cross_validate("obs", [
        ("obs", "A", "follow"),
        ("obs", "B", "follow"),
        ("A", "target", "follow"),
        ("B", "target", "mute"),
    ])


def test_cross_with_reports():
    """Cross-validate: graph with reports."""
    _cross_validate("obs", [
        ("obs", "A", "follow"),
        ("A", "target", "follow"),
        ("A", "target", "report"),
    ])


def test_cross_mixed_signals():
    """Cross-validate: follow + mute + report on same target."""
    _cross_validate("obs", [
        ("obs", "A", "follow"),
        ("obs", "B", "follow"),
        ("A", "target", "follow"),
        ("B", "target", "mute"),
        ("A", "target", "report"),
    ])


def test_cross_deeper_chain():
    """Cross-validate: 4-hop chain."""
    _cross_validate("obs", [
        ("obs", "A", "follow"),
        ("A", "B", "follow"),
        ("B", "C", "follow"),
        ("C", "D", "follow"),
    ])


def test_cross_sybils():
    """Cross-validate: sybil followers (zero-influence raters)."""
    edges = [
        ("obs", "A", "follow"),
        ("A", "target", "follow"),
    ] + [(f"sybil_{i}", "target", "follow") for i in range(20)]
    _cross_validate("obs", edges)


def test_cross_complex_graph():
    """Cross-validate: moderately complex graph with cycles and mixed edges."""
    _cross_validate("obs", [
        ("obs", "A", "follow"),
        ("obs", "B", "follow"),
        ("A", "C", "follow"),
        ("B", "C", "follow"),
        ("C", "D", "follow"),
        ("D", "A", "follow"),  # cycle
        ("A", "E", "follow"),
        ("B", "E", "mute"),
        ("C", "F", "follow"),
        ("D", "F", "report"),
    ])


# ============================================================================
# 5. Standalone module behavioral tests
# ============================================================================

def test_basic_trust_propagation():
    """Observer follows A, A follows B. B should get a nonzero score."""
    observer = "observer"
    ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "B", observer="observer"),
    ]
    scores = graperank(observer, ["A", "B"], ratings)
    assert scores["A"] > 0, "A should have a positive score"
    assert scores["B"] > 0, "B should have a positive score (trust propagated from A)"
    assert scores["A"] > scores["B"], "A should score higher than B (direct trust > transitive)"


def test_sybil_followers_do_not_boost():
    """Adding 100 sybil followers should NOT increase the target's score."""
    observer = "observer"
    baseline_ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "target", observer="observer"),
    ]
    baseline_scores = graperank(observer, ["A", "target"], baseline_ratings)

    sybils = [f"sybil_{i}" for i in range(100)]
    sybil_ratings = baseline_ratings.copy()
    for s in sybils:
        sybil_ratings.append(make_follow(s, "target", observer="observer"))

    all_pubkeys = ["A", "target"] + sybils
    sybil_scores = graperank(observer, all_pubkeys, sybil_ratings)
    assert sybil_scores["target"] <= baseline_scores["target"] + 0.001, \
        f"Sybil attack should not boost score: {sybil_scores['target']} vs baseline {baseline_scores['target']}"


def test_mutes_from_trusted_lower_score():
    """Mutes from trusted accounts should lower the target's score."""
    observer = "observer"
    baseline_ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "target", observer="observer"),
    ]
    baseline_scores = graperank(observer, ["A", "target"], baseline_ratings)

    muted_ratings = baseline_ratings + [make_mute("A", "target")]
    muted_scores = graperank(observer, ["A", "target"], muted_ratings)
    assert muted_scores["target"] < baseline_scores["target"], \
        f"Mute from trusted A should lower target: {muted_scores['target']} vs {baseline_scores['target']}"


def test_reports_from_trusted_lower_score():
    """Reports from trusted accounts should lower the target's score."""
    observer = "observer"
    baseline_ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "target", observer="observer"),
    ]
    baseline_scores = graperank(observer, ["A", "target"], baseline_ratings)

    reported_ratings = baseline_ratings + [make_report("A", "target")]
    reported_scores = graperank(observer, ["A", "target"], reported_ratings)
    assert reported_scores["target"] < baseline_scores["target"], \
        f"Report should lower score: {reported_scores['target']} vs {baseline_scores['target']}"


def test_mutes_from_untrusted_no_effect():
    """Mutes from untrusted accounts should have no effect."""
    observer = "observer"
    baseline_ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "target", observer="observer"),
    ]
    baseline_scores = graperank(observer, ["A", "target", "nobody"], baseline_ratings)

    muted_ratings = baseline_ratings + [make_mute("nobody", "target")]
    muted_scores = graperank(observer, ["A", "target", "nobody"], muted_ratings)
    assert abs(muted_scores["target"] - baseline_scores["target"]) < 0.001, \
        f"Mute from untrusted should have no effect: {muted_scores['target']} vs {baseline_scores['target']}"


def test_all_scores_between_0_and_1():
    """All scores should be in the [0, 1] range."""
    observer = "observer"
    pubkeys = ["A", "B", "C", "D", "E"]
    ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("observer", "B", observer="observer"),
        make_follow("A", "C", observer="observer"),
        make_follow("B", "D", observer="observer"),
        make_follow("C", "E", observer="observer"),
        make_follow("D", "E", observer="observer"),
        make_mute("A", "D"),
    ]
    scores = graperank(observer, pubkeys, ratings)
    for pk, score in scores.items():
        assert 0.0 <= score <= 1.0, f"Score for {pk} out of range: {score}"


def test_different_observers_different_scores():
    """Different observers should get different scores for the same target."""
    pubkeys = ["A", "B", "target"]
    ratings1 = [
        make_follow("obs1", "A", observer="obs1"),
        make_follow("A", "target", observer="obs1"),
        make_follow("B", "target", observer="obs1"),
    ]
    scores1 = graperank("obs1", pubkeys, ratings1)

    ratings2 = [
        make_follow("obs2", "B", observer="obs2"),
        make_follow("A", "target", observer="obs2"),
        make_follow("B", "target", observer="obs2"),
    ]
    scores2 = graperank("obs2", pubkeys, ratings2)
    assert scores1["A"] > scores1["B"], "obs1 should trust A more than B"
    assert scores2["B"] > scores2["A"], "obs2 should trust B more than A"


def test_convergence_under_10_iterations():
    """Algorithm should converge in under 10 iterations for a small graph."""
    observer = "observer"
    pubkeys = ["A", "B", "C", "D"]
    ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "B", observer="observer"),
        make_follow("B", "C", observer="observer"),
        make_follow("C", "D", observer="observer"),
    ]
    scores_10 = graperank(observer, pubkeys, ratings, max_iterations=10)
    scores_100 = graperank(observer, pubkeys, ratings, max_iterations=100)
    for pk in pubkeys:
        assert abs(scores_10[pk] - scores_100[pk]) < 0.0001, \
            f"Score for {pk} didn't converge in 10 iterations: {scores_10[pk]} vs {scores_100[pk]}"


def test_empty_inputs():
    """Empty inputs should return empty results."""
    assert graperank("obs", [], []) == {}
    assert graperank("obs", ["A"], []) == {}


def test_observer_direct_follow_high_score():
    """A direct follow from the observer should give a meaningful score."""
    observer = "observer"
    ratings = [make_follow("observer", "A", observer="observer")]
    scores = graperank(observer, ["A"], ratings)
    assert scores["A"] > 0.2, f"Direct follow should give high score, got {scores['A']}"


def test_chain_attenuation():
    """Scores should decrease along a trust chain."""
    observer = "observer"
    pubkeys = ["A", "B", "C", "D"]
    ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "B", observer="observer"),
        make_follow("B", "C", observer="observer"),
        make_follow("C", "D", observer="observer"),
    ]
    scores = graperank(observer, pubkeys, ratings)
    assert scores["A"] > scores["B"] > scores["C"] > scores["D"], \
        f"Scores should decrease along chain: {[scores[pk] for pk in pubkeys]}"


# ============================================================================
# 6. Production algorithm tests
# ============================================================================

def test_prod_basic_trust_propagation():
    """Production: Observer follows A, A follows B. B should get a nonzero score."""
    scores = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("A", "B", "follow"),
    ])
    assert scores["A"] > 0
    assert scores["B"] > 0
    assert scores["A"] > scores["B"]


def test_prod_sybil_resistance():
    """Production: Sybil followers should NOT boost the target's score."""
    baseline = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("A", "target", "follow"),
    ])
    sybil_edges = [
        ("observer", "A", "follow"),
        ("A", "target", "follow"),
    ] + [(f"sybil_{i}", "target", "follow") for i in range(100)]
    sybil = prod_graperank("observer", sybil_edges)
    assert sybil["target"] <= baseline["target"] + 0.001


def test_prod_mutes_lower_score():
    """Production: Mutes from trusted raters should lower score (negative rating)."""
    baseline = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("A", "target", "follow"),
    ])
    muted = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("A", "target", "follow"),
        ("A", "target", "mute"),
    ])
    assert muted["target"] < baseline["target"]


def test_prod_reports_lower_score():
    """Production: Reports from trusted raters should lower score."""
    baseline = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("A", "target", "follow"),
    ])
    reported = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("A", "target", "follow"),
        ("A", "target", "report"),
    ])
    assert reported["target"] < baseline["target"]


def test_prod_chain_attenuation():
    """Production: Scores decrease along trust chain."""
    scores = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("A", "B", "follow"),
        ("B", "C", "follow"),
        ("C", "D", "follow"),
    ])
    assert scores["A"] > scores["B"] > scores["C"] > scores["D"]


def test_prod_personalized_scores():
    """Production: Different observers produce different scores."""
    scores1 = prod_graperank("obs1", [
        ("obs1", "A", "follow"),
        ("A", "target", "follow"),
        ("B", "target", "follow"),
    ])
    scores2 = prod_graperank("obs2", [
        ("obs2", "B", "follow"),
        ("A", "target", "follow"),
        ("B", "target", "follow"),
    ])
    assert scores1["A"] > scores1["B"]
    assert scores2["B"] > scores2["A"]


def test_prod_all_scores_bounded():
    """Production: All scores in [0, 1]."""
    scores = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("observer", "B", "follow"),
        ("A", "C", "follow"),
        ("B", "D", "follow"),
        ("C", "E", "follow"),
        ("A", "D", "mute"),
        ("B", "C", "report"),
    ])
    for pk, score in scores.items():
        assert 0.0 <= score <= 1.0, f"Score for {pk} out of range: {score}"


def test_prod_direct_follow_score():
    """Production: Direct follow should give a meaningful score."""
    scores = prod_graperank("observer", [("observer", "A", "follow")])
    assert scores["A"] > 0.2, f"Direct follow should give decent score, got {scores['A']}"


def test_prod_observer_mute_zeroes():
    """Production: Observer following + muting should give low score."""
    scores = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("observer", "A", "mute"),
    ])
    assert scores["A"] < 0.3, f"Follow+mute should have low score, got {scores['A']}"


def test_prod_convergence():
    """Production: Algorithm converges for moderate graphs."""
    edges = [("observer", "A", "follow")]
    for i in range(20):
        edges.append((f"user_{i}", f"user_{i+1}", "follow"))
        edges.append(("observer", f"user_{i}", "follow"))
    scores = prod_graperank("observer", edges)
    assert len(scores) > 0


def test_prod_isolated_node():
    """Production: A node not reachable from observer gets zero score."""
    scores = prod_graperank("observer", [
        ("observer", "A", "follow"),
        ("isolated", "B", "follow"),
    ])
    assert scores["A"] > 0
    assert scores.get("B", 0) == 0 or scores["B"] < 0.001


# ============================================================================
# 7. Edge cases
# ============================================================================

def test_observer_only_mutes():
    """Observer only mutes a user (no follow). Score should be 0."""
    scores = prod_graperank("obs", [("obs", "A", "mute")])
    # mute: rating=-0.1, avg=-0.1, influence=max(-0.1*conf, 0)=0
    assert scores["A"] == 0.0

    standalone = graperank("obs", ["A"], [make_mute("obs", "A")])
    assert standalone["A"] == 0.0


def test_heavy_muting_drives_to_zero():
    """Multiple mutes should drive a target to zero even with one follow."""
    edges = [
        ("obs", "A", "follow"),
        ("obs", "B", "follow"),
        ("obs", "C", "follow"),
        ("A", "target", "follow"),
        ("obs", "target", "mute"),
        ("B", "target", "mute"),
        ("C", "target", "mute"),
    ]
    prod = prod_graperank("obs", edges)
    standalone = graperank("obs", _edges_to_pubkeys("obs", edges), _edges_to_ratings("obs", edges))
    # Target should have very low score with 3 mutes vs 1 follow from trusted A
    assert prod["target"] < 0.05, f"Heavily muted target: {prod['target']}"
    assert abs(prod["target"] - standalone["target"]) < 0.001


def test_mutual_follows():
    """A and B mutually follow each other. Both should get scores."""
    edges = [
        ("obs", "A", "follow"),
        ("A", "B", "follow"),
        ("B", "A", "follow"),
    ]
    prod = prod_graperank("obs", edges)
    assert prod["A"] > 0
    assert prod["B"] > 0
    # A should still be higher (directly followed by observer)
    assert prod["A"] > prod["B"]


# ============================================================================
# Runner
# ============================================================================

if __name__ == "__main__":
    tests = [
        # Constants
        test_constants_match_java_reference,
        test_constants_match_production,
        test_constants_match_shared_source_of_truth,
        test_confidence_formula,
        # Hand-computed numeric
        test_numeric_single_follow,
        test_numeric_two_hop_chain,
        test_numeric_observer_follow_plus_mute,
        # Cross-validation
        test_cross_simple_chain,
        test_cross_fan_out,
        test_cross_diamond,
        test_cross_with_mutes,
        test_cross_with_reports,
        test_cross_mixed_signals,
        test_cross_deeper_chain,
        test_cross_sybils,
        test_cross_complex_graph,
        # Standalone behavioral
        test_basic_trust_propagation,
        test_sybil_followers_do_not_boost,
        test_mutes_from_trusted_lower_score,
        test_reports_from_trusted_lower_score,
        test_mutes_from_untrusted_no_effect,
        test_all_scores_between_0_and_1,
        test_different_observers_different_scores,
        test_convergence_under_10_iterations,
        test_empty_inputs,
        test_observer_direct_follow_high_score,
        test_chain_attenuation,
        # Production behavioral
        test_prod_basic_trust_propagation,
        test_prod_sybil_resistance,
        test_prod_mutes_lower_score,
        test_prod_reports_lower_score,
        test_prod_chain_attenuation,
        test_prod_personalized_scores,
        test_prod_all_scores_bounded,
        test_prod_direct_follow_score,
        test_prod_observer_mute_zeroes,
        test_prod_convergence,
        test_prod_isolated_node,
        # Edge cases
        test_observer_only_mutes,
        test_heavy_muting_drives_to_zero,
        test_mutual_follows,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed+failed} tests passed")
    if failed:
        exit(1)
