# GrapeRank unit tests
#
# Licensed under AGPL-3.0 — see LICENSE file in project root.

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'indexer'))

from graperank import (
    DEFAULT_ATTENUATION,
    DEFAULT_OBSERVER_CONFIDENCE,
    DEFAULT_RIGOR,
    graperank,
    Rating,
)


def make_follow(rater, ratee, confidence=0.03, observer_confidence=0.5, observer=None):
    """Create a follow rating. Uses observer_confidence if rater is the observer."""
    c = observer_confidence if rater == observer else confidence
    return Rating(rater, ratee, 1.0, c)


def make_mute(rater, ratee):
    return Rating(rater, ratee, 0.0, 0.5)


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

    # Baseline: observer follows A, A follows target
    baseline_ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "target", observer="observer"),
    ]
    baseline_scores = graperank(observer, ["A", "target"], baseline_ratings)

    # Now add 100 sybil accounts that also follow target
    sybils = [f"sybil_{i}" for i in range(100)]
    sybil_ratings = baseline_ratings.copy()
    for s in sybils:
        sybil_ratings.append(make_follow(s, "target", observer="observer"))

    all_pubkeys = ["A", "target"] + sybils
    sybil_scores = graperank(observer, all_pubkeys, sybil_ratings)

    # Sybil followers should NOT boost the score
    assert sybil_scores["target"] <= baseline_scores["target"] + 0.001, \
        f"Sybil attack should not boost score: {sybil_scores['target']} vs baseline {baseline_scores['target']}"


def test_sybil_followers_dilute():
    """Sybil followers should dilute (not increase) the target's score when mixed."""
    observer = "observer"

    # Observer follows A, A follows target, one trusted friend also follows target
    baseline_ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("observer", "friend", observer="observer"),
        make_follow("A", "target", observer="observer"),
        make_follow("friend", "target", observer="observer"),
    ]
    baseline_scores = graperank(observer, ["A", "friend", "target"], baseline_ratings)

    # Add sybils that follow target with low scores (nobody trusts them)
    sybils = [f"sybil_{i}" for i in range(50)]
    diluted_ratings = baseline_ratings.copy()
    for s in sybils:
        diluted_ratings.append(make_follow(s, "target", observer="observer"))

    all_pubkeys = ["A", "friend", "target"] + sybils
    diluted_scores = graperank(observer, all_pubkeys, diluted_ratings)

    # Sybil followers have zero influence, so their ratings contribute nothing
    # Score should remain the same (not increase)
    assert diluted_scores["target"] <= baseline_scores["target"] + 0.001, \
        f"Sybils should not boost: {diluted_scores['target']} vs {baseline_scores['target']}"


def test_mutes_from_trusted_lower_score():
    """Mutes from trusted accounts should lower the target's score."""
    observer = "observer"

    # Baseline: observer follows A, A follows target
    baseline_ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "target", observer="observer"),
    ]
    baseline_scores = graperank(observer, ["A", "target"], baseline_ratings)

    # Now A also mutes target (conflicting signals)
    muted_ratings = baseline_ratings + [make_mute("A", "target")]
    muted_scores = graperank(observer, ["A", "target"], muted_ratings)

    assert muted_scores["target"] < baseline_scores["target"], \
        f"Mute from trusted A should lower target: {muted_scores['target']} vs {baseline_scores['target']}"


def test_mutes_from_untrusted_no_effect():
    """Mutes from untrusted accounts should have no effect."""
    observer = "observer"

    baseline_ratings = [
        make_follow("observer", "A", observer="observer"),
        make_follow("A", "target", observer="observer"),
    ]
    baseline_scores = graperank(observer, ["A", "target", "nobody"], baseline_ratings)

    # Nobody (untrusted) mutes target
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
    # Observer1 follows A, Observer2 follows B. Both A and B follow target.
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

    # obs1 trusts A directly, so A's endorsement of target matters more
    # obs2 trusts B directly, so B's endorsement of target matters more
    # Both score target, but through different trust paths
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

    # Run with max_iterations=10 and very tight precision
    scores_10 = graperank(observer, pubkeys, ratings, max_iterations=10)

    # Run with max_iterations=100 for reference
    scores_100 = graperank(observer, pubkeys, ratings, max_iterations=100)

    # Should be essentially identical — convergence happened well before 10
    for pk in pubkeys:
        assert abs(scores_10[pk] - scores_100[pk]) < 0.0001, \
            f"Score for {pk} didn't converge in 10 iterations: {scores_10[pk]} vs {scores_100[pk]}"


def test_empty_inputs():
    """Empty inputs should return empty results."""
    assert graperank("obs", [], []) == {}
    assert graperank("obs", ["A"], []) == {}


def test_observer_direct_follow_high_score():
    """A direct follow from the observer should give a high score."""
    observer = "observer"
    ratings = [make_follow("observer", "A", observer="observer")]
    scores = graperank(observer, ["A"], ratings)
    expected = 1 - (DEFAULT_RIGOR ** (DEFAULT_OBSERVER_CONFIDENCE * DEFAULT_ATTENUATION))
    assert abs(scores["A"] - expected) < 0.001, \
        f"Direct follow should match current default constants, got {scores['A']} vs {expected}"


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


if __name__ == "__main__":
    tests = [
        test_basic_trust_propagation,
        test_sybil_followers_do_not_boost,
        test_sybil_followers_dilute,
        test_mutes_from_trusted_lower_score,
        test_mutes_from_untrusted_no_effect,
        test_all_scores_between_0_and_1,
        test_different_observers_different_scores,
        test_convergence_under_10_iterations,
        test_empty_inputs,
        test_observer_direct_follow_high_score,
        test_chain_attenuation,
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
