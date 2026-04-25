# GrapeRank — Personalized Web of Trust for Nostr
#
# This file contains code derived from GrapeRank.
# Copyright (c) Pretty-Good-Freedom-Tech / NosFabrica
#
# Original implementation (TypeScript):
#   https://github.com/Pretty-Good-Freedom-Tech/graperank-nodejs
#
# Reference implementation:
#   https://github.com/NosFabrica/brainstorm_graperank_algorithm
#
# Algorithm design: David Strayhorn (straycat)
#   https://njump.me/npub1u5njm6g5h5cpw4wy8xugu62e5s7f6fnysv0sj0z3a8rengt2zqhsxrldq3
#
# Licensed under AGPL-3.0 — see LICENSE file in project root.
#
# Python port and nostrbtc.com relay integration by nostrbtc.

"""
GrapeRank: Personalized web-of-trust scoring for Nostr.

Core properties:
- Trust score is a CAPPED WEIGHTED AVERAGE, not a sum
- Adding fake followers drags the average down (sybil-resistant)
- Confidence scales with evidence quantity
- Personalized per observer (your follows matter most)
"""

import math
from typing import Dict, List, Optional

# INDEPENDENT constants — intentionally duplicated from
# shared/graperank_constants.py. This module is the *reference* port used by
# tests/test_graperank.py to cross-validate the production implementation
# in backend/graperank.py. If both pulled from shared/, a silent bug in the
# shared values would pass the cross-check. Keep the numbers in lockstep
# with NosFabrica/brainstorm_graperank_algorithm Constants.java — there's a
# test (test_graperank.py::test_constants_match_shared) that asserts parity.
DEFAULT_ATTENUATION = 0.85          # GLOBAL_ATTENUATION_FACTOR (applied to ALL raters incl. observer)
DEFAULT_RIGOR = 0.5                 # GLOBAL_RIGOR (confidence saturation speed)
DEFAULT_PRECISION = 0.0001          # THRESHOLD_OF_LOOP_BREAK_GIVEN_MINIMUM_DELTA_INFLUENCE
DEFAULT_MAX_ITERATIONS = 100        # Hard cap on iterations (safety guard; upstream has none)
DEFAULT_OBSERVER_CONFIDENCE = 0.5   # DEFAULT_CONFIDENCE_FOR_FOLLOW_FROM_OBSERVER
DEFAULT_FOLLOW_CONFIDENCE = 0.03    # DEFAULT_CONFIDENCE_FOR_FOLLOW
DEFAULT_MUTE_CONFIDENCE = 0.5       # DEFAULT_CONFIDENCE_FOR_MUTE
DEFAULT_REPORT_CONFIDENCE = 0.5     # DEFAULT_CONFIDENCE_FOR_REPORT
DEFAULT_FOLLOW_SCORE = 1.0          # DEFAULT_RATING_FOR_FOLLOW
DEFAULT_MUTE_SCORE = -0.1           # DEFAULT_RATING_FOR_MUTE
DEFAULT_REPORT_SCORE = -0.1         # DEFAULT_RATING_FOR_REPORT
DEFAULT_CUTOFF_VALID_USER = 0.02    # DEFAULT_CUTOFF_OF_VALID_USER


class Rating:
    """A single rating from one pubkey to another."""
    __slots__ = ('rater', 'ratee', 'score', 'confidence')

    def __init__(self, rater: str, ratee: str, score: float, confidence: float):
        self.rater = rater
        self.ratee = ratee
        self.score = score
        self.confidence = confidence


class Scorecard:
    """Accumulated score for a single ratee from the observer's perspective."""
    __slots__ = ('sum_of_weights', 'sum_of_products', 'average', 'confidence',
                 'influence', 'prev_influence')

    def __init__(self):
        self.sum_of_weights = 0.0
        self.sum_of_products = 0.0
        self.average = 0.0
        self.confidence = 0.0
        self.influence = 0.0
        self.prev_influence = 0.0

    def reset(self):
        self.sum_of_weights = 0.0
        self.sum_of_products = 0.0

    def add(self, weight: float, product: float):
        self.sum_of_weights += weight
        self.sum_of_products += product

    def calculate(self, rigor: float):
        # Weighted average (NOT clamped — upstream does not clamp average,
        # only the final influence is clamped to >= 0)
        if self.sum_of_weights > 0:
            self.average = self.sum_of_products / self.sum_of_weights
        else:
            self.average = 0.0

        # Confidence: 1 - rigor^(sum_of_weights)
        # = 1 - exp(sum_of_weights * ln(rigor))
        if self.sum_of_weights > 0 and 0 < rigor < 1:
            self.confidence = 1.0 - math.exp(self.sum_of_weights * math.log(rigor))
        else:
            self.confidence = 0.0

        # Influence = average * confidence, clamped >= 0
        self.prev_influence = self.influence
        self.influence = max(self.average * self.confidence, 0.0)


def graperank(
    observer: str,
    pubkeys: List[str],
    ratings: List[Rating],
    attenuation: float = DEFAULT_ATTENUATION,
    rigor: float = DEFAULT_RIGOR,
    precision: float = DEFAULT_PRECISION,
    max_iterations: int = DEFAULT_MAX_ITERATIONS,
) -> Dict[str, float]:
    """
    Compute personalized GrapeRank scores from the observer's perspective.

    Args:
        observer: The pubkey whose perspective we're computing from.
        pubkeys: List of pubkeys to score.
        ratings: List of Rating objects (rater, ratee, score, confidence).
        attenuation: Dampening factor for non-observer ratings (0-1).
        rigor: Controls confidence saturation speed (0-1, lower = faster).
        precision: Convergence threshold for influence delta.
        max_iterations: Hard cap on iteration count.

    Returns:
        Dict mapping pubkey -> score (average * confidence), values in [0, 1].
    """
    # Bound parameters that would otherwise silently produce degenerate output
    # (NEW-DIR-10 / NEW-DIR-11). rigor ∈ (0,1) so confidence saturation is
    # well-defined; attenuation ∈ [0,1] so per-hop dampening can't amplify.
    if not (0.0 < rigor < 1.0):
        raise ValueError(f"rigor must be in (0, 1), got {rigor}")
    if not (0.0 <= attenuation <= 1.0):
        raise ValueError(f"attenuation must be in [0, 1], got {attenuation}")

    if not pubkeys or not ratings:
        return {}

    # Collect all pubkeys that participate in ratings (raters + ratees)
    # Non-member raters need scorecards too so trust can propagate through them
    all_participants = set(pubkeys)
    for r in ratings:
        all_participants.add(r.rater)
        all_participants.add(r.ratee)

    # Build scorecards for all participants
    scorecards: Dict[str, Scorecard] = {pk: Scorecard() for pk in all_participants}

    # Initialize observer scorecard — same as Java initGrapeRankScorecards:
    # observer gets influence=1.0, confidence=1.0, averageScore=1.0
    if observer in scorecards:
        obs_sc = scorecards[observer]
        obs_sc.average = 1.0
        obs_sc.confidence = 1.0
        obs_sc.influence = 1.0

    # Index ratings by ratee for fast lookup
    ratings_by_ratee: Dict[str, List[Rating]] = {}
    for r in ratings:
        if r.ratee in all_participants:
            ratings_by_ratee.setdefault(r.ratee, []).append(r)

    # Iterate until convergence or max iterations. This mirrors the Java
    # reference's full scorecard sweep: each scorecard is recalculated
    # immediately, so later scorecards in the same round see earlier updates.
    ordered_pubkeys = sorted(scorecards.keys())
    for iteration in range(max_iterations):
        converged = True
        for ratee_pk in ordered_pubkeys:
            if ratee_pk == observer:
                continue
            sc = scorecards[ratee_pk]
            sc.reset()
            for r in ratings_by_ratee.get(ratee_pk, []):
                rater_sc = scorecards.get(r.rater)
                if rater_sc is None:
                    continue
                influence = rater_sc.influence
                if influence <= 0:
                    continue
                weight = influence * r.confidence * attenuation
                sc.add(weight, weight * r.score)
            sc.calculate(rigor)
            if abs(sc.influence - sc.prev_influence) > precision:
                converged = False

        if converged:
            break

    # Return scores only for the requested pubkeys (not intermediate raters)
    return {pk: scorecards[pk].influence for pk in pubkeys if pk in scorecards}
