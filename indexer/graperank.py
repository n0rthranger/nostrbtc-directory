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
# Python port and relay integration.

"""
GrapeRank: Personalized web-of-trust scoring for Nostr.

Core properties:
- Trust score is a CAPPED WEIGHTED AVERAGE, not a sum
- Adding fake followers drags the average down (sybil-resistant)
- Confidence scales with evidence quantity
- Personalized per observer (your follows matter most)
"""

import math
from typing import Dict, List, Optional, Tuple

# Default parameters
DEFAULT_ATTENUATION = 0.7       # Dampening for non-observer raters
DEFAULT_RIGOR = 0.2             # How fast confidence saturates (lower = faster)
DEFAULT_PRECISION = 0.00001     # Convergence threshold
DEFAULT_MAX_ITERATIONS = 100    # Hard cap on iterations
DEFAULT_OBSERVER_CONFIDENCE = 0.5   # Confidence for observer's own ratings
DEFAULT_FOLLOW_CONFIDENCE = 0.03    # Confidence for others' follows
DEFAULT_MUTE_CONFIDENCE = 0.5       # Confidence for mutes
DEFAULT_REPORT_CONFIDENCE = 0.5     # Confidence for reports
DEFAULT_FOLLOW_SCORE = 1.0     # Rating value for a follow
DEFAULT_MUTE_SCORE = 0.0       # Rating value for a mute
DEFAULT_REPORT_SCORE = 0.0     # Rating value for a report


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
        # Weighted average, clamped to >= 0
        if self.sum_of_weights > 0:
            self.average = max(self.sum_of_products / self.sum_of_weights, 0.0)
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
    if not pubkeys or not ratings:
        return {}

    pubkey_set = set(pubkeys)

    # Collect all pubkeys that participate in ratings (raters + ratees)
    # Non-member raters need scorecards too so trust can propagate through them
    all_participants = set(pubkeys)
    for r in ratings:
        all_participants.add(r.rater)
        all_participants.add(r.ratee)

    # Build scorecards for all participants
    scorecards: Dict[str, Scorecard] = {pk: Scorecard() for pk in all_participants}

    # Index ratings by ratee for fast lookup
    ratings_by_ratee: Dict[str, List[Rating]] = {}
    for r in ratings:
        if r.ratee in all_participants:
            ratings_by_ratee.setdefault(r.ratee, []).append(r)

    # Iterate until convergence or max iterations
    for iteration in range(max_iterations):
        # Reset all sums
        for sc in scorecards.values():
            sc.reset()

        # Accumulate weighted ratings
        for ratee_pk, ratee_ratings in ratings_by_ratee.items():
            sc = scorecards[ratee_pk]
            for r in ratee_ratings:
                if r.rater == observer:
                    # Observer's own ratings: full influence, no attenuation
                    influence = 1.0
                    weight = influence * r.confidence
                else:
                    # Other raters: use their current influence score
                    rater_sc = scorecards.get(r.rater)
                    if rater_sc is None:
                        continue
                    influence = rater_sc.influence
                    if influence <= 0:
                        continue
                    weight = influence * r.confidence * attenuation

                sc.add(weight, weight * r.score)

        # Calculate new scores
        converged = True
        for sc in scorecards.values():
            sc.calculate(rigor)
            if abs(sc.influence - sc.prev_influence) > precision:
                converged = False

        if converged and iteration > 0:
            break

    # Return scores only for the requested pubkeys (not intermediate raters)
    return {pk: scorecards[pk].influence for pk in pubkeys if pk in scorecards}
