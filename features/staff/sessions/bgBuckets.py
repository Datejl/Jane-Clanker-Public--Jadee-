from __future__ import annotations

from typing import Iterable


adultBgReviewBucket = "adult"
minorBgReviewBucket = "minor"


def normalizeBgReviewBucket(value: object, *, default: str = adultBgReviewBucket) -> str:
    rawValue = str(value or "").strip().lower()
    if rawValue in {
        adultBgReviewBucket,
        "+18",
        "18+",
        "adult",
        "adults",
        "over18",
        "over-18",
        "plus18",
        "plus-18",
    }:
        return adultBgReviewBucket
    if rawValue in {
        minorBgReviewBucket,
        "-18",
        "18-",
        "minor",
        "minors",
        "under18",
        "under-18",
        "minus18",
        "minus-18",
    }:
        return minorBgReviewBucket
    return default


def bgReviewBucketLabel(value: object) -> str:
    bucket = normalizeBgReviewBucket(value)
    if bucket == minorBgReviewBucket:
        return "-18"
    return "+18"


def bgReviewBucketTitle(value: object) -> str:
    bucket = normalizeBgReviewBucket(value)
    if bucket == minorBgReviewBucket:
        return "Under 18"
    return "18 and Older"


def isMinorAgeGroup(ageGroup: object, minorAgeGroups: Iterable[object]) -> bool:
    normalizedAgeGroup = str(ageGroup or "").strip().upper()
    if not normalizedAgeGroup:
        return False
    normalizedMinorGroups = {str(value or "").strip().upper() for value in list(minorAgeGroups or []) if str(value or "").strip()}
    return normalizedAgeGroup in normalizedMinorGroups

