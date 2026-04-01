from __future__ import annotations

import json
import logging
from typing import Any, Optional

from features.staff.applications.questionEditor import (
    _normalizeQuestionStyle,
    _questionTypeKeyToken,
    _shouldNumberQuestionType,
)

log = logging.getLogger(__name__)


def loadDivisionsPayload(configPath: Optional[str]) -> Optional[dict[str, Any]]:
    if not configPath:
        return None
    try:
        with open(configPath, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def saveDivisionsPayload(configPath: Optional[str], payload: dict[str, Any]) -> bool:
    if not configPath:
        return False
    try:
        with open(configPath, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=True)
            handle.write("\n")
        return True
    except Exception:
        log.exception("Failed to save divisions config: %s", configPath)
        return False


def findRawDivision(payload: dict[str, Any], divisionKey: str) -> Optional[dict[str, Any]]:
    divisionsRaw = payload.get("divisions")
    if not isinstance(divisionsRaw, list):
        return None
    normalizedKey = str(divisionKey or "").strip().lower()
    for rawDivision in divisionsRaw:
        if not isinstance(rawDivision, dict):
            continue
        key = str(rawDivision.get("key") or "").strip().lower()
        if key == normalizedKey:
            return rawDivision
    return None


def assignDivisionQuestionKeys(
    divisionKey: str,
    questions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalizedDivisionKey = str(divisionKey or "").strip().lower()
    counters: dict[str, int] = {}
    output: list[dict[str, Any]] = []
    for question in questions:
        if not isinstance(question, dict):
            continue
        style = _normalizeQuestionStyle(question.get("style"))
        token = _questionTypeKeyToken(style)
        if _shouldNumberQuestionType(style):
            count = counters.get(token, 0) + 1
            counters[token] = count
            key = f"{normalizedDivisionKey}{token}{count}"
        else:
            key = f"{normalizedDivisionKey}{token}"
        rewritten = dict(question)
        rewritten["key"] = key
        output.append(rewritten)
    return output


def sanitizeRawQuestions(divisionKey: str, rawQuestions: Any) -> list[dict[str, Any]]:
    if not isinstance(rawQuestions, list):
        return []
    sanitized: list[dict[str, Any]] = []
    for index, rawQuestion in enumerate(rawQuestions):
        if not isinstance(rawQuestion, dict):
            continue
        style = _normalizeQuestionStyle(rawQuestion.get("style"))
        label = str(rawQuestion.get("label") or f"Question {index + 1}").strip() or f"Question {index + 1}"
        key = str(rawQuestion.get("key") or label).strip().lower()
        if style in {"short", "paragraph"}:
            try:
                maxLength = int(rawQuestion.get("maxLength", 400))
            except (TypeError, ValueError):
                maxLength = 400
            sanitized.append(
                {
                    "key": key,
                    "label": label,
                    "required": bool(rawQuestion.get("required", True)),
                    "style": style,
                    "maxLength": max(1, min(maxLength, 4000)),
                    "placeholder": str(rawQuestion.get("placeholder") or "").strip(),
                }
            )
            continue
        if style == "multiple-choice":
            rawChoices = rawQuestion.get("choices")
            choices: list[str] = []
            if isinstance(rawChoices, list):
                for rawChoice in rawChoices:
                    cleanChoice = str(rawChoice or "").strip()
                    if cleanChoice and cleanChoice not in choices:
                        choices.append(cleanChoice)
            sanitized.append(
                {
                    "key": key,
                    "label": label,
                    "required": bool(rawQuestion.get("required", True)),
                    "style": "multiple-choice",
                    "choices": choices[:25],
                }
            )
            continue
        if style == "form":
            link = str(rawQuestion.get("link") or rawQuestion.get("url") or "").strip()
            sanitized.append(
                {
                    "key": key,
                    "label": label,
                    "required": True,
                    "style": "form",
                    "link": link,
                }
            )
            continue
        inviteUrl = str(rawQuestion.get("inviteUrl") or rawQuestion.get("url") or "").strip()
        sanitized.append(
            {
                "key": key,
                "label": label,
                "required": True,
                "style": "server-invite",
                "inviteUrl": inviteUrl,
            }
        )
    return assignDivisionQuestionKeys(divisionKey, sanitized)


__all__ = [
    "assignDivisionQuestionKeys",
    "findRawDivision",
    "loadDivisionsPayload",
    "sanitizeRawQuestions",
    "saveDivisionsPayload",
]

