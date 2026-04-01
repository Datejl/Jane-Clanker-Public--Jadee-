from __future__ import annotations

import random
import re

_separatorSpacePattern = re.compile(r"\s*([|,.:;-])\s*")
_extraSpacePattern = re.compile(r"\s+")


def _normalizeText(value: str) -> str:
    text = str(value or "").strip().lower()
    text = _separatorSpacePattern.sub(r"\1", text)
    text = _extraSpacePattern.sub(" ", text)
    return text


def _taskNumberChain() -> dict:
    start = random.randint(4, 22)
    end = start + random.randint(18, 32)
    answer = ",".join(str(value) for value in range(start, end + 1))
    prompt = (
        "Number Chain:\n"
        f"Type every number from **{start}** to **{end}** in order.\n"
        "Use commas between numbers, no spaces, no missing values.\n"
        "Example format: 4,5,6,7"
    )
    return {
        "type": "numberChain",
        "title": "Number Chain",
        "prompt": prompt,
        "answer": answer,
    }


def _taskPhraseChant() -> dict:
    phrase = random.choice(
        (
            "house edge is permanent",
            "i accept the odds",
            "dealer never misses",
            "the wheel is not my friend",
            "this is definitely fair",
        )
    )
    repeatCount = random.randint(6, 10)
    answer = "|".join(phrase for _ in range(repeatCount))
    prompt = (
        "Phrase Chant:\n"
        f"Repeat this phrase **{repeatCount}** times with `|` between each copy:\n"
        f"`{phrase}`\n"
        "No extra text."
    )
    return {
        "type": "phraseChant",
        "title": "Phrase Chant",
        "prompt": prompt,
        "answer": answer,
    }


def _taskChunkLoop() -> dict:
    chunks = [
        "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(3))
        + str(random.randint(10, 99))
        for _ in range(6)
    ]
    repeatCount = random.randint(4, 7)
    chunkLine = ",".join(chunks)
    answer = ";".join(chunkLine for _ in range(repeatCount))
    prompt = (
        "Chunk Loop:\n"
        f"Base chunk list: `{chunkLine}`\n"
        f"Repeat that exact chunk list **{repeatCount}** times with `;` between each repetition.\n"
        "Keep commas inside each chunk list."
    )
    return {
        "type": "chunkLoop",
        "title": "Chunk Loop",
        "prompt": prompt,
        "answer": answer,
    }


def _taskRollCall() -> dict:
    handles = random.sample(
        (
            "alpha",
            "bravo",
            "charlie",
            "delta",
            "echo",
            "foxtrot",
            "golf",
            "hotel",
            "india",
            "juliet",
            "kilo",
            "lima",
            "mike",
            "november",
            "oscar",
            "papa",
            "quebec",
            "romeo",
            "sierra",
            "tango",
            "uniform",
            "victor",
            "whiskey",
            "xray",
            "yankee",
            "zulu",
        ),
        12,
    )
    answer = ",".join(handles)
    prompt = (
        "Roll Call:\n"
        "Copy these in the exact same order, separated by commas:\n"
        f"{' '.join(handles)}\n"
        "Do not add spaces after commas."
    )
    return {
        "type": "rollCall",
        "title": "Roll Call",
        "prompt": prompt,
        "answer": answer,
    }


def _taskLedgerCopy() -> dict:
    entries = [
        "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(2))
        + str(random.randint(100, 999))
        + "-"
        + str(random.randint(10, 99))
        for _ in range(16)
    ]
    answer = "|".join(entries)
    prompt = (
        "Ledger Copy:\n"
        "Copy this ledger exactly using `|` between entries:\n"
        f"`{answer}`\n"
        "Do not add spaces."
    )
    return {
        "type": "ledgerCopy",
        "title": "Ledger Copy",
        "prompt": prompt,
        "answer": answer,
    }


def _taskDoubleEntry() -> dict:
    words = random.sample(
        (
            "steel",
            "copper",
            "wiring",
            "coil",
            "plate",
            "panel",
            "valve",
            "switch",
            "frame",
            "motor",
            "piston",
            "filter",
            "module",
            "sensor",
            "bolt",
            "hinge",
            "drill",
            "casing",
        ),
        10,
    )
    pairs = [f"{word},{word}" for word in words]
    answer = ";".join(pairs)
    prompt = (
        "Double Entry:\n"
        "For each word below, write it twice as `word,word`.\n"
        "Join each pair with `;` in the same order:\n"
        f"{' '.join(words)}"
    )
    return {
        "type": "doubleEntry",
        "title": "Double Entry",
        "prompt": prompt,
        "answer": answer,
    }


def _taskStampStrip() -> dict:
    prefixes = [
        "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(3))
        for _ in range(12)
    ]
    numbers = [str(random.randint(1000, 9999)) for _ in range(12)]
    chunks = [f"{prefixes[index]}:{numbers[index]}" for index in range(12)]
    answer = ">".join(chunks)
    prompt = (
        "Stamp Strip:\n"
        "Copy the strip exactly with `>` separators:\n"
        f"`{answer}`\n"
        "No spaces, no changes."
    )
    return {
        "type": "stampStrip",
        "title": "Stamp Strip",
        "prompt": prompt,
        "answer": answer,
    }


def _taskShiftTimeline() -> dict:
    startHour = random.randint(5, 10)
    startMinute = random.choice((0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55))
    stepMinutes = random.choice((5, 10, 15))
    count = random.randint(14, 22)

    stamps: list[str] = []
    minuteTotal = (startHour * 60) + startMinute
    for _ in range(count):
        hour = (minuteTotal // 60) % 24
        minute = minuteTotal % 60
        stamps.append(f"{hour:02d}:{minute:02d}")
        minuteTotal += stepMinutes

    answer = ",".join(stamps)
    prompt = (
        "Shift Timeline:\n"
        "Rewrite all timestamps in the same order with commas between each value:\n"
        f"{' '.join(stamps)}\n"
        "No extra spaces."
    )
    return {
        "type": "shiftTimeline",
        "title": "Shift Timeline",
        "prompt": prompt,
        "answer": answer,
    }


def _taskBatchReplay() -> dict:
    segments = [
        "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(6))
        for _ in range(7)
    ]
    base = ",".join(segments)
    loops = random.randint(5, 8)
    answer = "/".join(base for _ in range(loops))
    prompt = (
        "Batch Replay:\n"
        f"Base sequence: `{base}`\n"
        f"Repeat the base sequence **{loops}** times with `/` between each repetition.\n"
        "Keep commas inside each base sequence."
    )
    return {
        "type": "batchReplay",
        "title": "Batch Replay",
        "prompt": prompt,
        "answer": answer,
    }


def _taskGridReport() -> dict:
    rows = []
    for _ in range(9):
        row = "".join(random.choice("0123456789") for _ in range(5))
        rows.append(row)
    answer = "|".join(rows)
    prompt = (
        "Grid Report:\n"
        "Copy each 5-digit row in order using `|` between rows:\n"
        f"{' '.join(rows)}\n"
        "Do not reorder anything."
    )
    return {
        "type": "gridReport",
        "title": "Grid Report",
        "prompt": prompt,
        "answer": answer,
    }


def createTask() -> dict:
    taskFactory = random.choice(
        (
            _taskNumberChain,
            _taskPhraseChant,
            _taskChunkLoop,
            _taskRollCall,
            _taskLedgerCopy,
            _taskDoubleEntry,
            _taskStampStrip,
            _taskShiftTimeline,
            _taskBatchReplay,
            _taskGridReport,
        )
    )
    return taskFactory()


def titleText(task: dict) -> str:
    title = str(task.get("title") or "").strip()
    if title:
        return title
    return "Shift Task"


def promptText(task: dict) -> str:
    return str(task.get("prompt") or "Complete the task")


def validateAnswer(task: dict, userAnswer: str) -> bool:
    expected = _normalizeText(str(task.get("answer") or ""))
    actual = _normalizeText(str(userAnswer or ""))
    return bool(expected) and expected == actual


def expectedAnswer(task: dict) -> str:
    return str(task.get("answer") or "").strip()
