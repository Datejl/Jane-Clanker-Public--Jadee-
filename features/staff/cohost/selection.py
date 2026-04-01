from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Optional

import pandas as pd

EVENT_LABELS = {
    "solo": "Solo",
    "turbine": "Turbine",
    "emergency": "Emergency",
    "grid": "Grid",
    "shift": "Shift",
}

# Each event stores two fields: total count and last cohost date.
COUNT_COLUMNS = {key: f"{label}Count" for key, label in EVENT_LABELS.items()}
LAST_COLUMNS = {key: f"{label}Last" for key, label in EVENT_LABELS.items()}

LOG_COLUMNS = (
    ["UserID", "Rank"]
    + [column for key in EVENT_LABELS for column in (COUNT_COLUMNS[key], LAST_COLUMNS[key])]
    + ["Cohosts", "timeSinceFirstAttempt"]
)

GROUP_LABELS = {
    "group1": "newbie",
    "group2": "missing_event",
    "group3": "repeat",
}

@dataclass(frozen=True)
class SelectionResult:
    userId: str
    rank: str
    pool: str
    group: str
    totalCohosts: int
    eventCount: int
    eventTypesCompleted: int
    lastEventDate: Optional[datetime]
    lastAnyDate: Optional[datetime]
    firstAttemptDate: Optional[datetime]


def _eventColumns(event: str) -> tuple[str, str, str]:
    # Normalize event input and return the count/last columns tied to it.
    key = event.strip().lower()
    if key not in EVENT_LABELS:
        raise ValueError(f"Unknown event '{event}'. Use one of: {', '.join(EVENT_LABELS)}")
    return COUNT_COLUMNS[key], LAST_COLUMNS[key], EVENT_LABELS[key]


def _formatDate(value: Optional[datetime]) -> str:
    # Keep CSV dates in a compact M/D/YYYY format.
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    return f"{value.month}/{value.day}/{value.year}"


def _parseDates(df: pd.DataFrame) -> pd.DataFrame:
    # Convert last-cohost and first-attempt fields back into datetimes.
    for column in list(LAST_COLUMNS.values()) + ["timeSinceFirstAttempt"]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def _migrateLegacyColumns(df: pd.DataFrame) -> pd.DataFrame:
    # Support the old schema where event columns stored a date directly.
    originalColumns = set(df.columns)

    for key, eventLabel in EVENT_LABELS.items():
        countColumn = COUNT_COLUMNS[key]
        lastColumn = LAST_COLUMNS[key]

        needCount = countColumn not in originalColumns
        needLast = lastColumn not in originalColumns

        if needCount:
            df[countColumn] = 0
        if needLast:
            df[lastColumn] = pd.NaT

        if eventLabel in df.columns and (needCount or needLast):
            parsedDate = pd.to_datetime(df[eventLabel], errors="coerce")
            parsedCount = pd.to_numeric(df[eventLabel], errors="coerce")
            hasNumeric = parsedCount.notna()
            hasDate = parsedDate.notna()

            if needCount:
                # If the legacy column looks numeric, treat it as a count.
                df.loc[hasNumeric, countColumn] = parsedCount[hasNumeric]
                # If it's a date, treat it as a single completion.
                df.loc[hasDate & ~hasNumeric, countColumn] = 1
            if needLast:
                # Preserve the last cohost date when present.
                df.loc[hasDate, lastColumn] = parsedDate[hasDate]

    return df


def loadLogs(logPath: str | Path) -> pd.DataFrame:
    # Load logs and normalize missing columns to the latest schema.
    path = Path(logPath)
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=LOG_COLUMNS)
    df = pd.read_csv(path, dtype={"UserID": "string", "Rank": "string"})
    df = _migrateLegacyColumns(df)

    for column in LOG_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    df = df[LOG_COLUMNS].copy()
    df["UserID"] = df["UserID"].astype("string")
    df["Rank"] = df["Rank"].astype("string")

    for column in COUNT_COLUMNS.values():
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)

    # If total cohosts is missing, recompute from event counts.
    df["Cohosts"] = pd.to_numeric(df["Cohosts"], errors="coerce")
    countsSum = df[list(COUNT_COLUMNS.values())].sum(axis=1)
    df.loc[df["Cohosts"].isna(), "Cohosts"] = countsSum[df["Cohosts"].isna()]
    df["Cohosts"] = df["Cohosts"].fillna(0).astype(int)

    return _parseDates(df)


def saveLogs(df: pd.DataFrame, logPath: str | Path) -> None:
    # Persist logs back to CSV with human-readable dates.
    output = df.copy()
    for column in list(LAST_COLUMNS.values()) + ["timeSinceFirstAttempt"]:
        if column in output.columns:
            output[column] = output[column].apply(_formatDate)
    output.to_csv(logPath, index=False)


def ensureMember(
    df: pd.DataFrame,
    userId: str,
    rank: Optional[str] = None,
    firstAttempt: Optional[datetime] = None,
) -> pd.DataFrame:
    # Add a user row if missing and set timeSinceFirstAttempt once.
    userId = str(userId)
    if userId in df["UserID"].astype(str).tolist():
        if rank is not None:
            df.loc[df["UserID"] == userId, "Rank"] = rank
        if firstAttempt is not None:
            mask = df["UserID"] == userId
            if df.loc[mask, "timeSinceFirstAttempt"].isna().any():
                df.loc[mask, "timeSinceFirstAttempt"] = firstAttempt
        return df

    newRow = {column: pd.NA for column in LOG_COLUMNS}
    for column in COUNT_COLUMNS.values():
        newRow[column] = 0
    for column in LAST_COLUMNS.values():
        newRow[column] = pd.NaT
    newRow["UserID"] = userId
    newRow["Rank"] = rank if rank is not None else ""
    newRow["Cohosts"] = 0
    newRow["timeSinceFirstAttempt"] = firstAttempt
    return pd.concat([df, pd.DataFrame([newRow])], ignore_index=True)


def recordCohosts(
    logPath: str | Path,
    userIds: Iterable[str],
    event: str,
    date: Optional[datetime] = None,
    ranks: Optional[Mapping[str, str]] = None,
) -> None:
    # Record multiple winners at once so counts and dates stay consistent.
    df = loadLogs(logPath)
    countColumn, lastColumn, _ = _eventColumns(event)
    date = date or datetime.now()
    rankMap = {str(userId): rank for userId, rank in ranks.items()} if ranks else {}
    uniqueUserIds = list(dict.fromkeys(str(userId) for userId in userIds))

    for userId in uniqueUserIds:
        # Ensure they exist, then increment the event count + last date.
        df = ensureMember(df, userId=userId, rank=rankMap.get(userId), firstAttempt=date)
        mask = df["UserID"] == str(userId)
        df.loc[mask, countColumn] = df.loc[mask, countColumn].fillna(0).astype(int) + 1
        df.loc[mask, lastColumn] = date
        if df.loc[mask, "timeSinceFirstAttempt"].isna().any():
            df.loc[mask, "timeSinceFirstAttempt"] = date

    # Recompute total cohosts after updates.
    df["Cohosts"] = df[list(COUNT_COLUMNS.values())].sum(axis=1).astype(int)
    saveLogs(df, logPath)


def recordCohost(
    logPath: str | Path,
    userId: str,
    event: str,
    date: Optional[datetime] = None,
    rank: Optional[str] = None,
) -> None:
    recordCohosts(
        logPath,
        [userId],
        event,
        date=date,
        ranks={str(userId): rank} if rank is not None else None,
    )


def _buildPool(
    df: pd.DataFrame,
    poolUserIds: Iterable[str],
    poolRanks: Optional[Mapping[str, str]] = None,
    firstAttemptDate: Optional[datetime] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Normalize the candidate pool and set firstAttempt for newcomers.
    poolUserIds = [str(uid) for uid in poolUserIds]
    rankMap = {str(userId): rank for userId, rank in poolRanks.items()} if poolRanks else {}
    firstAttemptDate = firstAttemptDate or datetime.now()

    for userId in poolUserIds:
        df = ensureMember(
            df,
            userId=userId,
            rank=rankMap.get(userId),
            firstAttempt=firstAttemptDate,
        )

    poolDf = df[df["UserID"].isin(poolUserIds)].copy()
    return df, poolDf


def buildQueue(
    logPath: str | Path,
    event: str,
    poolUserIds: Iterable[str],
    poolRanks: Optional[Mapping[str, str]] = None,
    asOf: Optional[datetime] = None,
    saveFirstAttempts: bool = True,
) -> pd.DataFrame:
    # Build a ranked queue for the requested event from the reaction pool.
    df = loadLogs(logPath)
    countColumn, lastColumn, _ = _eventColumns(event)
    asOf = pd.Timestamp(asOf or datetime.now())
    df, poolDf = _buildPool(
        df,
        poolUserIds,
        poolRanks,
        firstAttemptDate=asOf.to_pydatetime(),
    )
    if saveFirstAttempts:
        saveLogs(df, logPath)

    # Per-event stats used for grouping and tie-breaking.
    poolDf["_eventCount"] = poolDf[countColumn].fillna(0).astype(int)
    poolDf["_eventDate"] = poolDf[lastColumn]
    poolDf["_firstAttempt"] = poolDf["timeSinceFirstAttempt"]
    poolDf["_totalCohosts"] = poolDf["Cohosts"].fillna(0).astype(int)
    poolDf["_eventTypesCompleted"] = (
        poolDf[list(COUNT_COLUMNS.values())].fillna(0).astype(int).gt(0).sum(axis=1)
    )
    poolDf["_lastAny"] = poolDf[list(LAST_COLUMNS.values())].max(axis=1)
    # Wait priority: last date for this event, otherwise first attempt.
    poolDf["_waitDate"] = poolDf["_eventDate"]
    poolDf.loc[poolDf["_eventCount"] == 0, "_waitDate"] = poolDf["_firstAttempt"]
    poolDf["_waitDate"] = poolDf["_waitDate"].fillna(asOf)
    # Group by need: newbie -> missing event type -> repeat host.
    poolDf["_group"] = "group3"
    poolDf.loc[poolDf["_totalCohosts"] == 0, "_group"] = "group1"
    poolDf.loc[
        (poolDf["_totalCohosts"] > 0) & (poolDf["_eventCount"] == 0), "_group"
    ] = "group2"

    # Oldest wait first, then fewer cohosts, then fewer event types completed.
    return poolDf.sort_values(
        by=["_waitDate", "_totalCohosts", "_eventTypesCompleted", "UserID"],
        ascending=True,
    )


def _pickFromGroup(queueDf: pd.DataFrame, groupOrder: Iterable[str]) -> Optional[pd.Series]:
    # Select the top-ranked candidate from the first non-empty group.
    for group in groupOrder:
        groupDf = queueDf[queueDf["_group"] == group]
        if not groupDf.empty:
            return groupDf.iloc[0]
    return None


def selectCohosts(
    logPath: str | Path,
    event: str,
    poolUserIds: Iterable[str],
    poolRanks: Optional[Mapping[str, str]] = None,
    asOf: Optional[datetime] = None,
    saveFirstAttempts: bool = True,
    preferSro: bool = True,
    slots: int = 2,
    groupOrder: Optional[Iterable[str]] = None,
) -> list[SelectionResult]:
    # Select N winners using group ordering and optional SRO priority.
    queueDf = buildQueue(
        logPath,
        event,
        poolUserIds,
        poolRanks,
        asOf=asOf,
        saveFirstAttempts=saveFirstAttempts,
    )
    if queueDf.empty:
        return []

    queueDf["Rank"] = queueDf["Rank"].fillna("").astype(str)

    groupOrder = list(groupOrder) if groupOrder is not None else ["group1", "group2", "group3"]

    picks: list[pd.Series] = []
    if preferSro:
        # First try to fill slots from SROs, then fall back to backup pool.
        sroDf = queueDf[queueDf["Rank"].str.upper() == "SRO"].copy()
        backupDf = queueDf[queueDf["Rank"].str.upper() != "SRO"].copy()

        for _ in range(max(0, slots)):
            chosen = _pickFromGroup(sroDf, groupOrder)
            if chosen is None:
                chosen = _pickFromGroup(backupDf, groupOrder)
            if chosen is None:
                continue
            picks.append(chosen)
            userId = chosen["UserID"]
            sroDf = sroDf[sroDf["UserID"] != userId]
            backupDf = backupDf[backupDf["UserID"] != userId]
    else:
        remaining = queueDf
        for _ in range(max(0, slots)):
            chosen = _pickFromGroup(remaining, groupOrder)
            if chosen is None:
                continue
            picks.append(chosen)
            remaining = remaining[remaining["UserID"] != chosen["UserID"]]

    results: list[SelectionResult] = []
    for pick in picks:
        rank = str(pick["Rank"])
        poolLabel = "SRO" if rank.upper() == "SRO" else "backup"
        groupLabel = GROUP_LABELS.get(str(pick["_group"]), str(pick["_group"]))
        results.append(
            SelectionResult(
                userId=str(pick["UserID"]),
                rank=rank,
                pool=poolLabel,
                group=groupLabel,
                totalCohosts=int(pick["_totalCohosts"]),
                eventCount=int(pick["_eventCount"]),
                eventTypesCompleted=int(pick["_eventTypesCompleted"]),
                lastEventDate=pick.get("_eventDate"),
                lastAnyDate=pick.get("_lastAny"),
                firstAttemptDate=pick.get("_firstAttempt"),
            )
        )
    return results


def selectTwoCohosts(
    logPath: str | Path,
    event: str,
    poolUserIds: Iterable[str],
    poolRanks: Optional[Mapping[str, str]] = None,
    asOf: Optional[datetime] = None,
    saveFirstAttempts: bool = True,
    preferSro: bool = True,
) -> list[SelectionResult]:
    return selectCohosts(
        logPath,
        event,
        poolUserIds,
        poolRanks,
        asOf=asOf,
        saveFirstAttempts=saveFirstAttempts,
        preferSro=preferSro,
        slots=2,
    )


def selectNextCohost(
    logPath: str | Path,
    event: str,
    poolUserIds: Iterable[str],
    poolRanks: Optional[Mapping[str, str]] = None,
    asOf: Optional[datetime] = None,
    saveFirstAttempts: bool = True,
    preferSro: bool = True,
) -> Optional[SelectionResult]:
    picks = selectCohosts(
        logPath,
        event,
        poolUserIds,
        poolRanks,
        asOf=asOf,
        saveFirstAttempts=saveFirstAttempts,
        preferSro=preferSro,
        slots=1,
    )
    return picks[0] if picks else None
