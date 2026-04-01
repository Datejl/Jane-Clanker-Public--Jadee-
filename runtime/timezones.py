from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

_utcOffsetTimezoneRegex = re.compile(
    r"^(?:UTC|GMT)\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?$",
    re.IGNORECASE,
)

_timezoneAliasToIana = {
    "UTC": "UTC",
    "GMT": "UTC",
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "AKST": "America/Anchorage",
    "AKDT": "America/Anchorage",
    "HST": "Pacific/Honolulu",
}

_timezoneAliasToFixedMinutes = {
    # Europe / Africa
    "WET": 0,
    "WEST": 60,
    "CET": 60,
    "CEST": 120,
    "EET": 120,
    "EEST": 180,
    "BST": 60,  # British Summer Time
    "SAST": 120,
    "WAT": 60,
    "CAT": 120,
    "EAT": 180,
    # Americas
    "AST": -240,
    "ADT": -180,
    "NST": -210,
    "NDT": -150,
    "ART": -180,
    "BRT": -180,
    "CLT": -240,
    "CLST": -180,
    "PET": -300,
    "COT": -300,
    # Middle East / Central Asia
    "MSK": 180,
    "GST": 240,  # Gulf Standard Time
    "IRST": 210,
    "IRDT": 270,
    "PKT": 300,
    # South / East / Southeast Asia
    "IST": 330,  # India Standard Time
    "NPT": 345,
    "MMT": 390,
    "THA": 420,
    "ICT": 420,
    "WIB": 420,
    "WITA": 480,
    "WIT": 540,
    "SGT": 480,
    "HKT": 480,
    "CSTCHINA": 480,
    "CSTCN": 480,
    "PHT": 480,
    "JST": 540,
    "KST": 540,
    # Oceania / Pacific
    "AWST": 480,
    "ACST": 570,
    "ACDT": 630,
    "AEST": 600,
    "AEDT": 660,
    "CHAST": 765,
    "CHADT": 825,
    "NZST": 720,
    "NZDT": 780,
    "CHST": 600,
    "SBT": 660,
    "FJT": 720,
    "TOT": 780,
}

_ianaToPreferredAlias = {
    "UTC": "UTC",
    "America/New_York": "EST",
    "America/Chicago": "CST",
    "America/Denver": "MST",
    "America/Los_Angeles": "PST",
    "America/Anchorage": "AKST",
    "Pacific/Honolulu": "HST",
}


def invalidTimezoneMessage() -> str:
    return (
        "Invalid timezone. Use aliases like EST/CST/CET/IST/UTC "
        "or UTC offsets like UTC-6."
    )


def formatUtcOffsetLabel(totalMinutes: int) -> str:
    if totalMinutes == 0:
        return "UTC"
    sign = "+" if totalMinutes >= 0 else "-"
    absoluteMinutes = abs(totalMinutes)
    hours = absoluteMinutes // 60
    minutes = absoluteMinutes % 60
    if minutes == 0:
        return f"UTC{sign}{hours}"
    return f"UTC{sign}{hours}:{minutes:02d}"


def parseUtcOffsetTimezone(token: str) -> tuple[timezone, str] | None:
    text = str(token or "").strip()
    if not text:
        return None

    upper = text.upper()
    if upper in {"UTC", "GMT", "Z"}:
        return timezone.utc, "UTC"

    match = _utcOffsetTimezoneRegex.match(text)
    if not match:
        return None

    signToken, hoursToken, minutesToken = match.groups()
    hours = int(hoursToken)
    minutes = int(minutesToken or "0")
    if hours > 14 or minutes > 59:
        return None

    totalMinutes = (hours * 60) + minutes
    if signToken == "-":
        totalMinutes *= -1
    return timezone(timedelta(minutes=totalMinutes)), formatUtcOffsetLabel(totalMinutes)


def displayTimezoneLabel(token: str) -> str:
    text = str(token or "").strip()
    if not text:
        return "UTC"

    upper = text.upper()
    normalizedUpper = upper.replace("-", "").replace("_", "").replace(" ", "")
    if upper in _timezoneAliasToIana or normalizedUpper in _timezoneAliasToFixedMinutes:
        return upper

    utcOffset = parseUtcOffsetTimezone(text)
    if utcOffset is not None:
        return utcOffset[1]

    return _ianaToPreferredAlias.get(text, text)


def resolveTimezoneToken(token: str, *, allowIana: bool = False) -> tuple[tzinfo, str]:
    text = str(token or "").strip()
    if not text:
        raise ValueError("Timezone is required.")

    utcOffset = parseUtcOffsetTimezone(text)
    if utcOffset is not None:
        return utcOffset

    upper = text.upper()
    normalizedUpper = upper.replace("-", "").replace("_", "").replace(" ", "")
    if normalizedUpper in _timezoneAliasToFixedMinutes:
        minutes = int(_timezoneAliasToFixedMinutes[normalizedUpper])
        return timezone(timedelta(minutes=minutes)), upper

    alias = _timezoneAliasToIana.get(upper)
    if alias:
        try:
            return ZoneInfo(alias), upper
        except Exception as exc:
            raise ValueError(invalidTimezoneMessage()) from exc

    if allowIana:
        try:
            return ZoneInfo(text), displayTimezoneLabel(text)
        except Exception as exc:
            raise ValueError(invalidTimezoneMessage()) from exc

    raise ValueError(invalidTimezoneMessage())


def parseDateTimeWithTimezone(
    dateText: str,
    timeWithTimezoneText: str,
    *,
    allowIana: bool = False,
) -> tuple[datetime, str]:
    dateValue = str(dateText or "").strip()
    timeWithTimezoneValue = str(timeWithTimezoneText or "").strip()
    if not timeWithTimezoneValue:
        raise ValueError("Time and timezone are required. Use HH:MM <timezone>.")

    parts = timeWithTimezoneValue.split(maxsplit=1)
    if len(parts) != 2:
        raise ValueError("Use time and timezone as: HH:MM EST (or UTC-6).")
    timeValue = str(parts[0] or "").strip()
    timezoneInput = str(parts[1] or "").strip()

    try:
        localTime = datetime.strptime(timeValue, "%H:%M")
    except ValueError as exc:
        raise ValueError("Time must be HH:MM in 24-hour format.") from exc

    tzInfo, timezoneLabel = resolveTimezoneToken(timezoneInput, allowIana=allowIana)
    if dateValue:
        try:
            parsedDate = datetime.strptime(dateValue, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("Date must be YYYY-MM-DD.") from exc
    else:
        parsedDate = datetime.now(tzInfo).date()

    localDateTime = datetime(
        year=parsedDate.year,
        month=parsedDate.month,
        day=parsedDate.day,
        hour=localTime.hour,
        minute=localTime.minute,
        tzinfo=tzInfo,
    )
    return localDateTime.astimezone(timezone.utc), timezoneLabel
