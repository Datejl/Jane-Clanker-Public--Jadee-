from typing import Optional, List, Dict

from db.sqlite import execute, executeReturnId, fetchAll


def normalizeSeverity(value: object) -> int:
    try:
        severity = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, min(100, severity))


async def addRule(
    ruleType: str,
    ruleValue: str,
    note: Optional[str],
    createdBy: Optional[int],
    severity: int = 0,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO bg_flag_rules (ruleType, ruleValue, note, severity, createdBy)
        VALUES (?, ?, ?, ?, ?)
        """,
        (ruleType, ruleValue, note, normalizeSeverity(severity), createdBy),
    )


async def removeRule(ruleId: int) -> None:
    await execute("DELETE FROM bg_flag_rules WHERE ruleId = ?", (ruleId,))


async def listRules(ruleType: Optional[str] = None) -> List[Dict]:
    if ruleType:
        return await fetchAll(
            "SELECT * FROM bg_flag_rules WHERE ruleType = ? ORDER BY ruleId ASC",
            (ruleType,),
        )
    return await fetchAll("SELECT * FROM bg_flag_rules ORDER BY ruleId ASC")
