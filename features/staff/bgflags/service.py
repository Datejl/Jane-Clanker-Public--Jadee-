from typing import Optional, List, Dict

from db.sqlite import execute, executeReturnId, fetchAll


async def addRule(ruleType: str, ruleValue: str, note: Optional[str], createdBy: Optional[int]) -> int:
    return await executeReturnId(
        """
        INSERT INTO bg_flag_rules (ruleType, ruleValue, note, createdBy)
        VALUES (?, ?, ?, ?)
        """,
        (ruleType, ruleValue, note, createdBy),
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
