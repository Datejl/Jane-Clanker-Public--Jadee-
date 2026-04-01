from __future__ import annotations

from db.sqlite import execute, executeReturnId, fetchAll


def _normalizeLinkType(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"SHARED_STAFF", "REVIEW_ROUTING", "MIRROR"} else "SHARED_STAFF"


async def addFederationLink(
    *,
    guildId: int,
    linkedGuildId: int,
    linkType: str,
    note: str,
    createdBy: int,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO guild_federation_links (
            guildId, linkedGuildId, linkType, note, createdBy, createdAt
        )
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            int(guildId),
            int(linkedGuildId),
            _normalizeLinkType(linkType),
            str(note or "").strip(),
            int(createdBy or 0),
        ),
    )


async def removeFederationLink(*, guildId: int, linkedGuildId: int) -> None:
    await execute(
        "DELETE FROM guild_federation_links WHERE guildId = ? AND linkedGuildId = ?",
        (int(guildId), int(linkedGuildId)),
    )


async def listFederationLinks(*, guildId: int) -> list[dict]:
    return await fetchAll(
        """
        SELECT linkId, guildId, linkedGuildId, linkType, note, createdBy, createdAt
        FROM guild_federation_links
        WHERE guildId = ?
        ORDER BY linkId ASC
        """,
        (int(guildId),),
    )
