from __future__ import annotations

from typing import Optional

import discord


def buildOutfitEmbed(
    targetUserId: int,
    robloxUsername: Optional[str],
    outfits: list[dict],
    thumbnails: dict[int, Optional[str]],
    index: int,
) -> discord.Embed:
    outfit = outfits[index]
    outfitId = outfit.get("id")
    name = outfit.get("name") or "Outfit"
    total = len(outfits)
    title = f"{name} ({index + 1}/{total})"
    description = f"<@{targetUserId}> - Outfit ID: {outfitId}"
    embed = discord.Embed(title=title, description=description)

    imageUrl = thumbnails.get(outfitId) if isinstance(outfitId, int) else None
    if imageUrl:
        embed.set_image(url=imageUrl)
    else:
        embed.add_field(name="Preview", value="Thumbnail unavailable.", inline=False)

    if robloxUsername:
        embed.set_footer(text=f"Roblox: {robloxUsername}")
    return embed


def buildOutfitPageEmbeds(
    targetUserId: int,
    robloxUsername: Optional[str],
    outfits: list[dict],
    thumbnails: dict[int, Optional[str]],
    pageIndex: int,
    pageSize: int = 10,
) -> list[discord.Embed]:
    total = len(outfits)
    if total <= 0:
        return [discord.Embed(title="Outfits", description="No outfits found.")]

    pageSize = max(1, min(10, int(pageSize)))
    start = pageIndex * pageSize
    end = min(total, start + pageSize)
    pageNumber = pageIndex + 1
    pageCount = max(1, (total + pageSize - 1) // pageSize)
    embeds: list[discord.Embed] = []

    for idx in range(start, end):
        outfit = outfits[idx]
        outfitId = outfit.get("id")
        name = str(outfit.get("name") or "Outfit").strip()
        embed = discord.Embed(
            title=f"{idx + 1}. {name[:90]}",
            description=f"<@{targetUserId}> | Outfit ID: {outfitId}",
        )

        imageUrl = thumbnails.get(outfitId) if isinstance(outfitId, int) else None
        if imageUrl:
            # Keep images compact for high-volume reviews.
            embed.set_thumbnail(url=imageUrl)
        else:
            embed.add_field(name="Preview", value="Thumbnail unavailable.", inline=False)

        if idx == start:
            footerParts = [f"Page {pageNumber}/{pageCount}", f"Showing {start + 1}-{end} of {total}"]
            if robloxUsername:
                footerParts.insert(0, f"Roblox: {robloxUsername}")
            embed.set_footer(text=" | ".join(footerParts))
        embeds.append(embed)

    return embeds

