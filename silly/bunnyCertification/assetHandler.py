import os

import discord

plate = os.path.abspath("silly/bunnyCertification/assets/plate.png")
hello = os.path.abspath("silly/bunnyCertification/assets/hello.png")

_metaDataImageURL = "https://ibb.co/FkYYQfXP"
_stageFourUserId = 5297112306
_secondStageText = """
I come once a year with colors so bright,
With eggs that are hidden just out of sight.
Follow the bunny, quick on your feet,
Through fields where spring and blossoms meet.

Where flowers bloom and new life begins,
Look past the eggs and chocolate wins.
Not in your basket, nor where you play,
But where tall things grow and quietly stay.

They sway in the breeze, rooted and tall,
Guarding the place that’s central to all.
Not at the edges, not near the line,
But deep where many pathways combine.

But don’t take them all, be careful, be wise—
Split what you see into three equal tries.
Count what stands in the heart of the scene,
Then take just a third of the tall and green.

What do you find when your counting is fair,
At the center, with trees divided with care?
"""

_assetTable = [
    lambda: discord.File(plate, filename="image.png"),
    lambda: _secondStageText,
    lambda: _metaDataImageURL,
    lambda: _stageFourUserId,
    lambda: discord.File(hello, filename="image.png"),
]

def getAsset(StageId: int):
    return _assetTable[StageId - 1]()