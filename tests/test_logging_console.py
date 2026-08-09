from __future__ import annotations

import logging
import unittest

from runtime.loggingConsole import (
    ConsoleNoiseFilter,
    JaneConsoleFormatter,
    _bannerLines,
    _shortLoggerName,
)


class _AsciiStream:
    encoding = "ascii"


class JaneConsoleFormatterTests(unittest.TestCase):
    def test_plain_formatter_is_compact_and_aligns_multiline_messages(self) -> None:
        formatter = JaneConsoleFormatter(useColor=False, useUnicode=False)
        record = logging.LogRecord(
            "runtime.bootstrap",
            logging.INFO,
            "",
            0,
            "Command sync complete\nGlobal: skipped",
            (),
            None,
        )

        rendered = formatter.format(record)
        lines = rendered.splitlines()

        self.assertRegex(lines[0], r"^\d{2}:\d{2}:\d{2}  > INFO  bootstrap\s+\|")
        self.assertTrue(lines[1].endswith("| Global: skipped"))
        self.assertNotIn("\x1b[", rendered)

    def test_color_formatter_only_adds_ansi_when_requested(self) -> None:
        formatter = JaneConsoleFormatter(useColor=True, useUnicode=True)
        record = logging.LogRecord(
            "cogs.community.eventCog",
            logging.WARNING,
            "",
            0,
            "Something needs attention.",
            (),
            None,
        )

        rendered = formatter.format(record)

        self.assertIn("\x1b[", rendered)
        self.assertIn("events", rendered)
        self.assertIn("WARN", rendered)

    def test_ascii_banner_avoids_unicode_only_characters(self) -> None:
        banner = "\n".join(_bannerLines(stream=_AsciiStream()))

        banner.encode("ascii")
        self.assertIn("JANE CLANKER", banner)
        self.assertIn("mind the loose bolts", banner)

    def test_common_logger_names_are_short_and_readable(self) -> None:
        self.assertEqual(_shortLoggerName("runtime.entrypoint"), "jane")
        self.assertEqual(_shortLoggerName("runtime.bootstrap"), "bootstrap")
        self.assertEqual(_shortLoggerName("cogs.community.eventCog"), "events")

    def test_discord_static_token_noise_is_filtered(self) -> None:
        record = logging.LogRecord(
            "discord.client",
            logging.INFO,
            "",
            0,
            "logging in using static token",
            (),
            None,
        )

        self.assertFalse(ConsoleNoiseFilter().filter(record))


if __name__ == "__main__":
    unittest.main()
