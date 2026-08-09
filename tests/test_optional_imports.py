from __future__ import annotations

import unittest
from unittest.mock import patch

from runtime import extensionLayout, privateServices
from runtime.optionalImports import isRequestedModuleMissing


class OptionalImportTests(unittest.TestCase):
    def test_requested_module_or_parent_can_be_optional(self) -> None:
        missingModule = ModuleNotFoundError("missing optional module", name="plugins.private.example")
        missingParent = ModuleNotFoundError("missing optional parent", name="plugins.private")

        self.assertTrue(isRequestedModuleMissing(missingModule, "plugins.private.example"))
        self.assertTrue(isRequestedModuleMissing(missingParent, "plugins.private.example"))

    def test_missing_dependency_is_not_treated_as_an_optional_module(self) -> None:
        missingDependency = ModuleNotFoundError("missing dependency", name="third_party_dependency")

        self.assertFalse(
            isRequestedModuleMissing(missingDependency, "plugins.private.example")
        )

    def test_private_service_import_propagates_missing_dependency(self) -> None:
        missingDependency = ModuleNotFoundError("missing dependency", name="googleapiclient")
        with patch(
            "runtime.optionalImports.importlib.import_module",
            side_effect=missingDependency,
        ):
            with self.assertRaises(ModuleNotFoundError):
                privateServices._tryImportModule("features.staff.orbat.sheets")

    def test_extension_list_allows_missing_list_but_not_broken_list(self) -> None:
        missingList = ModuleNotFoundError("missing list", name="plugins.optional")
        with patch("runtime.extensionLayout.importlib.import_module", side_effect=missingList):
            self.assertEqual(
                extensionLayout._loadOptionalExtensionNames("plugins.optional.extensionList"),
                [],
            )

        missingDependency = ModuleNotFoundError("missing dependency", name="dependency")
        with patch(
            "runtime.extensionLayout.importlib.import_module",
            side_effect=missingDependency,
        ):
            with self.assertRaises(ModuleNotFoundError):
                extensionLayout._loadOptionalExtensionNames("plugins.optional.extensionList")


if __name__ == "__main__":
    unittest.main()
