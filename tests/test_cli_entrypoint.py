import runpy
import types
import unittest
from unittest.mock import patch


class ModuleEntrypointTests(unittest.TestCase):
    def test_module_preserves_cli_exit_status(self):
        for status in (0, 7):
            with self.subTest(status=status):
                cli = types.ModuleType("bidbridge.cli")
                cli.main = lambda: status
                with patch.dict("sys.modules", {"bidbridge.cli": cli}):
                    with self.assertRaises(SystemExit) as raised:
                        runpy.run_module("bidbridge", run_name="__main__")
                self.assertEqual(raised.exception.code, status)
