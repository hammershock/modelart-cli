import unittest
from argparse import Namespace
from unittest.mock import Mock, patch

from macli.commands.server import (
    _apply_server_context_after_autologin,
    _update_server_context_cfg,
)


class ServerContextTests(unittest.TestCase):
    def test_update_server_context_cfg_preserves_existing_without_args(self):
        cfg = {
            "enabled": True,
            "port": 8086,
            "region": "cn-north-9",
            "workspace_name": "SAI2",
            "workspace_id": "ws-old",
        }
        updated = _update_server_context_cfg(
            cfg.copy(),
            Namespace(region=None, workspace=None, workspace_id=None),
        )
        self.assertEqual(updated["region"], "cn-north-9")
        self.assertEqual(updated["workspace_name"], "SAI2")
        self.assertEqual(updated["workspace_id"], "ws-old")

    def test_update_server_context_cfg_workspace_name_clears_stale_id(self):
        updated = _update_server_context_cfg(
            {"workspace_id": "stale"},
            Namespace(region="cn-north-9", workspace="SAI2", workspace_id=None),
        )
        self.assertEqual(updated["region"], "cn-north-9")
        self.assertEqual(updated["workspace_name"], "SAI2")
        self.assertNotIn("workspace_id", updated)

    def test_apply_server_context_resolves_workspace_name(self):
        session = Mock()
        session.restore.return_value = True
        session.region = "cn-east-3"
        session.agency_id = "old-agency"
        session.cftk = "token"
        session.http = Mock()

        with patch("macli.commands.server.load_server_cfg", return_value={
            "region": "cn-north-9",
            "workspace_name": "SAI2",
        }), patch("macli.session.ConsoleSession", return_value=session), patch(
            "macli.auth._me",
            return_value={"projectId": "project-9", "id": "agency-9"},
        ), patch(
            "macli.auth._fetch_workspaces",
            return_value=[{"id": "ws-sai2", "name": "SAI2"}],
        ), patch("macli.commands.server.load_session", return_value={
            "region": "cn-east-3",
            "project_id": "project-old",
            "workspace_id": "ws-old",
        }), patch("macli.commands.server.save_session") as save_session:
            self.assertTrue(_apply_server_context_after_autologin())

        saved = save_session.call_args.args[0]
        self.assertEqual(saved["region"], "cn-north-9")
        self.assertEqual(saved["project_id"], "project-9")
        self.assertEqual(saved["agency_id"], "agency-9")
        self.assertEqual(saved["workspace_id"], "ws-sai2")
        self.assertEqual(session.workspace_id, "ws-sai2")


if __name__ == "__main__":
    unittest.main()
