"""统一 macOS launchd / Linux 进程 守护进程管理"""
import os, sys, time, signal, subprocess as _subprocess
from pathlib import Path

from macli.log import dprint


class DaemonManager:
    """Platform-specific daemon lifecycle for both *watch* and *server*.

    Parameters
    ----------
    name : str
        Human-readable name used in error messages (e.g. "watch", "server").
    plist_label : str
        macOS launchd job label (e.g. "com.macli.watch").
    plist_path : Path
        Absolute path to the .plist file under ~/Library/LaunchAgents.
    pid_file : Path
        PID file path for Linux process management.
    log_file : Path
        Log file path (used by both plist and Linux background process).
    cron_marker : str | None
        Comment marker for crontab entries (e.g. "# macli-watch").
        Only needed for watch-style cron integration.
    """

    def __init__(self, name: str, plist_label: str, plist_path: Path,
                 pid_file: Path, log_file: Path, cron_marker: str = None):
        self.name = name
        self.plist_label = plist_label
        self.plist_path = plist_path
        self.pid_file = pid_file
        self.log_file = log_file
        self.cron_marker = cron_marker

    # ── macOS launchd ────────────────────────────────────────────

    def plist_xml(self, program_args: list, interval_secs: int = None,
                  keep_alive: bool = False, run_at_load: bool = False) -> str:
        """Generate launchd plist XML.

        *program_args* — list of strings for ``<ProgramArguments>``.
        *interval_secs* — if set, adds ``<StartInterval>`` (watch pattern).
        *keep_alive* / *run_at_load* — used by the server pattern.
        """
        log = str(self.log_file)
        args_xml = "".join(
            f"        <string>{a}</string>\n" for a in program_args
        )

        optional_keys = ""
        if interval_secs is not None:
            optional_keys += (
                f"    <key>StartInterval</key>\n"
                f"    <integer>{interval_secs}</integer>\n"
            )
        optional_keys += (
            f"    <key>RunAtLoad</key>\n"
            f"    <{'true' if run_at_load else 'false'}/>\n"
        )
        if keep_alive:
            optional_keys += (
                "    <key>KeepAlive</key>\n    <true/>\n"
            )

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"'
            ' "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
            '<plist version="1.0">\n'
            '<dict>\n'
            f'    <key>Label</key>\n    <string>{self.plist_label}</string>\n'
            '    <key>ProgramArguments</key>\n    <array>\n'
            f'{args_xml}'
            '    </array>\n'
            f'{optional_keys}'
            f'    <key>StandardOutPath</key>\n    <string>{log}</string>\n'
            f'    <key>StandardErrorPath</key>\n    <string>{log}</string>\n'
            '</dict>\n'
            '</plist>\n'
        )

    def launchctl(self, action: str) -> bool:
        """Run ``launchctl load/unload -w <plist_path>``."""
        r = _subprocess.run(
            ["launchctl", action, "-w", str(self.plist_path)],
            capture_output=True, text=True,
        )
        dprint(f"[dim]launchctl {action} → {r.returncode}[/dim]")
        return r.returncode == 0

    def launchctl_is_loaded(self) -> bool:
        """Return *True* if the launchd job is currently loaded."""
        r = _subprocess.run(
            ["launchctl", "list", self.plist_label],
            capture_output=True, text=True,
        )
        return r.returncode == 0

    # ── Linux process management ─────────────────────────────────

    def linux_is_running(self) -> bool:
        """Check whether the daemon is alive via its PID file."""
        if not self.pid_file.exists():
            return False
        try:
            pid = int(self.pid_file.read_text(encoding="utf-8").strip())
            os.kill(pid, 0)
            return True
        except (ValueError, ProcessLookupError, PermissionError):
            return False

    def linux_start(self, cmd: list) -> None:
        """Stop any existing instance, start *cmd* as a background process,
        and write the new PID file.

        Raises ``RuntimeError`` if the process exits within 1.5 s.
        """
        self.linux_stop()
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        log_fd = open(str(self.log_file), "a")
        proc = _subprocess.Popen(
            cmd, stdout=log_fd, stderr=log_fd,
            start_new_session=True, close_fds=True,
        )
        log_fd.close()
        time.sleep(1.5)
        if proc.poll() is not None:
            try:
                lines = self.log_file.read_text(
                    encoding="utf-8", errors="replace",
                ).splitlines()
                tail = "\n".join(lines[-5:])
            except Exception:
                tail = "(unable to read log)"
            raise RuntimeError(f"{self.name} daemon startup failed:\n{tail}")
        self.pid_file.write_text(str(proc.pid), encoding="utf-8")

    def linux_stop(self) -> None:
        """Send SIGTERM to the recorded PID and remove the PID file."""
        if not self.pid_file.exists():
            return
        try:
            pid = int(self.pid_file.read_text(encoding="utf-8").strip())
            os.kill(pid, signal.SIGTERM)
        except (ValueError, ProcessLookupError, PermissionError):
            pass
        finally:
            self.pid_file.unlink(missing_ok=True)

    # ── Cron helpers (used by watch) ─────────────────────────────

    def cron_get_lines(self) -> list:
        """Return non-empty lines from the current crontab."""
        r = _subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if r.returncode != 0:
            return []
        return [l for l in r.stdout.splitlines() if l]

    def cron_set_lines(self, lines: list):
        """Replace the entire crontab with *lines*."""
        text = "\n".join(lines) + "\n"
        _subprocess.run(["crontab", "-"], input=text, text=True, check=True)

    def cron_is_active(self) -> bool:
        """Return *True* if the crontab contains this daemon's marker."""
        if not self.cron_marker:
            return False
        return any(self.cron_marker in l for l in self.cron_get_lines())

    def cron_install(self, cron_expr: str, cmd_str: str):
        """Add (or replace) a crontab entry for this daemon.

        *cron_expr* — e.g. ``"0 */2 * * *"``
        *cmd_str*   — the shell command (without the cron schedule prefix)
        """
        if not self.cron_marker:
            raise ValueError("cron_marker not set for this DaemonManager")
        lines = [l for l in self.cron_get_lines() if self.cron_marker not in l]
        entry = f"{cron_expr} {cmd_str} {self.cron_marker}"
        lines.append(entry)
        self.cron_set_lines(lines)

    def cron_remove(self):
        """Remove all crontab entries matching this daemon's marker."""
        if not self.cron_marker:
            return
        lines = [l for l in self.cron_get_lines() if self.cron_marker not in l]
        self.cron_set_lines(lines)


# ── Standalone helpers ───────────────────────────────────────────

def _run_check_once(script_path: Path, threshold_hours: int) -> int:
    """Execute the watch check script once and return its exit code."""
    cmd = [sys.executable, str(script_path), "--threshold-hours", str(threshold_hours)]
    return _subprocess.run(cmd, text=True).returncode
