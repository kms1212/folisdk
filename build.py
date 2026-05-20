#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import deque
import hashlib
import os
import re
import shlex
import shutil
import signal
import select
import subprocess
import sys
import tarfile
import textwrap
import threading
import time
import tty
import termios
from dataclasses import dataclass, field
from pathlib import Path

from build_steps import (
    ArchBuildState,
    BuildStep,
    JobControl,
    StepGraph,
    StepRestart,
    StampedStep,
    create_arch_graph,
    create_global_prepare_graph,
    create_host_cleanup_graph,
    create_host_graph,
)


SYSTEM_PATH = "/usr/bin:/bin:/usr/sbin:/sbin"
FREEBSD_LOCAL_PATH = "/usr/local/bin:/usr/local/sbin"
ANSI_RESET = "\x1b[0m"
ANSI_SGR_RE = re.compile(r"\x1b\[[0-9;]*m")


class BuildCancelled(KeyboardInterrupt):
    def __init__(self, reason: str = "build cancelled") -> None:
        super().__init__(reason)
        self.reason = reason


def script_body(script: str) -> str:
    return (
        "set -xeuo pipefail\n"
        "make() { \"$MAKE\" \"$@\"; }\n"
        + textwrap.dedent(script).strip()
        + "\n"
    )


def resolve_path(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def stdout_supports_ansi(stream: object, env: dict[str, str] | None = None) -> bool:
    term_env = os.environ if env is None else env
    term = term_env.get("TERM", "")
    is_tty = getattr(stream, "isatty", lambda: False)()
    return bool(is_tty and term not in ("", "dumb"))


@dataclass(frozen=True)
class SourceArchive:
    output_name: str
    url_key: str
    sha256_key: str
    extracted_dir: str | None
    destination_dir: str


@dataclass
class ActiveJob:
    graph_label: str
    step_name: str
    title: str
    slots: int
    slot_mode: str
    started_at: float
    status: str = "running"
    failure_message: str | None = None
    lines: list[str] = field(default_factory=list)


@dataclass
class GraphProgress:
    label: str = ""
    total: int = 0
    completed: int = 0
    ready: int = 0
    running: int = 0
    used_slots: int = 0
    failed: bool = False


@dataclass
class ManagedProcess:
    job_key: str | None
    process: subprocess.Popen[str]


def _consume_escape_sequence(text: str, start: int) -> tuple[str, int]:
    if start >= len(text):
        return "", start

    if text[start] != "\x1b":
        return "", start + 1

    if start + 1 >= len(text):
        return text[start], len(text)

    marker = text[start + 1]
    if marker == "[":
        index = start + 2
        while index < len(text):
            if 0x40 <= ord(text[index]) <= 0x7E:
                return text[start : index + 1], index + 1
            index += 1
        return text[start:], len(text)

    if marker == "]":
        index = start + 2
        while index < len(text):
            if text[index] == "\x07":
                return text[start : index + 1], index + 1
            if text[index] == "\x1b" and index + 1 < len(text) and text[index + 1] == "\\":
                return text[start : index + 2], index + 2
            index += 1
        return text[start:], len(text)

    return text[start : start + 2], min(len(text), start + 2)


def sanitize_terminal_line(text: str, tab_width: int = 8) -> str:
    current = text.split("\r")[-1]
    result: list[str] = []
    visible = 0
    index = 0

    while index < len(current):
        char = current[index]
        if char == "\x1b":
            sequence, index = _consume_escape_sequence(current, index)
            if ANSI_SGR_RE.fullmatch(sequence):
                result.append(sequence)
            continue

        if char == "\t":
            spaces = tab_width - (visible % tab_width)
            result.append(" " * spaces)
            visible += spaces
            index += 1
            continue

        if char in ("\b", "\x00"):
            index += 1
            continue

        if ord(char) < 32 or ord(char) == 127:
            index += 1
            continue

        result.append(char)
        visible += 1
        index += 1

    return "".join(result)


class TerminalStatusPanel:
    TITLE_STYLE = "\x1b[1;97;48;5;24m"
    FAILED_TITLE_STYLE = "\x1b[1;97;48;5;124m"
    SEPARATOR_STYLE = "\x1b[48;5;24m"

    def __init__(self, ctx: "BuildContext") -> None:
        self.ctx = ctx
        self.stream = sys.stdout
        self.enabled = False
        self.stop_event = threading.Event()
        self.refresh_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.rows = 0
        self.columns = 0

    def _supports_terminal(self) -> bool:
        return (
            not self.ctx.direct_output
            and stdout_supports_ansi(self.stream, self.ctx.env)
            and self.ctx.env.get("GITHUB_ACTIONS", "false") != "true"
        )

    def _update_dimensions(self) -> None:
        size = shutil.get_terminal_size((120, 30))
        self.columns = size.columns
        self.rows = size.lines

    def start(self) -> None:
        if not self._supports_terminal():
            return

        self._update_dimensions()
        if self.rows <= 8:
            return

        self.enabled = True
        self._write("\x1b[?1049h\x1b[?25l\x1b[2J\x1b[H")
        self._render()
        self.thread = threading.Thread(target=self._run, name="build-status-panel", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        if not self.enabled:
            return

        self.stop_event.set()
        self.refresh_event.set()
        if self.thread is not None:
            self.thread.join()
            self.thread = None
        self._clear()
        self.enabled = False

    def request_render(self) -> None:
        if self.enabled:
            self.refresh_event.set()

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self.refresh_event.wait(0.25)
            self.refresh_event.clear()
            if self.stop_event.is_set():
                break
            self._render()

    def _write(self, text: str) -> None:
        self.ctx.emit_text(text)

    def _visible_width(self, line: str) -> int:
        width = 0
        index = 0
        while index < len(line):
            if line[index] == "\x1b":
                _, index = _consume_escape_sequence(line, index)
                continue
            width += 1
            index += 1
        return width

    def _pad_visible(self, line: str, width: int) -> str:
        padded = line
        if "\x1b[" in padded:
            padded += ANSI_RESET
        padding = max(0, width - self._visible_width(padded))
        if padding:
            padded += " " * padding
        return padded

    def _pane_style(self, job: ActiveJob) -> str:
        if job.status == "failed":
            return self.FAILED_TITLE_STYLE
        return self.TITLE_STYLE

    def _apply_pane_style(self, line: str, style: str) -> str:
        if not style:
            return line
        return f"{style}{line.replace(ANSI_RESET, ANSI_RESET + style)}{ANSI_RESET}"

    def _trim(self, line: str, width: int | None = None) -> str:
        limit = self.columns if width is None else max(0, width)
        if self._visible_width(line) <= limit:
            return line
        if limit <= 1:
            return "…"[:limit]

        target = limit - 1
        visible = 0
        index = 0
        parts: list[str] = []
        saw_ansi = False
        while index < len(line):
            if line[index] == "\x1b":
                sequence, index = _consume_escape_sequence(line, index)
                if ANSI_SGR_RE.fullmatch(sequence):
                    saw_ansi = True
                    parts.append(sequence)
                continue

            if visible >= target:
                index += 1
                continue

            parts.append(line[index])
            visible += 1
            index += 1

        if saw_ansi:
            parts.append(ANSI_RESET)
        parts.append("…")
        return "".join(parts)

    def _update_active_sgr(self, active: list[str], sequence: str) -> list[str]:
        if not ANSI_SGR_RE.fullmatch(sequence):
            return active

        codes = [code for code in sequence[2:-1].split(";") if code]
        has_reset = not codes or "0" in codes
        has_style = any(code != "0" for code in codes)

        next_active = [] if has_reset else list(active)
        if has_style:
            next_active.append(sequence)
        return next_active

    def _wrap_line(
        self,
        line: str,
        width: int,
        *,
        first_prefix: str = "",
        continuation_prefix: str = "",
    ) -> list[str]:
        if width <= 0:
            return []

        if self._visible_width(first_prefix) >= width:
            return [self._pad_visible(self._trim(f"{first_prefix}{line}", width), width)]

        current_prefix = first_prefix
        current_parts: list[str] = [current_prefix]
        current_width = self._visible_width(current_prefix)
        active_sgr: list[str] = []
        wrapped: list[str] = []
        saw_content = False
        index = 0

        while index < len(line):
            if line[index] == "\x1b":
                sequence, index = _consume_escape_sequence(line, index)
                if ANSI_SGR_RE.fullmatch(sequence):
                    active_sgr = self._update_active_sgr(active_sgr, sequence)
                    current_parts.append(sequence)
                continue

            if current_width >= width:
                segment = "".join(current_parts)
                wrapped.append(self._pad_visible(segment, width))
                current_prefix = continuation_prefix
                current_parts = [current_prefix]
                current_width = self._visible_width(current_prefix)
                if active_sgr:
                    current_parts.extend(active_sgr)

            current_parts.append(line[index])
            current_width += 1
            saw_content = True
            index += 1

        if not saw_content and not wrapped:
            return [self._pad_visible(first_prefix, width)]

        segment = "".join(current_parts)
        wrapped.append(self._pad_visible(segment, width))
        return wrapped

    def _format_duration(self, started_at: float) -> str:
        seconds = int(max(0, time.monotonic() - started_at))
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:02}:{minutes:02}:{seconds:02}"
        return f"{minutes:02}:{seconds:02}"

    def _pane_body(self, job: ActiveJob, height: int, width: int) -> list[str]:
        if height <= 0 or width <= 0:
            return []

        title_style = self._pane_style(job)
        if job.status == "failed":
            header = f"FAIL {job.title} | {self._format_duration(job.started_at)} | {job.slots} cpu"
        else:
            header = f"[{job.slots} cpu] {job.title} | {self._format_duration(job.started_at)}"

        lines = [self._apply_pane_style(self._pad_visible(self._trim(header, width), width), title_style)]
        body_height = max(1, height - 1)
        body = list(job.lines[-body_height:]) if job.lines else ["(no output yet)"]
        if job.status == "failed" and job.failure_message:
            failure_line = f"\x1b[1;31merror:\x1b[0m {job.failure_message}"
            body = [failure_line, *body]

        wrapped_body: list[str] = []
        for line in body:
            wrapped_body.extend(
                self._wrap_line(
                    line,
                    width,
                    first_prefix="",
                    continuation_prefix="",
                )
            )

        for line in wrapped_body[-body_height:]:
            lines.append(line)
        while len(lines) < height:
            lines.append(" " * width)
        return lines[:height]

    def _layout_jobs(
        self,
        jobs: list[ActiveJob],
        available_rows: int,
        min_pane_height: int,
        min_pane_width: int,
        separator_width: int,
    ) -> tuple[int, int]:
        if not jobs or available_rows < min_pane_height:
            return 1, 0

        max_columns = min(len(jobs), max(1, (self.columns + separator_width) // (min_pane_width + separator_width)))
        best_columns = 1
        best_visible = min(len(jobs), max(1, available_rows // min_pane_height))

        for columns in range(1, max_columns + 1):
            pane_width = (self.columns - separator_width * max(0, columns - 1)) // columns
            if pane_width < min_pane_width:
                continue
            pane_rows = max(1, available_rows // min_pane_height)
            visible = min(len(jobs), columns * pane_rows)
            if visible > best_visible or (visible == best_visible and columns > best_columns):
                best_columns = columns
                best_visible = visible

        return best_columns, best_visible

    def _render_lines(self) -> list[str]:
        snapshot = self.ctx.status_snapshot()
        graph: GraphProgress = snapshot["graph"]
        jobs: list[ActiveJob] = snapshot["jobs"]
        failed_jobs: list[ActiveJob] = snapshot["failed_jobs"]
        notices: list[str] = snapshot["notices"]
        separator_width = 1
        separator = self._apply_pane_style(" " * separator_width, self.SEPARATOR_STYLE)
        lines: list[str] = []
        lines.append(
            self._trim(
                f"Build Jobs | CPU {graph.used_slots}/{self.ctx.parallel} active | spare {self.ctx.spare_slots} | running {len(jobs)} | failed {len(failed_jobs)}"
            )
        )

        if graph.label:
            status_suffix = " | FAILED" if graph.failed else ""
            lines.append(
                self._trim(
                    f"Graph {graph.label} | done {graph.completed}/{graph.total} | ready {graph.ready} | running {graph.running}{status_suffix}"
                )
            )
        else:
            lines.append(self._trim("Graph idle"))

        if self.ctx.failure_prompt:
            lines.append(self._trim(f"\x1b[1;97;41m {self.ctx.failure_prompt} \x1b[0m"))
        elif notices:
            lines.append(self._trim(f"Notice: {notices[-1]}"))

        sorted_jobs = sorted(
            [*failed_jobs, *jobs],
            key=lambda job: (0 if job.status == "failed" else 1, -job.slots, job.started_at, job.title),
        )
        remaining_rows = max(0, self.rows - len(lines))
        min_pane_height = 4
        min_pane_width = 32

        if not sorted_jobs:
            filler = ["waiting for runnable jobs..."]
            lines.extend(filler)
        else:
            columns, visible_jobs = self._layout_jobs(
                sorted_jobs,
                remaining_rows,
                min_pane_height,
                min_pane_width,
                separator_width,
            )
            visible = sorted_jobs[:visible_jobs]
            overflow = len(sorted_jobs) - visible_jobs
            if overflow > 0:
                lines.append(self._trim(f"Showing {visible_jobs}/{len(sorted_jobs)} running jobs"))
                remaining_rows = max(0, self.rows - len(lines))
                columns, visible_jobs = self._layout_jobs(
                    sorted_jobs,
                    remaining_rows,
                    min_pane_height,
                    min_pane_width,
                    separator_width,
                )
                visible = sorted_jobs[:visible_jobs]

            pane_rows = max(1, (len(visible) + columns - 1) // columns)
            total_separator_width = separator_width * max(0, columns - 1)
            pane_width = max(1, (self.columns - total_separator_width) // columns)
            base_height = max(min_pane_height, remaining_rows // pane_rows)
            extra_rows = max(0, remaining_rows - base_height * pane_rows)

            for row_index in range(pane_rows):
                row_height = base_height + (1 if row_index < extra_rows else 0)
                row_jobs = visible[row_index * columns : (row_index + 1) * columns]
                panes = [self._pane_body(job, row_height, pane_width) for job in row_jobs]
                while len(panes) < columns:
                    panes.append([" " * pane_width for _ in range(row_height)])

                for line_index in range(row_height):
                    lines.append(separator.join(pane[line_index] for pane in panes))

        while len(lines) < self.rows:
            lines.append("")
        return lines[: self.rows]

    def _render(self) -> None:
        if not self.enabled:
            return

        self._update_dimensions()
        if self.rows <= 8:
            return

        lines = self._render_lines()
        buffer = ["\x1b[H"]
        for index, line in enumerate(lines):
            row = index + 1
            buffer.append(f"\x1b[{row};1H\x1b[2K{line}")
        for row in range(len(lines) + 1, self.rows + 1):
            buffer.append(f"\x1b[{row};1H\x1b[2K")
        self._write("".join(buffer))

    def _clear(self) -> None:
        self._write("\x1b[?25h\x1b[?1049l")

    def hold_on_failure(self) -> None:
        if not self.enabled or not self.ctx.failed_jobs:
            return

        self.stop_event.set()
        self.refresh_event.set()
        if self.thread is not None:
            self.thread.join()
            self.thread = None

        self.ctx.failure_prompt = "Build failed. Press Enter to exit."
        self._render()

        if not sys.stdin.isatty():
            time.sleep(3)
            return

        fd = sys.stdin.fileno()
        try:
            old_settings = termios.tcgetattr(fd)
        except termios.error:
            return

        try:
            tty.setcbreak(fd)
            while True:
                ready, _, _ = select.select([fd], [], [], 0.25)
                if not ready:
                    continue
                key = os.read(fd, 1)
                if not key or key in (b"\r", b"\n", b"\x03", b"\x04"):
                    break
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


class BuildContext:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.root = Path.cwd().resolve()
        self.osname = os.uname().sysname
        self.bash = shutil.which("bash") or "/bin/bash"
        self.dry_run = args.dry_run
        self.cpu_count = os.cpu_count() or 1
        self.output_lock = threading.RLock()
        self.status_lock = threading.RLock()
        self.thread_state = threading.local()
        self.active_jobs: dict[str, ActiveJob] = {}
        self.failed_jobs: dict[str, ActiveJob] = {}
        self.managed_processes: dict[int, ManagedProcess] = {}
        self.notices: deque[str] = deque(maxlen=5)
        self.graph_progress = GraphProgress()
        self.failure_prompt = ""
        self.cancel_event = threading.Event()
        self.force_cancel_event = threading.Event()
        self.cancel_reason = ""
        self._saved_signal_handlers: dict[signal.Signals, object] = {}

        self.archs = [arch.strip() for arch in args.arch.split(",") if arch.strip()]
        if not self.archs:
            raise SystemExit("At least one architecture must be specified.")

        self.builddir = resolve_path(self.root, args.build_dir or "build")
        self.builddir_layout = bool(args.builddir_layout)
        if self.builddir_layout and args.destination:
            raise SystemExit("--builddir-layout cannot be combined with --destination.")
        self.pkgbuilddir = self.builddir if self.builddir_layout else self.builddir / "pkgroot"

        if self.builddir_layout:
            self.destination = ""
        elif args.destination:
            self.destination = args.destination
        elif self.osname == "Darwin":
            self.destination = "/opt/homebrew/opt"
        else:
            self.destination = "/opt"

        self.parallel = args.jobs or self.default_jobs()
        self.spare_slots = max(0, self.cpu_count - self.parallel)
        self.no_libs = args.no_libs
        self.sed_type = "bsd" if self.osname in {"Darwin", "FreeBSD", "OpenBSD", "NetBSD", "DragonFly"} else "gnu"
        self.rerun_step_selectors = tuple(selector.strip() for selector in (args.rerun_step or []) if selector.strip())
        self.rerun_job_keys: set[str] = set()

        self.env = os.environ.copy()
        self.env.update(self.base_environment())
        self.env.update(self.load_versions())
        self.direct_output = bool(args.direct_output) or not stdout_supports_ansi(sys.stdout, os.environ)

        if not self.dry_run:
            self.pkgbuilddir.mkdir(parents=True, exist_ok=True)
        self.host_prefix = f"{self.destination}/folisdk-host"
        self.env.update(
            {
                "ROOT": str(self.root),
                "OSNAME": self.osname,
                "BUILDDIR": str(self.builddir),
                "PKGBUILDDIR": str(self.pkgbuilddir),
                "DESTINATION": self.destination,
                "PREFIX_LAYOUT": "builddir" if self.builddir_layout else "staged",
                "PARALLEL": str(self.parallel),
                "HOST_PREFIX": self.host_prefix,
                "SED_TYPE": self.sed_type,
                "ARCH_LIST": ",".join(self.archs),
                "M4": self.env["M4"],
                "MAKE": self.env["MAKE"],
                "PATH": f"{self.pkg_prefix_join(self.host_prefix, 'bin')}:{self.system_path()}",
                "CPPFLAGS": f"-I{self.pkg_prefix_join(self.host_prefix, 'include')}",
                "LDFLAGS": f"-L{self.pkg_prefix_join(self.host_prefix, 'lib')}",
            }
        )

        for key in [
            "LD_LIBRARY_PATH",
            "DYLD_LIBRARY_PATH",
            "C_INCLUDE_PATH",
            "CPLUS_INCLUDE_PATH",
            "LIBRARY_PATH",
        ]:
            self.env.pop(key, None)

        if self.osname == "Darwin":
            self.env["SDKROOT"] = self.capture(
                ["xcrun", "--sdk", "macosx", "--show-sdk-path"],
                env=os.environ.copy(),
            )
            self.env["MAKEINFO"] = "/opt/homebrew/bin/makeinfo"

        self.root_path = ""
        self.root_cppflags = ""
        self.root_ldflags = ""
        self.build_triplet = ""
        self.status_panel = TerminalStatusPanel(self)

    def default_jobs(self) -> int:
        return self.cpu_count - 1 if self.cpu_count > 1 else 1

    def emit_text(self, text: str) -> None:
        with self.output_lock:
            sys.stdout.write(text)
            sys.stdout.flush()

    def emit_line(self, text: str) -> None:
        self.emit_text(f"{text}\n")

    def current_job_key(self) -> str | None:
        return getattr(self.thread_state, "job_key", None)

    def current_job_control(self) -> JobControl | None:
        return getattr(self.thread_state, "job_control", None)

    def bind_current_job(self, graph_label: str, step_name: str, control: JobControl | None = None) -> None:
        self.thread_state.job_key = f"{graph_label}:{step_name}"
        self.thread_state.job_control = control

    def clear_current_job(self) -> None:
        if hasattr(self.thread_state, "job_key"):
            del self.thread_state.job_key
        if hasattr(self.thread_state, "job_control"):
            del self.thread_state.job_control

    def append_job_output(self, job_key: str | None, text: str) -> None:
        if not text:
            return

        lines = [sanitize_terminal_line(line) for line in text.splitlines()]
        if not lines:
            lines = [sanitize_terminal_line(text)]

        with self.status_lock:
            if job_key is None or job_key not in self.active_jobs:
                for line in lines:
                    self.notices.append(line)
            else:
                job = self.active_jobs[job_key]
                job.lines.extend(lines)
                if len(job.lines) > 200:
                    del job.lines[:-200]
        self.status_panel.request_render()

    def log_line(self, text: str) -> None:
        if self.status_panel.enabled:
            self.append_job_output(self.current_job_key(), text)
            return
        self.emit_line(text)

    def cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def cancellation_error(self) -> BuildCancelled:
        reason = self.cancel_reason or "build cancelled"
        return BuildCancelled(reason)

    def raise_if_cancelled(self) -> None:
        if self.cancelled():
            raise self.cancellation_error()

    def _signal_name(self, sig: signal.Signals | int) -> str:
        try:
            return signal.Signals(sig).name
        except ValueError:
            return str(sig)

    def _notify_notice(self, message: str) -> None:
        with self.status_lock:
            self.notices.append(message)
        self.status_panel.request_render()
        if not self.status_panel.enabled:
            self.emit_line(message)

    def request_cancel(self, source: str, *, force: bool = False) -> None:
        if force:
            self.force_cancel_event.set()
        first_request = not self.cancel_event.is_set()
        self.cancel_event.set()
        self.cancel_reason = f"build cancelled by {source}"
        if first_request:
            self._notify_notice(f"Interrupt received ({source}); stopping running jobs...")
            self._signal_managed_processes(signal.SIGINT)
            return
        if force:
            self._notify_notice(f"Second interrupt ({source}); force stopping running jobs...")
            self._signal_managed_processes(signal.SIGKILL)

    def _handle_signal(self, signum: int, _frame: object | None) -> None:
        signal_name = self._signal_name(signum)
        if not self.cancel_event.is_set():
            self.request_cancel(signal_name)
            return
        self.request_cancel(signal_name, force=True)

    def _install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            if sig in self._saved_signal_handlers:
                continue
            self._saved_signal_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, self._handle_signal)

    def _restore_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig, handler in self._saved_signal_handlers.items():
            signal.signal(sig, handler)
        self._saved_signal_handlers.clear()

    def _register_process(self, process: subprocess.Popen[str], job_key: str | None) -> None:
        with self.status_lock:
            self.managed_processes[process.pid] = ManagedProcess(job_key=job_key, process=process)
        if self.force_cancel_event.is_set():
            self._signal_process_group(process, signal.SIGKILL)
        elif self.cancel_event.is_set():
            self._signal_process_group(process, signal.SIGINT)

    def _unregister_process(self, process: subprocess.Popen[str]) -> None:
        with self.status_lock:
            self.managed_processes.pop(process.pid, None)

    def _signal_managed_processes(self, sig: signal.Signals) -> None:
        with self.status_lock:
            processes = [managed.process for managed in self.managed_processes.values()]
        for process in processes:
            self._signal_process_group(process, sig)

    def require_tool(self, name: str) -> str:
        path = shutil.which(name)
        if not path:
            raise SystemExit(f"Required tool not found: {name}")
        return path

    def system_path(self) -> str:
        if self.osname == "FreeBSD":
            return f"{SYSTEM_PATH}:{FREEBSD_LOCAL_PATH}"
        return SYSTEM_PATH

    def is_gnu_m4(self, path: str) -> bool:
        try:
            result = subprocess.run(
                [path, "--version"],
                cwd=str(self.root),
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        return "gnu m4" in result.stdout.lower()

    def require_gnu_m4(self) -> str:
        candidates: list[str] = []
        env_m4 = os.environ.get("M4")
        if env_m4:
            candidates.append(env_m4)

        if self.osname == "Darwin":
            candidates.extend(
                [
                    "/opt/homebrew/opt/m4/bin/m4",
                    "/usr/local/opt/m4/bin/m4",
                    "gm4",
                    "m4",
                ]
            )
        elif self.osname == "FreeBSD":
            candidates.extend(
                [
                    "gm4",
                    "/usr/local/bin/gm4",
                    "m4",
                ]
            )
        else:
            candidates.extend(["m4", "gm4"])

        seen: set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            path = shutil.which(candidate)
            if path and self.is_gnu_m4(path):
                return path

        raise SystemExit(
            "Required GNU m4 1.4 or later not found. Install GNU m4 "
            "(FreeBSD: pkg install m4; macOS: brew install m4) or set M4=/path/to/gm4."
        )

    def is_gnu_make(self, path: str) -> bool:
        try:
            result = subprocess.run(
                [path, "--version"],
                cwd=str(self.root),
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        return "gnu make" in result.stdout.lower()

    def require_gnu_make(self) -> str:
        candidates: list[str] = []
        env_make = os.environ.get("MAKE")
        if env_make:
            candidates.append(env_make)

        if self.osname == "FreeBSD":
            candidates.extend(
                [
                    "gmake",
                    "/usr/local/bin/gmake",
                    "make",
                ]
            )
        else:
            candidates.extend(["make", "gmake"])

        seen: set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            path = shutil.which(candidate)
            if path and self.is_gnu_make(path):
                return path

        raise SystemExit(
            "Required GNU make not found. Install GNU make "
            "(FreeBSD: pkg install gmake) or set MAKE=/path/to/gmake."
        )

    def pkg_prefix_text(self, prefix: str) -> str:
        # Preserve the shell-style "$PKGBUILDDIR/$PREFIX" spelling used by build.sh.
        # Some generated libtool scripts compare compiler paths textually.
        return f"{self.pkgbuilddir}/{prefix}"

    def pkg_prefix_join(self, prefix: str, relative: str) -> str:
        base = self.pkg_prefix_text(prefix)
        return f"{base.rstrip('/')}/{relative.lstrip('/')}"

    def base_environment(self) -> dict[str, str]:
        automake115_bin = self.builddir / "automake-115/bin"
        autoconf269_bin = self.builddir / "autoconf-269/bin"

        if self.osname == "Darwin":
            env = {
                "TCLSH": "/opt/homebrew/opt/tcl-tk/bin/tclsh",
                "M4": self.require_gnu_m4(),
                "MAKE": self.require_gnu_make(),
                "ACLOCAL_1_15_HOST": str(automake115_bin / "aclocal"),
                "AUTOMAKE_1_15_HOST": str(automake115_bin / "automake"),
                "AUTOCONF_2_69_HOST": str(autoconf269_bin / "autoconf"),
                "AUTORECONF_2_69_HOST": str(autoconf269_bin / "autoreconf"),
            }
        else:
            env = {
                "TCLSH": self.require_tool("tclsh"),
                "M4": self.require_gnu_m4(),
                "MAKE": self.require_gnu_make(),
                "ACLOCAL_1_15_HOST": str(automake115_bin / "aclocal"),
                "AUTOMAKE_1_15_HOST": str(automake115_bin / "automake"),
                "AUTOCONF_2_69_HOST": str(autoconf269_bin / "autoconf"),
                "AUTORECONF_2_69_HOST": str(autoconf269_bin / "autoreconf"),
            }

        env.update(
            {
                "ACLOCAL_HOST": self.require_tool("aclocal"),
                "AUTOMAKE_HOST": self.require_tool("automake"),
                "AUTOCONF_HOST": self.require_tool("autoconf"),
                "AUTORECONF_HOST": self.require_tool("autoreconf"),
                "AUTOHEADER_HOST": self.require_tool("autoheader"),
            }
        )
        return env

    def load_versions(self) -> dict[str, str]:
        cfg = self.root / "versions.cfg"
        command = [
            self.bash,
            "-lc",
            f"set -a; source {shlex.quote(str(cfg))}; env -0",
        ]
        result = subprocess.run(
            command,
            cwd=str(self.root),
            env=os.environ.copy(),
            capture_output=True,
            check=True,
        )

        loaded: dict[str, str] = {}
        for entry in result.stdout.split(b"\0"):
            if not entry:
                continue
            key, _, value = entry.partition(b"=")
            key_text = key.decode()
            if key_text == "GNU_MIRROR" or key_text.endswith(("_VERSION", "_URL", "_SHA256")):
                loaded[key_text] = value.decode()
        return loaded

    def source_archives(self) -> list[SourceArchive]:
        versions = self.env
        return [
            SourceArchive(f"pkgconf-{versions['PKGCONF_VERSION']}.tar.xz", "PKGCONF_URL", "PKGCONF_SHA256", f"pkgconf-{versions['PKGCONF_VERSION']}", "pkgconf-src"),
            SourceArchive(f"gettext-{versions['GETTEXT_VERSION']}.tar.xz", "GETTEXT_URL", "GETTEXT_SHA256", f"gettext-{versions['GETTEXT_VERSION']}", "gettext-src"),
            SourceArchive(f"autoconf-{versions['AUTOCONF269_VERSION']}.tar.xz", "AUTOCONF269_URL", "AUTOCONF269_SHA256", f"autoconf-{versions['AUTOCONF269_VERSION']}", "autoconf269-src"),
            SourceArchive(f"automake-{versions['AUTOMAKE115_VERSION']}.tar.xz", "AUTOMAKE115_URL", "AUTOMAKE115_SHA256", f"automake-{versions['AUTOMAKE115_VERSION']}", "automake115-src"),
            SourceArchive(f"gmp-{versions['GMP_VERSION']}.tar.xz", "GMP_URL", "GMP_SHA256", f"gmp-{versions['GMP_VERSION']}", "gmp-src"),
            SourceArchive(f"mpfr-{versions['MPFR_VERSION']}.tar.xz", "MPFR_URL", "MPFR_SHA256", f"mpfr-{versions['MPFR_VERSION']}", "mpfr-src"),
            SourceArchive(f"mpc-{versions['MPC_VERSION']}.tar.gz", "MPC_URL", "MPC_SHA256", f"mpc-{versions['MPC_VERSION']}", "mpc-src"),
            SourceArchive(f"isl-{versions['ISL_VERSION']}.tar.gz", "ISL_URL", "ISL_SHA256", f"isl-{versions['ISL_VERSION']}", "isl-src"),
            SourceArchive(f"nettle-{versions['NETTLE_VERSION']}.tar.gz", "NETTLE_URL", "NETTLE_SHA256", f"nettle-{versions['NETTLE_VERSION']}", "nettle-src"),
            SourceArchive(f"libsodium-{versions['LIBSODIUM_VERSION']}.tar.gz", "LIBSODIUM_URL", "LIBSODIUM_SHA256", f"libsodium-{versions['LIBSODIUM_VERSION']}", "libsodium-src"),
            SourceArchive(f"libffi-{versions['LIBFFI_VERSION']}.tar.gz", "LIBFFI_URL", "LIBFFI_SHA256", f"libffi-{versions['LIBFFI_VERSION']}", "libffi-src"),
            SourceArchive(f"libuv-v{versions['LIBUV_VERSION']}.tar.gz", "LIBUV_URL", "LIBUV_SHA256", f"libuv-v{versions['LIBUV_VERSION']}", "libuv-src"),
            SourceArchive(f"libxml2-{versions['LIBXML2_VERSION']}.tar.xz", "LIBXML2_URL", "LIBXML2_SHA256", f"libxml2-{versions['LIBXML2_VERSION']}", "libxml2-src"),
            SourceArchive(f"libxslt-{versions['LIBXSLT_VERSION']}.tar.xz", "LIBXSLT_URL", "LIBXSLT_SHA256", f"libxslt-{versions['LIBXSLT_VERSION']}", "libxslt-src"),
            SourceArchive(f"expat-{versions['LIBEXPAT_VERSION']}.tar.xz", "LIBEXPAT_URL", "LIBEXPAT_SHA256", f"expat-{versions['LIBEXPAT_VERSION']}", "libexpat-src"),
            SourceArchive(f"yyjson-{versions['YYJSON_VERSION']}.tar.gz", "YYJSON_URL", "YYJSON_SHA256", f"yyjson-{versions['YYJSON_VERSION']}", "yyjson-src"),
            SourceArchive(f"zlib-{versions['ZLIB_VERSION']}.tar.gz", "ZLIB_URL", "ZLIB_SHA256", f"zlib-{versions['ZLIB_VERSION']}", "zlib-src"),
            SourceArchive(f"bzip2-{versions['BZIP2_VERSION']}.tar.gz", "BZIP2_URL", "BZIP2_SHA256", f"bzip2-{versions['BZIP2_VERSION']}", "bzip2-src"),
            SourceArchive(f"xz-{versions['XZ_VERSION']}.tar.gz", "XZ_URL", "XZ_SHA256", f"xz-{versions['XZ_VERSION']}", "xz-src"),
            SourceArchive(f"lz4-{versions['LZ4_VERSION']}.tar.gz", "LZ4_URL", "LZ4_SHA256", f"lz4-{versions['LZ4_VERSION']}", "lz4-src"),
            SourceArchive(f"zstd-{versions['ZSTD_VERSION']}.tar.gz", "ZSTD_URL", "ZSTD_SHA256", f"zstd-{versions['ZSTD_VERSION']}", "zstd-src"),
            SourceArchive(f"libarchive-{versions['LIBARCHIVE_VERSION']}.tar.gz", "LIBARCHIVE_URL", "LIBARCHIVE_SHA256", f"libarchive-{versions['LIBARCHIVE_VERSION']}", "libarchive-src"),
            SourceArchive(f"libiconv-{versions['LIBICONV_VERSION']}.tar.gz", "LIBICONV_URL", "LIBICONV_SHA256", f"libiconv-{versions['LIBICONV_VERSION']}", "libiconv-src"),
            SourceArchive(f"ncurses-{versions['NCURSES_VERSION']}.tar.gz", "NCURSES_URL", "NCURSES_SHA256", f"ncurses-{versions['NCURSES_VERSION']}", "ncurses-src"),
            SourceArchive(f"editline-{versions['EDITLINE_VERSION']}.tar.gz", "EDITLINE_URL", "EDITLINE_SHA256", f"editline-{versions['EDITLINE_VERSION']}", "editline-src"),
            SourceArchive(f"readline-{versions['READLINE_VERSION']}.tar.gz", "READLINE_URL", "READLINE_SHA256", f"readline-{versions['READLINE_VERSION']}", "readline-src"),
            SourceArchive(f"sqlite-autoconf-{versions['SQLITE3_VERSION']}.tar.gz", "SQLITE3_URL", "SQLITE3_SHA256", None, "sqlite3-src"),
        ]

    def source_archive_path(self, archive: SourceArchive) -> Path:
        return self.builddir / archive.output_name

    def file_sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def ensure_gnulib_checkout(self) -> None:
        gnulib_dir = self.builddir / "gnulib"
        git_dir = gnulib_dir / ".git"

        if self.dry_run:
            if git_dir.is_dir():
                self.dry_run_log(f"reuse {gnulib_dir}")
            else:
                self.dry_run_log(f"would clone gnulib into {gnulib_dir}")
            return

        self.builddir.mkdir(parents=True, exist_ok=True)
        if git_dir.is_dir():
            return

        if gnulib_dir.exists():
            shutil.rmtree(gnulib_dir, ignore_errors=True)

        self.run(["git", "clone", "https://git.savannah.gnu.org/git/gnulib.git", "--depth", "1"], cwd=self.builddir)

    def sync_submodules(self) -> None:
        if not (self.root / ".gitmodules").is_file():
            if self.dry_run:
                self.dry_run_log("skip submodule sync (no .gitmodules)")
            return

        if self.dry_run:
            self.dry_run_log("would initialize missing submodules only (preserve initialized submodule HEADs)")
            return

        status_text = self.capture(["git", "submodule", "status"], cwd=self.root)
        if not status_text:
            return

        missing_paths: list[str] = []
        for raw_line in status_text.splitlines():
            line = raw_line.strip()
            if not line or line[0] != "-":
                continue

            # git submodule status format:
            #   <prefix><sha1> <path> (<describe>)
            parts = line[1:].strip().split()
            if len(parts) < 2:
                continue
            missing_paths.append(parts[1])

        if not missing_paths:
            self.log_line("submodules already initialized; preserving local submodule HEADs")
            return

        self.run(
            [
                "git",
                "submodule",
                "update",
                "--init",
                "--jobs",
                str(max(1, self.parallel)),
                "--",
                *missing_paths,
            ],
            cwd=self.root,
        )

    def download_source_archive(self, archive: SourceArchive) -> None:
        self.raise_if_cancelled()

        archive_path = self.source_archive_path(archive)
        expected_hash = self.env.get(archive.sha256_key, "").strip().lower()
        if not expected_hash:
            raise RuntimeError(f"Missing {archive.sha256_key} for {archive.output_name} in versions.cfg")

        current_hash = self.file_sha256(archive_path) if archive_path.is_file() else None
        if current_hash is not None and current_hash == expected_hash:
            self.log_line(f"verified {archive.output_name} [{current_hash[:12]}]")
            return

        if current_hash is None:
            self.log_line(f"missing {archive.output_name}; downloading")
        else:
            self.log_line(
                f"hash mismatch for {archive.output_name}: expected {expected_hash[:12]}, got {current_hash[:12]}; re-downloading"
            )

        temp_name = f".{archive.output_name}.part"
        if self.dry_run:
            self.run(
                [
                    "curl",
                    "--fail",
                    "--retry",
                    "5",
                    "--retry-delay",
                    "2",
                    "-L",
                    "-o",
                    temp_name,
                    self.env[archive.url_key],
                ],
                cwd=self.builddir,
            )
            self.dry_run_log(f"would verify sha256 for {archive.output_name} [{expected_hash[:12]}]")
            return

        self.builddir.mkdir(parents=True, exist_ok=True)
        temp_path = self.builddir / temp_name
        if temp_path.exists():
            temp_path.unlink()

        self.run(
            [
                "curl",
                "--fail",
                "--retry",
                "5",
                "--retry-delay",
                "2",
                "-L",
                "-o",
                temp_name,
                self.env[archive.url_key],
            ],
            cwd=self.builddir,
        )

        downloaded_hash = self.file_sha256(temp_path)
        if downloaded_hash != expected_hash:
            temp_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"sha256 mismatch for {archive.output_name}: expected {expected_hash}, got {downloaded_hash}"
            )
        temp_path.replace(archive_path)
        self.log_line(f"downloaded {archive.output_name} [{downloaded_hash[:12]}]")

    def pkg_prefix_path(self, prefix: str) -> Path:
        return self.pkgbuilddir / prefix.lstrip("/")

    def stamp_path(self, name: str) -> Path:
        return self.builddir / f".{name}.stamp"

    def resolve_rerun_steps(self, graphs: list[StepGraph]) -> None:
        if not self.rerun_step_selectors:
            return

        alias_map: dict[str, list[tuple[str, str]]] = {}
        for graph in graphs:
            for step in graph.steps.values():
                if not isinstance(step, StampedStep):
                    continue

                target = (graph.label, step.name)
                for alias in {
                    step.name,
                    f"{graph.label}:{step.name}",
                }:
                    alias_map.setdefault(alias, []).append(target)

        resolved: set[str] = set()
        errors: list[str] = []

        for selector in self.rerun_step_selectors:
            matches = list(dict.fromkeys(alias_map.get(selector, [])))
            if not matches:
                errors.append(
                    f"Unknown --rerun-step target {selector!r}. Use a step name or graph:step."
                )
                continue

            if len(matches) > 1:
                rendered = ", ".join(
                    f"{graph_label}:{step_name}"
                    for graph_label, step_name in matches
                )
                errors.append(
                    f"Ambiguous --rerun-step target {selector!r}: {rendered}. Use graph:step."
                )
                continue

            graph_label, step_name = matches[0]
            resolved.add(f"{graph_label}:{step_name}")

        if errors:
            raise SystemExit("\n".join(errors))

        self.rerun_job_keys = resolved

    def should_rerun_current_step(self) -> bool:
        job_key = self.current_job_key()
        return job_key is not None and job_key in self.rerun_job_keys

    def start_section(self, title: str) -> None:
        if self.status_panel.enabled:
            return
        if self.dry_run:
            title = f"[dry-run] {title}"
        if self.env.get("GITHUB_ACTIONS", "false") == "true":
            self.emit_line(f"::group::{title}")
        else:
            self.emit_line(f"=== {title} ===")

    def end_section(self) -> None:
        if self.status_panel.enabled:
            return
        if self.env.get("GITHUB_ACTIONS", "false") == "true":
            self.emit_line("::endgroup::")

    def dry_run_log(self, message: str) -> None:
        self.log_line(f"[dry-run] {message}")

    def describe_error(self, error: BaseException) -> str:
        if isinstance(error, subprocess.CalledProcessError):
            if isinstance(error.cmd, list):
                parts = [str(part) for part in error.cmd]
                shell_name = Path(parts[0]).name if parts else ""
                if len(parts) >= 2 and shell_name in {"bash", "sh", "zsh"} and parts[1] == "-lc":
                    command = shell_name
                else:
                    shown = parts[:3]
                    command = shlex.join(shown)
                    if len(parts) > len(shown):
                        command += " ..."
            else:
                command = str(error.cmd)
            return f"exit {error.returncode}: {command}"
        return str(error) or error.__class__.__name__

    def format_command(self, args: list[str], cwd: Path | None = None, stdin_path: Path | None = None) -> str:
        command = shlex.join(args)
        location = cwd or self.root
        rendered = f"cd {shlex.quote(str(location))} && {command}"
        if stdin_path is not None:
            rendered = f"{rendered} < {shlex.quote(str(stdin_path))}"
        return rendered

    def merged_env(self, extra: dict[str, str | None] | None = None) -> dict[str, str]:
        env = self.env.copy()
        if extra:
            for key, value in extra.items():
                if value is None:
                    env.pop(key, None)
                else:
                    env[key] = value
        return env

    def run(
        self,
        args: list[str],
        cwd: Path | None = None,
        env: dict[str, str | None] | None = None,
        stdin_path: Path | None = None,
    ) -> None:
        self.raise_if_cancelled()
        if self.dry_run:
            self.dry_run_log(self.format_command(args, cwd=cwd, stdin_path=stdin_path))
            return

        merged = self.merged_env(env)
        if self.current_job_key() is not None:
            if self.direct_output:
                self._run_passthrough(args, cwd=cwd, env=merged, stdin_path=stdin_path)
                return
            self._run_streaming(args, cwd=cwd, env=merged, stdin_path=stdin_path)
            return

        stdin = None
        try:
            if stdin_path is not None:
                stdin = stdin_path.open("rb")
            subprocess.run(
                args,
                cwd=str(cwd or self.root),
                env=merged,
                stdin=stdin,
                check=True,
            )
        finally:
            if stdin is not None:
                stdin.close()

    def capture(self, args: list[str], cwd: Path | None = None, env: dict[str, str | None] | None = None) -> str:
        self.raise_if_cancelled()
        merged = self.merged_env(env)
        result = subprocess.run(
            args,
            cwd=str(cwd or self.root),
            env=merged,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def run_script(self, script: str, cwd: Path | None = None, env: dict[str, str | None] | None = None) -> None:
        self.raise_if_cancelled()
        if self.dry_run:
            body = textwrap.indent(textwrap.dedent(script).strip(), "    ")
            self.dry_run_log(f"script in {cwd or self.root}:\n{body}")
            return

        merged = self.merged_env(env)
        if self.current_job_key() is not None:
            if self.direct_output:
                self._run_passthrough([self.bash, "-lc", script_body(script)], cwd=cwd, env=merged)
                return
            self._run_streaming([self.bash, "-lc", script_body(script)], cwd=cwd, env=merged)
            return

        subprocess.run(
            [self.bash, "-lc", script_body(script)],
            cwd=str(cwd or self.root),
            env=merged,
            check=True,
        )

    def _run_streaming(
        self,
        args: list[str],
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        stdin_path: Path | None = None,
    ) -> None:
        stdin = None
        process: subprocess.Popen[str] | None = None
        try:
            if stdin_path is not None:
                stdin = stdin_path.open("rb")
            process = subprocess.Popen(
                args,
                cwd=str(cwd or self.root),
                env=env,
                stdin=stdin,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            self._register_process(process, self.current_job_key())
            assert process.stdout is not None
            terminated_for_restart = False
            terminate_started_at: float | None = None
            restart_target: int | None = None
            cancel_started_at: float | None = None
            cancel_stage = 0

            while True:
                control = self.current_job_control()
                if self.cancelled():
                    if self.force_cancel_event.is_set():
                        if cancel_stage < 3:
                            self._signal_process_group(process, signal.SIGKILL)
                            cancel_stage = 3
                    else:
                        if cancel_stage == 0:
                            self._signal_process_group(process, signal.SIGINT)
                            cancel_started_at = time.monotonic()
                            cancel_stage = 1
                        elif cancel_started_at is not None and cancel_stage == 1 and time.monotonic() - cancel_started_at >= 3:
                            self._signal_process_group(process, signal.SIGTERM)
                            cancel_stage = 2
                        elif cancel_started_at is not None and cancel_stage == 2 and time.monotonic() - cancel_started_at >= 8:
                            self._signal_process_group(process, signal.SIGKILL)
                            cancel_stage = 3
                else:
                    requested_slots = control.restart_requested if control is not None else None
                    if requested_slots is not None:
                        restart_target = requested_slots
                        if terminate_started_at is None:
                            self._signal_process_group(process, signal.SIGTERM)
                            terminate_started_at = time.monotonic()
                            terminated_for_restart = True
                        elif time.monotonic() - terminate_started_at >= 5:
                            self._signal_process_group(process, signal.SIGKILL)

                ready, _, _ = select.select([process.stdout], [], [], 0.25)
                if ready:
                    line = process.stdout.readline()
                    if line:
                        self._emit_stream_output(line)
                        continue

                if process.poll() is not None:
                    break

            remainder = process.stdout.read()
            if remainder:
                self._emit_stream_output(remainder)
            returncode = process.wait()
            if self.cancelled():
                raise self.cancellation_error()
            if terminated_for_restart:
                if control is not None:
                    control.restart_requested = None
                raise StepRestart(restart_target or 1)
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, args)
        finally:
            if process is not None:
                self._unregister_process(process)
            if process is not None and process.stdout is not None:
                process.stdout.close()
            if stdin is not None:
                stdin.close()

    def _run_passthrough(
        self,
        args: list[str],
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        stdin_path: Path | None = None,
    ) -> None:
        stdin = None
        process: subprocess.Popen[str] | None = None
        try:
            if stdin_path is not None:
                stdin = stdin_path.open("rb")
            process = subprocess.Popen(
                args,
                cwd=str(cwd or self.root),
                env=env,
                stdin=stdin,
                start_new_session=True,
            )
            self._register_process(process, self.current_job_key())
            terminated_for_restart = False
            terminate_started_at: float | None = None
            restart_target: int | None = None
            cancel_started_at: float | None = None
            cancel_stage = 0

            while True:
                control = self.current_job_control()
                if self.cancelled():
                    if self.force_cancel_event.is_set():
                        if cancel_stage < 3:
                            self._signal_process_group(process, signal.SIGKILL)
                            cancel_stage = 3
                    else:
                        if cancel_stage == 0:
                            self._signal_process_group(process, signal.SIGINT)
                            cancel_started_at = time.monotonic()
                            cancel_stage = 1
                        elif cancel_started_at is not None and cancel_stage == 1 and time.monotonic() - cancel_started_at >= 3:
                            self._signal_process_group(process, signal.SIGTERM)
                            cancel_stage = 2
                        elif cancel_started_at is not None and cancel_stage == 2 and time.monotonic() - cancel_started_at >= 8:
                            self._signal_process_group(process, signal.SIGKILL)
                            cancel_stage = 3
                else:
                    requested_slots = control.restart_requested if control is not None else None
                    if requested_slots is not None:
                        restart_target = requested_slots
                        if terminate_started_at is None:
                            self._signal_process_group(process, signal.SIGTERM)
                            terminate_started_at = time.monotonic()
                            terminated_for_restart = True
                        elif time.monotonic() - terminate_started_at >= 5:
                            self._signal_process_group(process, signal.SIGKILL)

                if process.poll() is not None:
                    break
                time.sleep(0.25)

            returncode = process.wait()
            if self.cancelled():
                raise self.cancellation_error()
            if terminated_for_restart:
                if control is not None:
                    control.restart_requested = None
                raise StepRestart(restart_target or 1)
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, args)
        finally:
            if process is not None:
                self._unregister_process(process)
            if stdin is not None:
                stdin.close()

    def _emit_stream_output(self, text: str) -> None:
        job_key = self.current_job_key()
        if self.status_panel.enabled and job_key is not None:
            self.append_job_output(job_key, text.rstrip("\n"))
            return
        self.emit_text(text)

    def _signal_process_group(self, process: subprocess.Popen[str], sig: signal.Signals) -> None:
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            return

    def build_subdir(self, name: str) -> Path:
        path = self.builddir / name
        if not self.dry_run:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def write_graph(self, graph: StepGraph, final_steps: list[str] | None = None) -> None:
        graph_dir = self.builddir / "graphs"
        graph_path = graph_dir / f"{graph.label}.dot"
        if self.dry_run:
            self.dry_run_log(f"would write graph to {graph_path}")
            return

        graph_dir.mkdir(parents=True, exist_ok=True)
        graph_path.write_text(graph.to_dot(final_steps), encoding="utf-8")

    def run_graph(self, graph: StepGraph, final_steps: list[str] | None = None) -> None:
        self.write_graph(graph, final_steps)
        graph.run(self, final_steps)

    def begin_graph(self, label: str, total: int) -> None:
        with self.status_lock:
            self.graph_progress = GraphProgress(label=label, total=total, failed=False)
        self.status_panel.request_render()

    def update_graph_progress(
        self,
        label: str,
        *,
        total: int,
        completed: int,
        ready: int,
        running: int,
        used_slots: int,
    ) -> None:
        with self.status_lock:
            if self.graph_progress.label != label:
                self.graph_progress = GraphProgress(label=label, total=total)
            self.graph_progress.total = total
            self.graph_progress.completed = completed
            self.graph_progress.ready = ready
            self.graph_progress.running = running
            self.graph_progress.used_slots = used_slots
            self.graph_progress.failed = False
        self.status_panel.request_render()

    def end_graph(self, label: str, failed: bool = False) -> None:
        with self.status_lock:
            if self.graph_progress.label == label:
                if failed:
                    self.graph_progress.failed = True
                    self.graph_progress.running = 0
                else:
                    self.graph_progress = GraphProgress()
        self.status_panel.request_render()

    def job_started(self, graph_label: str, step: BuildStep, slots: int) -> None:
        self.failure_prompt = ""
        with self.status_lock:
            self.active_jobs[f"{graph_label}:{step.name}"] = ActiveJob(
                graph_label=graph_label,
                step_name=step.name,
                title=step.status_title(),
                slots=slots,
                slot_mode=step.slot_mode,
                started_at=time.monotonic(),
                status="running",
                lines=[],
            )
        self.status_panel.request_render()

    def job_finished(self, graph_label: str, step_name: str, error: BaseException | None = None) -> None:
        job_key = f"{graph_label}:{step_name}"
        with self.status_lock:
            job = self.active_jobs.pop(job_key, None)
            if error is not None and job is not None:
                if isinstance(error, BuildCancelled):
                    job.status = "cancelled"
                    self.notices.append(f"CANCELLED {job.title}: {error.reason}")
                else:
                    message = sanitize_terminal_line(self.describe_error(error))
                    job.status = "failed"
                    job.failure_message = message
                    self.failed_jobs[job_key] = job
                    self.notices.append(f"FAILED {job.title}: {message}")
        self.status_panel.request_render()

    def job_resized(self, graph_label: str, step_name: str, slots: int) -> None:
        job_key = f"{graph_label}:{step_name}"
        with self.status_lock:
            job = self.active_jobs.get(job_key)
            if job is not None:
                job.slots = slots
        self.status_panel.request_render()

    def note_job_restart(self, slots: int) -> None:
        message = f"\x1b[33mreschedule:\x1b[0m restarting with {slots} cpu"
        job_key = self.current_job_key()
        if self.status_panel.enabled and job_key is not None:
            self.append_job_output(job_key, message)
            return
        self.emit_line(message)

    def status_snapshot(self) -> dict[str, object]:
        with self.status_lock:
            return {
                "graph": GraphProgress(**self.graph_progress.__dict__),
                "jobs": [
                    ActiveJob(
                        graph_label=job.graph_label,
                        step_name=job.step_name,
                        title=job.title,
                        slots=job.slots,
                        slot_mode=job.slot_mode,
                        started_at=job.started_at,
                        status=job.status,
                        failure_message=job.failure_message,
                        lines=list(job.lines),
                    )
                    for job in self.active_jobs.values()
                ],
                "failed_jobs": [
                    ActiveJob(
                        graph_label=job.graph_label,
                        step_name=job.step_name,
                        title=job.title,
                        slots=job.slots,
                        slot_mode=job.slot_mode,
                        started_at=job.started_at,
                        status=job.status,
                        failure_message=job.failure_message,
                        lines=list(job.lines),
                    )
                    for job in self.failed_jobs.values()
                ],
                "notices": list(self.notices),
            }

    def download_sources(self) -> None:
        self.ensure_gnulib_checkout()
        for archive in self.source_archives():
            self.download_source_archive(archive)

    def extract_sources(self) -> None:
        if self.dry_run:
            for archive in self.source_archives():
                destination = self.builddir / archive.destination_dir
                self.dry_run_log(f"would extract {self.source_archive_path(archive)} -> {destination}")
            return

        for archive in self.source_archives():
            self.raise_if_cancelled()
            destination = self.builddir / archive.destination_dir
            shutil.rmtree(destination, ignore_errors=True)

            archive_path = self.source_archive_path(archive)
            with tarfile.open(archive_path) as tar:
                first_member = tar.getmembers()[0].name.split("/", 1)[0]
                tar.extractall(self.builddir)

            extracted_name = archive.extracted_dir or first_member
            extracted_path = self.builddir / extracted_name
            if extracted_path != destination:
                shutil.move(str(extracted_path), str(destination))

    def copy_gnu_config(self, target_dir: Path) -> None:
        if self.dry_run:
            self.dry_run_log(f"would copy gnu-config into {target_dir}")
            return
        shutil.copy2(self.root / "gnu-config-strata/config.sub", target_dir / "config.sub")
        shutil.copy2(self.root / "gnu-config-strata/config.guess", target_dir / "config.guess")

    def apply_patch(self, cwd: Path, patch_name: str) -> None:
        self.raise_if_cancelled()
        if self.dry_run:
            self.dry_run_log(f"would apply patch {self.root / patch_name} in {cwd}")
            return
        self.run(["patch", "-p1"], cwd=cwd, stdin_path=self.root / patch_name)

    def set_host_libtool_env(self) -> None:
        self.env["LIBTOOL"] = self.pkg_prefix_join(self.host_prefix, "bin/libtool")
        self.env["LIBTOOLIZE"] = self.pkg_prefix_join(self.host_prefix, "bin/libtoolize")

    def set_host_cmake_tools(self) -> None:
        host_bin = self.pkg_prefix_join(self.host_prefix, "bin")
        self.env["CMAKE"] = f"{host_bin}/cmake"
        self.env["CCMAKE"] = f"{host_bin}/ccmake"
        self.env["CTEST"] = f"{host_bin}/ctest"
        self.env["CPACK"] = f"{host_bin}/cpack"

    def capture_host_state(self) -> None:
        self.env["TIC"] = str(self.builddir / "ncurses/progs/tic")
        self.env["SIDLC"] = self.pkg_prefix_join(self.host_prefix, "bin/sidlc")
        self.env["SIDLC_LIBDIR"] = str(self.root / "sidlc")
        self.env["SMA"] = self.pkg_prefix_join(self.host_prefix, "bin/sma")
        self.env.pop("LIBTOOL", None)
        self.env.pop("LIBTOOLIZE", None)
        self.root_path = self.env["PATH"]
        self.root_cppflags = self.env["CPPFLAGS"]
        self.root_ldflags = self.env["LDFLAGS"]
        if self.osname == "Darwin":
            self.root_ldflags = f"{self.root_ldflags} -Wl,-framework,CoreFoundation"
        self.build_triplet = self.capture([str(self.root / "gnu-config-strata/config.guess")])

    def arch_environment(self, arch: str) -> dict[str, str | None]:
        target_triplet = f"{arch}-strata-folios"
        arch_prefix = f"{self.destination}/folisdk-{arch}"
        sysroot = f"{arch_prefix}/{target_triplet}/sysroot"
        build_triplet = self.build_triplet or self.capture(
            [str(self.root / "gnu-config-strata/config.guess")],
            env=self.env,
        )
        root_path = self.root_path or self.env["PATH"]
        root_cppflags = self.root_cppflags or self.env["CPPFLAGS"]
        root_ldflags = self.root_ldflags or self.env["LDFLAGS"]

        arch_env: dict[str, str | None] = {
            "ARCH": arch,
            "TARGET_TRIPLET": target_triplet,
            "ARCH_PREFIX": arch_prefix,
            "SYSROOT": sysroot,
            "BUILD_TRIPLET": build_triplet,
            "ROOT_PATH": root_path,
            "ROOT_CPPFLAGS": root_cppflags,
            "ROOT_LDFLAGS": root_ldflags,
            "PATH": f"{self.pkg_prefix_join(arch_prefix, 'bin')}:{root_path}",
            "PKG_CONFIG_PATH": "",
            "PKG_CONFIG_LIBDIR": f"{self.pkg_prefix_join(self.host_prefix, 'lib/pkgconfig')}:{self.pkg_prefix_join(self.host_prefix, 'share/pkgconfig')}",
            "PKG_CONFIG_SYSROOT_DIR": self.pkg_prefix_text(self.host_prefix),
            "CPPFLAGS": None,
            "LDFLAGS": None,
            "PKG_CONFIG_ALLOW_SYSTEM_CFLAGS": None,
            "PKG_CONFIG_ALLOW_SYSTEM_LIBS": None,
        }
        if not self.dry_run:
            self.pkg_prefix_path(sysroot).mkdir(parents=True, exist_ok=True)
        return arch_env

    def copy_gcc_stdint(self, state: ArchBuildState) -> None:
        if self.dry_run:
            self.dry_run_log(
                f"would install GCC stdint header for {state.arch} into {self.pkg_prefix_path(state.env['ARCH_PREFIX'])}"
            )
            return

        target_triplet = state.env["TARGET_TRIPLET"]
        arch_prefix = state.env["ARCH_PREFIX"]
        gcc_builtin_include = self.capture(
            [self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-gcc"), "-print-file-name=include"],
            env=state.env,
        )
        shutil.copy2(
            self.root / "gcc-strata/gcc/ginclude/stdint-gcc.h",
            Path(gcc_builtin_include) / "stdint.h",
        )
        shutil.copy2(
            self.root / "gcc-strata/gcc/ginclude/stdint-gcc.h",
            Path(gcc_builtin_include) / "stdint-gcc.h",
        )

    def set_arch_libtool_env(self, state: ArchBuildState) -> None:
        target_triplet = state.env["TARGET_TRIPLET"]
        arch_prefix = state.env["ARCH_PREFIX"]
        state.env["LIBTOOL"] = self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-libtool")
        state.env["LIBTOOLIZE"] = self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-libtoolize")

    def set_arch_compiler_env(self, state: ArchBuildState) -> None:
        target_triplet = state.env["TARGET_TRIPLET"]
        arch_prefix = state.env["ARCH_PREFIX"]
        sysroot = state.env["SYSROOT"]
        state.env.update(
            {
                "PKG_CONFIG_LIBDIR": f"{self.pkg_prefix_join(sysroot, 'usr/lib/pkgconfig')}:{self.pkg_prefix_join(sysroot, 'usr/share/pkgconfig')}",
                "PKG_CONFIG_SYSROOT_DIR": self.pkg_prefix_text(sysroot),
                "CC": self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-gcc"),
                "CXX": self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-gcc"),
                "AR": f"{self.pkg_prefix_join(arch_prefix, f'bin/{target_triplet}-ld')} -r -o",
                "AS": self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-as"),
                "OBJCOPY": self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-objcopy"),
                "LD": self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-ld"),
                "NM": self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-nm"),
                "STRIP": self.pkg_prefix_join(arch_prefix, f"bin/{target_triplet}-strip"),
                "RANLIB": "true",
                "CC_FOR_BUILD": "cc",
                "CXX_FOR_BUILD": "c++",
                "CPP_FOR_BUILD": "cc -E",
                "LD_FOR_BUILD": "ld",
                "AR_FOR_BUILD": "ar",
                "RANLIB_FOR_BUILD": "ranlib",
            }
        )

    def build(self) -> None:
        global_graph = create_global_prepare_graph(self)
        host_graph = create_host_graph(self)
        arch_builds: list[tuple[ArchBuildState, StepGraph]] = []
        for arch in self.archs:
            arch_state = ArchBuildState(arch=arch, env={})
            arch_builds.append((arch_state, create_arch_graph(self, arch_state, include_target_libs=not self.no_libs)))
        host_cleanup_graph = create_host_cleanup_graph(self)
        graphs = [global_graph, host_graph, *(graph for _, graph in arch_builds), host_cleanup_graph]

        self.resolve_rerun_steps(graphs)
        self._install_signal_handlers()
        self.status_panel.start()
        try:
            self.run_graph(global_graph)
            self.run_graph(host_graph)
            for arch_state, graph in arch_builds:
                arch_state.env = self.arch_environment(arch_state.arch)
                self.run_graph(graph)
            self.run_graph(host_cleanup_graph)
        except BuildCancelled:
            raise
        except BaseException:
            self.status_panel.hold_on_failure()
            raise
        finally:
            self.status_panel.stop()
            self._restore_signal_handlers()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="build.sh",
        description="Build foliSDK toolchains and host utilities.",
    )
    parser.add_argument("-a", "--arch", default="x86_64", help="Target architecture list, comma-separated.")
    parser.add_argument("-b", "--build-dir", help="Build directory path.")
    parser.add_argument("-d", "--destination", help="Install destination prefix used inside pkgroot.")
    parser.add_argument(
        "--builddir-layout",
        action="store_true",
        help="Place host prefixes and target sysroots directly under the build directory instead of build/pkgroot.",
    )
    parser.add_argument("-j", "--jobs", type=int, help="Parallel build job count.")
    parser.add_argument("-n", "--no-libs", action="store_true", help="Skip additional target library builds.")
    parser.add_argument(
        "--direct-output",
        action="store_true",
        help="Disable the virtual terminal UI and stream child process output directly to stdout/stderr.",
    )
    parser.add_argument(
        "--rerun-step",
        action="append",
        metavar="STEP",
        help="Ignore the existing stamp for one stamped step. Repeatable; accepts step or graph:step.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print scheduled work without executing build commands.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    context = BuildContext(parse_args(argv))
    try:
        context.build()
    except (BuildCancelled, KeyboardInterrupt):
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
