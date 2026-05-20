from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from pathlib import Path
import shutil
import threading
from typing import TYPE_CHECKING, Callable, Iterable


if TYPE_CHECKING:
    from build import BuildContext


EnvMap = dict[str, str | None]
EnvProvider = EnvMap | Callable[["BuildContext"], EnvMap] | None
PathProvider = Path | Callable[["BuildContext"], Path]
StepAction = Callable[["BuildContext"], None]


@dataclass
class ArchBuildState:
    arch: str
    env: EnvMap


@dataclass
class JobControl:
    allocated_slots: int
    restart_requested: int | None = None


class StepRestart(Exception):
    def __init__(self, slots: int) -> None:
        super().__init__(f"restart with {slots} slots")
        self.slots = slots


class BuildStep(ABC):
    def __init__(
        self,
        name: str,
        dependencies: Iterable[str] = (),
        slot_mode: str = "single",
        restartable: bool = False,
    ) -> None:
        self.name = name
        self.dependencies = tuple(dependencies)
        self.slot_mode = slot_mode
        self.restartable = restartable

    def required_slots(self, available_slots: int) -> int:
        if self.slot_mode == "build":
            return max(1, available_slots)
        return 1

    def status_title(self) -> str:
        return self.name

    @abstractmethod
    def run(self, ctx: "BuildContext", allocated_slots: int = 1) -> None:
        raise NotImplementedError


class StepGraph:
    def __init__(self, label: str, steps: Iterable[BuildStep] | None = None) -> None:
        self.label = label
        self.steps: dict[str, BuildStep] = {}
        self.order: list[str] = []
        for step in steps or []:
            self.add(step)

    def add(self, step: BuildStep) -> None:
        if isinstance(step, CompositeStep):
            for inner_step in step.steps:
                self.add(inner_step)
            return

        if step.name in self.steps:
            raise ValueError(f"Duplicate graph step: {step.name}")
        self.steps[step.name] = step
        self.order.append(step.name)

    def _reachable(self, final_steps: list[str] | None = None) -> list[str]:
        roots = final_steps or list(self.order)
        missing = [name for name in roots if name not in self.steps]
        if missing:
            raise KeyError(f"Unknown graph step(s): {', '.join(sorted(missing))}")

        visited: set[str] = set()
        stack = list(roots)
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            for dependency in self.steps[current].dependencies:
                if dependency not in self.steps:
                    raise KeyError(f"Unknown dependency {dependency!r} in graph {self.label}")
                stack.append(dependency)

        return [name for name in self.order if name in visited]

    def run(self, ctx: "BuildContext", final_steps: list[str] | None = None) -> None:
        from build import BuildCancelled

        reachable = self._reachable(final_steps)
        indegree = {name: 0 for name in reachable}
        reverse_edges: dict[str, list[str]] = {name: [] for name in reachable}

        for name in reachable:
            for dependency in self.steps[name].dependencies:
                if dependency not in indegree:
                    continue
                indegree[name] += 1
                reverse_edges[dependency].append(name)

        ready = deque([name for name in reachable if indegree[name] == 0])
        completed: list[str] = []
        running: dict[str, JobControl] = {}
        first_error: BaseException | None = None
        total_slots = max(1, ctx.parallel)
        used_slots = 0
        condition = threading.Condition()

        def publish_state() -> None:
            ctx.update_graph_progress(
                self.label,
                total=len(reachable),
                completed=len(completed),
                ready=len(ready),
                running=len(running),
                used_slots=used_slots,
            )

        def complete_step(name: str, error: BaseException | None) -> None:
            nonlocal used_slots, first_error
            with condition:
                control = running.pop(name, None)
                if control is not None:
                    used_slots -= control.allocated_slots
                ctx.job_finished(self.label, name, error)
                if error is not None and first_error is None:
                    first_error = error
                else:
                    completed.append(name)
                    for dependent in reverse_edges[name]:
                        indegree[dependent] -= 1
                        if indegree[dependent] == 0:
                            ready.append(dependent)
                publish_state()
                condition.notify_all()

        def start_step(name: str, allocated_slots: int) -> None:
            control = JobControl(allocated_slots=allocated_slots)
            running[name] = control
            ctx.job_started(self.label, self.steps[name], allocated_slots)
            publish_state()

            def runner() -> None:
                error: BaseException | None = None
                try:
                    ctx.bind_current_job(self.label, name, control)
                    self.steps[name].run(ctx, allocated_slots)
                except BaseException as exc:
                    error = exc
                finally:
                    ctx.clear_current_job()
                complete_step(name, error)

            threading.Thread(target=runner, name=f"step:{self.label}:{name}", daemon=False).start()

        def next_ready(match_slot_mode: str) -> str | None:
            for name in ready:
                if self.steps[name].slot_mode == match_slot_mode:
                    return name
            return None

        def maybe_restart_build() -> bool:
            nonlocal used_slots
            if ready:
                return False

            if any(self.steps[name].slot_mode == "single" for name in running):
                return False

            for name in reachable:
                control = running.get(name)
                if control is None:
                    continue
                step = self.steps[name]
                if step.slot_mode != "build" or not step.restartable or control.restart_requested is not None:
                    continue

                target_slots = total_slots - sum(
                    other_control.allocated_slots
                    for other_name, other_control in running.items()
                    if other_name != name
                )
                target_slots = max(1, target_slots)
                if target_slots <= control.allocated_slots:
                    continue

                used_slots += target_slots - control.allocated_slots
                control.allocated_slots = target_slots
                control.restart_requested = target_slots
                ctx.job_resized(self.label, name, target_slots)
                publish_state()
                return True

            return False

        ctx.begin_graph(self.label, len(reachable))
        publish_state()
        try:
            while len(completed) < len(reachable):
                with condition:
                    if ctx.cancelled():
                        if not running:
                            raise ctx.cancellation_error()
                        condition.wait(0.25)
                        continue

                    started_any = False
                    while first_error is None and not ctx.cancelled():
                        free_slots = total_slots - used_slots
                        if free_slots <= 0:
                            break

                        fixed_name = next_ready("single")
                        if fixed_name is not None:
                            ready.remove(fixed_name)
                            used_slots += 1
                            start_step(fixed_name, 1)
                            started_any = True
                            continue

                        build_name = next_ready("build")
                        if build_name is not None:
                            allocated_slots = self.steps[build_name].required_slots(free_slots)
                            ready.remove(build_name)
                            used_slots += allocated_slots
                            start_step(build_name, allocated_slots)
                            started_any = True
                        break

                    if ctx.cancelled():
                        if not running:
                            raise ctx.cancellation_error()
                        if not started_any:
                            condition.wait(0.25)
                        continue

                    if first_error is None:
                        started_any = maybe_restart_build() or started_any

                    if first_error is not None and not running:
                        if isinstance(first_error, BuildCancelled):
                            raise first_error
                        raise first_error

                    if len(completed) == len(reachable):
                        break

                    if not running and not ready:
                        remaining = sorted(set(reachable) - set(completed))
                        raise RuntimeError(
                            f"Cycle detected while executing graph {self.label}: {', '.join(remaining)}"
                        )

                    if not started_any:
                        condition.wait(0.25)
        finally:
            ctx.end_graph(self.label, failed=first_error is not None and not ctx.cancelled())

    def to_dot(self, final_steps: list[str] | None = None) -> str:
        reachable = self._reachable(final_steps)
        lines = [f'digraph "{self.label}" {{', "  rankdir=LR;"]

        for name in reachable:
            dependencies = [dependency for dependency in self.steps[name].dependencies if dependency in reachable]
            if not dependencies:
                lines.append(f'  "{name}";')
                continue

            for dependency in dependencies:
                lines.append(f'  "{dependency}" -> "{name}";')

        lines.append("}")
        return "\n".join(lines) + "\n"


class StampedStep(BuildStep, ABC):
    def __init__(
        self,
        name: str,
        title: str,
        dependencies: Iterable[str] = (),
        slot_mode: str = "single",
        restartable: bool = False,
    ) -> None:
        super().__init__(name, dependencies, slot_mode=slot_mode, restartable=restartable)
        self.title = title

    def run(self, ctx: "BuildContext", allocated_slots: int = 1) -> None:
        stamp_path = ctx.stamp_path(self.name)
        force_rerun = ctx.should_rerun_current_step()
        if stamp_path.exists() and not force_rerun:
            if ctx.dry_run:
                ctx.dry_run_log(f"skip {self.name} (stamp exists)")
            return
        if force_rerun and stamp_path.exists():
            ctx.dry_run_log(f"rerun {self.name} (ignoring existing stamp)")

        ctx.start_section(self.title)
        try:
            current_slots = allocated_slots
            while True:
                try:
                    self.execute(ctx, current_slots)
                    break
                except StepRestart as restart:
                    next_slots = max(1, restart.slots)
                    if next_slots == current_slots:
                        raise
                    ctx.note_job_restart(next_slots)
                    current_slots = next_slots
        finally:
            ctx.end_section()

        if not ctx.dry_run:
            stamp_path.touch()

    def status_title(self) -> str:
        return self.title

    @abstractmethod
    def execute(self, ctx: "BuildContext", allocated_slots: int) -> None:
        raise NotImplementedError


class ActionStep(StampedStep):
    def __init__(
        self,
        name: str,
        title: str,
        action: StepAction,
        dependencies: Iterable[str] = (),
        slot_mode: str = "single",
        restartable: bool = False,
    ) -> None:
        super().__init__(
            name=name,
            title=title,
            dependencies=dependencies,
            slot_mode=slot_mode,
            restartable=restartable,
        )
        self.action = action

    def execute(self, ctx: "BuildContext", allocated_slots: int) -> None:
        self.action(ctx)


class ScriptStep(StampedStep):
    def __init__(
        self,
        name: str,
        title: str,
        script: str,
        cwd: PathProvider,
        env: EnvProvider = None,
        dependencies: Iterable[str] = (),
        slot_mode: str = "single",
        restartable: bool = False,
    ) -> None:
        super().__init__(
            name=name,
            title=title,
            dependencies=dependencies,
            slot_mode=slot_mode,
            restartable=restartable,
        )
        self.script = script
        self.cwd_ref = cwd
        self.env_ref = env

    def _cwd(self, ctx: "BuildContext") -> Path:
        if callable(self.cwd_ref):
            return self.cwd_ref(ctx)
        return self.cwd_ref

    def _env(self, ctx: "BuildContext", allocated_slots: int) -> EnvMap | None:
        env: EnvMap = {"PARALLEL": str(max(1, allocated_slots))}
        if self.env_ref is None:
            return env
        if callable(self.env_ref):
            env.update(self.env_ref(ctx))
            return env
        env.update(self.env_ref)
        return env

    def execute(self, ctx: "BuildContext", allocated_slots: int) -> None:
        ctx.run_script(self.script, cwd=self._cwd(ctx), env=self._env(ctx, allocated_slots))


class RuntimeStep(BuildStep):
    def __init__(
        self,
        name: str,
        action: StepAction,
        title: str | None = None,
        dependencies: Iterable[str] = (),
        slot_mode: str = "single",
        restartable: bool = False,
    ) -> None:
        super().__init__(name, dependencies, slot_mode=slot_mode, restartable=restartable)
        self.action = action
        self.title = title

    def status_title(self) -> str:
        return self.title or self.name

    def run(self, ctx: "BuildContext", allocated_slots: int = 1) -> None:
        if self.title is not None:
            ctx.start_section(self.title)
            try:
                self.action(ctx)
            finally:
                ctx.end_section()
            return

        self.action(ctx)


class CompositeStep(BuildStep):
    def __init__(self, name: str, steps: Iterable[BuildStep], dependencies: Iterable[str] = ()) -> None:
        super().__init__(name, dependencies)
        self.steps = list(steps)

    def run(self, ctx: "BuildContext", allocated_slots: int = 1) -> None:
        for step in self.steps:
            step.run(ctx, allocated_slots)


def _context_env(ctx: "BuildContext") -> EnvMap:
    return ctx.env


def configure_step_name(name: str) -> str:
    return f"configure-{name}"


def build_step_name(name: str) -> str:
    return f"build-{name}"


def package_step(
    *,
    name: str,
    display_name: str,
    dependencies: Iterable[str],
    configure_script: str,
    build_script: str,
    cwd: PathProvider,
    env: EnvProvider = None,
) -> CompositeStep:
    configure_name = configure_step_name(name)
    build_name = build_step_name(name)
    return CompositeStep(
        name=build_name,
        steps=[
            ScriptStep(
                name=configure_name,
                title=f"Configure {display_name}",
                script=configure_script,
                cwd=cwd,
                env=env,
                dependencies=dependencies,
            ),
            ScriptStep(
                name=build_name,
                title=f"Build {display_name}",
                script=build_script,
                cwd=cwd,
                env=env,
                dependencies=(configure_name,),
                slot_mode="build",
                restartable=True,
            ),
        ],
    )


def create_global_prepare_graph(ctx: "BuildContext") -> StepGraph:
    def patch_gnulib(run_ctx: "BuildContext") -> None:
        run_ctx.copy_gnu_config(run_ctx.builddir / "gnulib/build-aux")

    def patch_gmp(run_ctx: "BuildContext") -> None:
        run_ctx.apply_patch(run_ctx.builddir / "gmp-src", f"patches/gmp-{run_ctx.env['GMP_VERSION']}.patch")
        run_ctx.run(
            [run_ctx.env["AUTORECONF_2_69_HOST"]],
            cwd=run_ctx.builddir / "gmp-src",
            env={
                "ACLOCAL": "true",
                "AUTOMAKE": run_ctx.env["AUTOMAKE_1_15_HOST"],
                "AUTOCONF": run_ctx.env["AUTOCONF_2_69_HOST"],
            },
        )
        run_ctx.copy_gnu_config(run_ctx.builddir / "gmp-src")

    def patch_nettle(run_ctx: "BuildContext") -> None:
        run_ctx.apply_patch(run_ctx.builddir / "nettle-src", f"patches/nettle-{run_ctx.env['NETTLE_VERSION']}.patch")
        run_ctx.run(
            [run_ctx.env["AUTORECONF_2_69_HOST"]],
            cwd=run_ctx.builddir / "nettle-src",
            env={
                "ACLOCAL": "true",
                "AUTOMAKE": run_ctx.env["AUTOMAKE_1_15_HOST"],
                "AUTOCONF": run_ctx.env["AUTOCONF_2_69_HOST"],
            },
        )
        run_ctx.copy_gnu_config(run_ctx.builddir / "nettle-src")

    def patch_zlib(run_ctx: "BuildContext") -> None:
        run_ctx.apply_patch(run_ctx.builddir / "zlib-src", f"patches/zlib-{run_ctx.env['ZLIB_VERSION']}.patch")

    def patch_bzip2(run_ctx: "BuildContext") -> None:
        run_ctx.apply_patch(run_ctx.builddir / "bzip2-src", f"patches/bzip2-{run_ctx.env['BZIP2_VERSION']}.patch")

    def patch_lz4(run_ctx: "BuildContext") -> None:
        run_ctx.apply_patch(run_ctx.builddir / "lz4-src", f"patches/lz4-{run_ctx.env['LZ4_VERSION']}.patch")

    def patch_zstd(run_ctx: "BuildContext") -> None:
        run_ctx.apply_patch(run_ctx.builddir / "zstd-src", f"patches/zstd-{run_ctx.env['ZSTD_VERSION']}.patch")

    def patch_libiconv(run_ctx: "BuildContext") -> None:
        run_ctx.copy_gnu_config(run_ctx.builddir / "libiconv-src/build-aux")
        run_ctx.copy_gnu_config(run_ctx.builddir / "libiconv-src/libcharset/build-aux")

    def patch_readline(run_ctx: "BuildContext") -> None:
        run_ctx.apply_patch(run_ctx.builddir / "readline-src", f"patches/readline-{run_ctx.env['READLINE_VERSION']}.patch")
        run_ctx.copy_gnu_config(run_ctx.builddir / "readline-src/support")

    def patch_sqlite3(run_ctx: "BuildContext") -> None:
        source_dir = run_ctx.builddir / "sqlite3-src/autosetup"
        if run_ctx.dry_run:
            run_ctx.dry_run_log(f"would copy sqlite autosetup config into {source_dir}")
            return
        shutil.copy2(run_ctx.root / "gnu-config-strata/config.sub", source_dir / "autosetup-config.sub")
        shutil.copy2(run_ctx.root / "gnu-config-strata/config.guess", source_dir / "autosetup-config.guess")

    steps: list[BuildStep] = [
        RuntimeStep(
            name="sync-submodules",
            title="Sync submodules",
            action=lambda run_ctx: run_ctx.sync_submodules(),
        ),
        RuntimeStep(
            name="download-gnulib",
            title="Download gnulib",
            action=lambda run_ctx: run_ctx.ensure_gnulib_checkout(),
        ),
    ]

    extract_dependencies: list[str] = []
    for archive in ctx.source_archives():
        source_name = archive.destination_dir.removesuffix("-src")
        step_name = f"download-{source_name}"
        steps.append(
            RuntimeStep(
                name=step_name,
                title=f"Download {source_name} source",
                action=lambda run_ctx, archive=archive: run_ctx.download_source_archive(archive),
            )
        )
        extract_dependencies.append(step_name)

    legacy_autotools_dependencies = (
        "set-host-libtool-tools",
        build_step_name("autoconf269"),
        build_step_name("automake115"),
    )

    steps.extend(
        [
            ActionStep(
                name="extract-sources",
                title="Extract sources",
                action=lambda run_ctx: run_ctx.extract_sources(),
                dependencies=tuple(extract_dependencies),
            ),
            package_step(
                name="autoconf269",
                display_name="autoconf 2.69",
                dependencies=("extract-sources",),
                cwd=lambda run_ctx: run_ctx.build_subdir("autoconf-269"),
                env=_context_env,
                configure_script="""
                M4="$M4" \
                ../autoconf269-src/configure \
                    --prefix="$BUILDDIR/autoconf-269"
                """,
                build_script="""
                make -j"$PARALLEL" M4="$M4"
                # The build directory itself is the configured prefix, so the
                # generated bin scripts are already in place. GNU install
                # errors out on self-copies here, while install-data still
                # populates the shared data files we actually need.
                make install-data M4="$M4"
                """,
            ),
            package_step(
                name="automake115",
                display_name="automake 1.15",
                dependencies=("extract-sources",),
                cwd=lambda run_ctx: run_ctx.build_subdir("automake-115"),
                env=_context_env,
                configure_script="""
                M4="$M4" \
                ../automake115-src/configure \
                    --prefix="$BUILDDIR/automake-115"
                """,
                build_script="""
                make -j"$PARALLEL" M4="$M4"
                # Same-prefix builds already have the generated executables in
                # bin/, so only install the data payload to avoid self-copy
                # failures from GNU install on Linux.
                make install-data M4="$M4"
                """,
            ),
            ScriptStep(
                name="preconfigure-libtool",
                title="Preconfigure libtool",
                dependencies=("sync-submodules", "download-gnulib", "extract-sources"),
                cwd=lambda run_ctx: run_ctx.root / "libtool-strata",
                env=_context_env,
                script="""
            git clean -fdX
            echo "2.5.4" > .tarball-version
            echo "2.5.4" > .version
            echo "4442" > .serial
            OLD_PATH="$PATH"
            if [ "$OSNAME" = "Darwin" ]; then
                export PATH="$PATH:/opt/homebrew/bin"
            fi
            ./bootstrap --gnulib-srcdir="$BUILDDIR/gnulib" --skip-git --verbose
            export PATH="$OLD_PATH"
            """,
            ),
            ScriptStep(
                name="configure-libtool",
                title="Configure libtool",
                dependencies=("preconfigure-libtool",),
                cwd=lambda run_ctx: run_ctx.build_subdir("libtool"),
                env=_context_env,
                script="""
            ../../libtool-strata/configure \
                --prefix="$PKGBUILDDIR/$HOST_PREFIX" \
                --enable-ltdl-install
            """,
            ),
            ScriptStep(
                name="build-libtool",
                title="Build libtool",
                dependencies=("configure-libtool",),
                cwd=lambda run_ctx: run_ctx.build_subdir("libtool"),
                env=_context_env,
                slot_mode="build",
                restartable=True,
                script="""
            OLD_PATH="$PATH"
            if [ "$OSNAME" = "Darwin" ]; then
                export PATH="$PATH:/opt/homebrew/bin"
            fi
            make -j"$PARALLEL"
            export PATH="$OLD_PATH"
            make install
            """,
            ),
            RuntimeStep(
                name="set-host-libtool-tools",
                dependencies=("build-libtool",),
                action=lambda run_ctx: run_ctx.set_host_libtool_env(),
            ),
            ActionStep(
                name="patch-gnulib",
                title="Patch gnulib",
                action=patch_gnulib,
                dependencies=("set-host-libtool-tools",),
            ),
            ActionStep(
                name="patch-gmp",
                title="Patch gmp",
                action=patch_gmp,
                dependencies=legacy_autotools_dependencies,
            ),
            ScriptStep(
                name="patch-mpfr",
                title="Patch mpfr",
                dependencies=("set-host-libtool-tools",),
                cwd=lambda run_ctx: run_ctx.builddir / "mpfr-src",
                env=_context_env,
                script="""
            "$LIBTOOLIZE" --force --copy
            ACLOCAL="$ACLOCAL_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_HOST" \
            AUTOCONF="$AUTOCONF_HOST" \
            "$AUTORECONF_HOST" -ivf
            cp "$ROOT/gnu-config-strata/config.sub" ./config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./config.guess
            """,
            ),
        ScriptStep(
            name="patch-mpc",
            title="Patch mpc",
            dependencies=legacy_autotools_dependencies,
            cwd=lambda run_ctx: run_ctx.builddir / "mpc-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            ACLOCAL="$ACLOCAL_1_15_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_1_15_HOST" \
            AUTOCONF="$AUTOCONF_2_69_HOST" \
            "$AUTORECONF_2_69_HOST" -ivf
            cp "$ROOT/gnu-config-strata/config.sub" ./build-aux/config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./build-aux/config.guess
            """,
        ),
        ActionStep(
            name="patch-nettle",
            title="Patch nettle",
            action=patch_nettle,
            dependencies=legacy_autotools_dependencies,
        ),
        ScriptStep(
            name="patch-libsodium",
            title="Patch libsodium",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "libsodium-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            ACLOCAL="$ACLOCAL_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_HOST" \
            AUTOCONF="$AUTOCONF_HOST" \
            "$AUTORECONF_HOST" -ivf
            cp "$ROOT/gnu-config-strata/config.sub" ./build-aux/config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./build-aux/config.guess
            """,
        ),
        ScriptStep(
            name="patch-libffi",
            title="Patch libffi",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "libffi-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            ACLOCAL="$ACLOCAL_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_HOST" \
            AUTOCONF="$AUTOCONF_HOST" \
            "$AUTORECONF_HOST" -ivf
            cp "$ROOT/gnu-config-strata/config.sub" ./config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./config.guess
            """,
        ),
        ScriptStep(
            name="patch-libuv",
            title="Patch libuv",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "libuv-src",
            env=_context_env,
            script="""
            OLD_PATH="$PATH"
            if [ "$OSNAME" = "Darwin" ]; then
                export PATH="/opt/homebrew/bin:$PATH"
            fi
            ./autogen.sh
            export PATH="$OLD_PATH"
            cp "$ROOT/gnu-config-strata/config.sub" ./config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./config.guess
            """,
        ),
        ScriptStep(
            name="patch-libxml2",
            title="Patch libxml2",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "libxml2-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            OLD_PATH="$PATH"
            if [ "$OSNAME" = "Darwin" ]; then
                export PATH="$PATH:/opt/homebrew/bin"
            fi
            ACLOCAL="$ACLOCAL_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_HOST" \
            AUTOCONF="$AUTOCONF_HOST" \
            AUTORECONF="$AUTORECONF_HOST" \
            NOCONFIGURE="true" \
            ./autogen.sh
            export PATH="$OLD_PATH"
            cp "$ROOT/gnu-config-strata/config.sub" ./config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./config.guess
            """,
        ),
        ScriptStep(
            name="patch-libxslt",
            title="Patch libxslt",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "libxslt-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            ACLOCAL="$ACLOCAL_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_HOST" \
            AUTOCONF="$AUTOCONF_HOST" \
            "$AUTORECONF_HOST" -ivf
            cp "$ROOT/gnu-config-strata/config.sub" ./config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./config.guess
            """,
        ),
        ScriptStep(
            name="patch-libexpat",
            title="Patch libexpat",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "libexpat-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            ACLOCAL="$ACLOCAL_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_HOST" \
            AUTOCONF="$AUTOCONF_HOST" \
            "$AUTORECONF_HOST" -ivf
            cp "$ROOT/gnu-config-strata/config.sub" ./conftools/config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./conftools/config.guess
            """,
        ),
        ActionStep(
            name="patch-zlib",
            title="Patch zlib",
            action=patch_zlib,
            dependencies=("set-host-libtool-tools",),
        ),
        ActionStep(
            name="patch-bzip2",
            title="Patch bzip2",
            action=patch_bzip2,
            dependencies=("set-host-libtool-tools",),
        ),
        ScriptStep(
            name="patch-xz",
            title="Patch xz",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "xz-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            OLD_PATH="$PATH"
            if [ "$OSNAME" = "Darwin" ]; then
                export PATH="$PATH:/opt/homebrew/bin"
            fi
            ACLOCAL="$ACLOCAL_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_HOST" \
            AUTOCONF="$AUTOCONF_HOST" \
            "$AUTORECONF_HOST" -ivf
            export PATH="$OLD_PATH"
            cp "$ROOT/gnu-config-strata/config.sub" ./build-aux/config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./build-aux/config.guess
            """,
        ),
        ActionStep(
            name="patch-lz4",
            title="Patch lz4",
            action=patch_lz4,
            dependencies=("set-host-libtool-tools",),
        ),
        ActionStep(
            name="patch-zstd",
            title="Patch zstd",
            action=patch_zstd,
            dependencies=("set-host-libtool-tools",),
        ),
        ScriptStep(
            name="patch-libarchive",
            title="Patch libarchive",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "libarchive-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            ACLOCAL="$ACLOCAL_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_HOST" \
            AUTOCONF="$AUTOCONF_HOST" \
            "$AUTORECONF_HOST" -ivf
            cp "$ROOT/gnu-config-strata/config.sub" ./build/autoconf/config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./build/autoconf/config.guess
            if [ "$SED_TYPE" = "bsd" ]; then
                sed -i '' \
                    's/hmac_sha1_digest(ctx, (unsigned)\\*out_len, out)/hmac_sha1_digest(ctx, out)/g' \
                    ./libarchive/archive_hmac.c
            else
                sed -i \
                    's/hmac_sha1_digest(ctx, (unsigned)\\*out_len, out)/hmac_sha1_digest(ctx, out)/g' \
                    ./libarchive/archive_hmac.c
            fi
            """,
        ),
        ActionStep(
            name="patch-libiconv",
            title="Patch libiconv",
            action=patch_libiconv,
            dependencies=("set-host-libtool-tools",),
        ),
        ScriptStep(
            name="patch-ncurses",
            title="Patch ncurses",
            dependencies=("set-host-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.builddir / "ncurses-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            patch -p1 < "$ROOT/patches/ncurses-$NCURSES_VERSION.patch"
            cp "$ROOT/gnu-config-strata/config.sub" ./config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./config.guess
            """,
        ),
        ScriptStep(
            name="patch-editline",
            title="Patch editline",
            dependencies=legacy_autotools_dependencies,
            cwd=lambda run_ctx: run_ctx.builddir / "editline-src",
            env=_context_env,
            script="""
            "$LIBTOOLIZE" --force --copy
            ACLOCAL="$ACLOCAL_1_15_HOST -I $PKGBUILDDIR/$HOST_PREFIX/share/aclocal" \
            AUTOMAKE="$AUTOMAKE_1_15_HOST" \
            AUTOCONF="$AUTOCONF_2_69_HOST" \
            "$AUTORECONF_2_69_HOST" -ivf
            cp "$ROOT/gnu-config-strata/config.sub" ./aux/config.sub
            cp "$ROOT/gnu-config-strata/config.guess" ./aux/config.guess
            """,
        ),
        ActionStep(
            name="patch-readline",
            title="Patch readline",
            action=patch_readline,
            dependencies=("set-host-libtool-tools",),
        ),
        ActionStep(
            name="patch-sqlite3",
            title="Patch sqlite3",
            action=patch_sqlite3,
            dependencies=("set-host-libtool-tools",),
        ),
    ]
    )

    return StepGraph("global-prepare", steps)


def create_host_graph(ctx: "BuildContext") -> StepGraph:
    steps: list[BuildStep] = [
        package_step(
            name="pkgconf",
            display_name="pkgconf",
            dependencies=(),
            cwd=lambda run_ctx: run_ctx.build_subdir("pkgconf"),
            env=_context_env,
            configure_script="""
            CFLAGS="-Dbool=gboolean -Wno-error=int-conversion" \
            CXXFLAGS="-Dbool=gboolean" \
            ../pkgconf-src/configure \
                --prefix="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-internal-glib \
                --disable-host-tool \
                --disable-debug
            """,
            build_script="""
            make -j"$PARALLEL"
            make install
            """,
        ),
        package_step(
            name="zlib",
            display_name="host zlib",
            dependencies=(),
            cwd=lambda run_ctx: run_ctx.build_subdir("zlib"),
            env=_context_env,
            configure_script="""
            ../zlib-src/configure --prefix="$HOST_PREFIX" --static
            """,
            build_script="""
            make -j"$PARALLEL"
            make install DESTDIR="$PKGBUILDDIR"
            """,
        ),
        package_step(
            name="ncurses",
            display_name="host ncurses",
            dependencies=(),
            cwd=lambda run_ctx: run_ctx.build_subdir("ncurses"),
            env=_context_env,
            configure_script="""
            CC="${HOST_CC:-cc}" \
            CFLAGS="-O2 -Wno-implicit-int -Wno-return-type" \
            ../ncurses-src/configure \
                --without-shared \
                --without-debug \
                --without-ada \
                --without-cxx \
                --without-manpages \
                --without-tests \
                --disable-mixed-case \
                --enable-widec
            """,
            build_script="""
            make -j"$PARALLEL" -C include
            make -j"$PARALLEL" -C ncurses
            make -j"$PARALLEL" -C progs tic
            """,
        ),
        package_step(
            name="cmake",
            display_name="cmake",
            dependencies=(build_step_name("zlib"), build_step_name("ncurses")),
            cwd=lambda run_ctx: run_ctx.build_subdir("cmake"),
            env=_context_env,
            configure_script="""
            ../../cmake-strata/bootstrap --prefix="$HOST_PREFIX" --parallel="$PARALLEL"
            """,
            build_script="""
            make -j"$PARALLEL"
            make install DESTDIR="$PKGBUILDDIR"
            """,
        ),
        RuntimeStep(
            name="set-host-cmake-tools",
            dependencies=(build_step_name("cmake"),),
            action=lambda run_ctx: run_ctx.set_host_cmake_tools(),
        ),
        package_step(
            name="sidlc",
            display_name="sidlc",
            dependencies=("set-host-cmake-tools",),
            cwd=lambda run_ctx: run_ctx.build_subdir("sidlc"),
            env=_context_env,
            configure_script="""
            "$CMAKE" -S../../sidlc -B. \
                -DCMAKE_INSTALL_PREFIX="$HOST_PREFIX"
            """,
            build_script="""
            "$CMAKE" --build . --parallel="$PARALLEL"
            DESTDIR="$PKGBUILDDIR" "$CMAKE" --install .
            """,
        ),
        package_step(
            name="sma",
            display_name="sma",
            dependencies=("set-host-cmake-tools",),
            cwd=lambda run_ctx: run_ctx.build_subdir("sma"),
            env=_context_env,
            configure_script="""
            "$CMAKE" -S../../sma -B. \
                -DCMAKE_INSTALL_PREFIX="$HOST_PREFIX"
            """,
            build_script="""
            "$CMAKE" --build . --parallel="$PARALLEL"
            DESTDIR="$PKGBUILDDIR" "$CMAKE" --install .
            """,
        ),
        package_step(
            name="libiconv",
            display_name="host libiconv",
            dependencies=(build_step_name("pkgconf"),),
            cwd=lambda run_ctx: run_ctx.build_subdir("libiconv"),
            env=_context_env,
            configure_script="""
            ../libiconv-src/configure \
                --prefix="$PKGBUILDDIR/$HOST_PREFIX" \
                --disable-shared \
                --enable-static
            """,
            build_script="""
            make -j"$PARALLEL"
            make install
            """,
        ),
        package_step(
            name="gettext",
            display_name="host gettext",
            dependencies=(build_step_name("libiconv"),),
            cwd=lambda run_ctx: run_ctx.build_subdir("gettext"),
            env=_context_env,
            configure_script="""
            ../gettext-src/configure \
                --prefix="$PKGBUILDDIR/$HOST_PREFIX" \
                --disable-shared \
                --enable-static \
                --with-libiconv-prefix="$PKGBUILDDIR/$HOST_PREFIX"
            """,
            build_script="""
            make -j"$PARALLEL"
            make install
            """,
        ),
    ]

    # isl defaults to --with-int=gmp, so it only needs GMP here.
    for name, dependencies in [
        ("gmp", ()),
        ("mpfr", ("gmp",)),
        ("mpc", ("gmp", "mpfr")),
        ("isl", ("gmp",)),
    ]:
        steps.append(
            package_step(
                name=name,
                display_name=f"host {name}",
                dependencies=tuple(build_step_name(dependency) for dependency in dependencies),
                cwd=lambda run_ctx, name=name: run_ctx.build_subdir(name),
                env=_context_env,
                configure_script=f"""
                CFLAGS="-std=gnu17" \
                CXXFLAGS="-std=gnu++17" \
                ../{name}-src/configure \
                    --prefix="$PKGBUILDDIR/$HOST_PREFIX" \
                    --disable-shared \
                    --enable-static
                """,
                build_script="""
                make -j"$PARALLEL"
                make install
                """,
            )
        )

    steps.append(
        RuntimeStep(
            name="capture-host-state",
            dependencies=(
                build_step_name("pkgconf"),
                build_step_name("zlib"),
                build_step_name("ncurses"),
                build_step_name("cmake"),
                build_step_name("sidlc"),
                build_step_name("sma"),
                build_step_name("libiconv"),
                build_step_name("gettext"),
                build_step_name("gmp"),
                build_step_name("mpfr"),
                build_step_name("mpc"),
                build_step_name("isl"),
            ),
            action=lambda run_ctx: run_ctx.capture_host_state(),
        )
    )

    return StepGraph("host-build-targets", steps)


def create_arch_graph(ctx: "BuildContext", state: ArchBuildState, include_target_libs: bool) -> StepGraph:
    arch = state.arch
    arch_suffix = arch

    def arch_env(_ctx: "BuildContext") -> EnvMap:
        return state.env

    def root_flags_env(run_ctx: "BuildContext") -> EnvMap:
        return {
            **state.env,
            "CFLAGS": run_ctx.root_cppflags,
            "CXXFLAGS": run_ctx.root_cppflags,
            "LDFLAGS": f"{run_ctx.root_ldflags} -s",
        }

    def arch_name(name: str) -> str:
        return f"{name}-{arch_suffix}"

    def arch_build(name: str) -> str:
        return build_step_name(arch_name(name))

    steps: list[BuildStep] = [
        package_step(
            name=arch_name("binutils"),
            display_name=f"binutils ({arch})",
            dependencies=(),
            cwd=lambda run_ctx: run_ctx.build_subdir(f"binutils-{arch}"),
            env=root_flags_env,
            configure_script="""
            ../../binutils-strata/configure \
                --build="$BUILD_TRIPLET" \
                --target="$TARGET_TRIPLET" \
                --prefix="$ARCH_PREFIX" \
                --with-gmp="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-mpfr="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-mpc="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-isl="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-libintl-prefix="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-build-sysroot="$PKGBUILDDIR/$SYSROOT" \
                --with-sysroot="$SYSROOT" \
                --with-system-zlib \
                --disable-werror \
                --enable-static \
                --enable-nls \
                --enable-lto \
                --enable-plugins \
                --enable-year2038 \
                --disable-gprofng \
                --enable-default-hash-style=gnu \
                --enable-new-dtags \
                --enable-relro \
                --enable-separate-code \
                --enable-rosegment \
                --enable-error-execstack \
                --enable-error-rwx-segments \
                --enable-colored-disassembly \
                --enable-deterministic-archives \
                --enable-compressed-debug-sections=all \
                --enable-default-compressed-debug-sections-algorithm=zlib
            """,
            build_script="""
            make -j"$PARALLEL"
            make install DESTDIR="$PKGBUILDDIR"
            """,
        ),
        package_step(
            name=arch_name("gcc-pass1"),
            display_name=f"GCC pass1 ({arch})",
            dependencies=(arch_build("binutils"),),
            cwd=lambda run_ctx: run_ctx.build_subdir(f"gcc-pass1-{arch}"),
            env=root_flags_env,
            configure_script="""
            ../../gcc-strata/configure \
                --build="$BUILD_TRIPLET" \
                --target="$TARGET_TRIPLET" \
                --prefix="$ARCH_PREFIX" \
                --with-gmp="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-mpfr="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-mpc="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-isl="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-sysroot="$SYSROOT" \
                --with-native-system-header-dir="/usr/include" \
                --with-system-zlib \
                --with-newlib \
                --without-headers \
                --enable-languages=c \
                --disable-nls \
                --disable-libssp \
                --disable-threads \
                --disable-shared \
                --disable-libgomp \
                --disable-libquadmath \
                --disable-libatomic \
                --disable-lto
            """,
            build_script="""
            make -j"$PARALLEL" all-gcc
            make -j"$PARALLEL" all-target-libgcc
            make install-gcc DESTDIR="$PKGBUILDDIR"
            make install-target-libgcc DESTDIR="$PKGBUILDDIR"
            """,
        ),
        ActionStep(
            name=f"install-gcc-stdint-{arch_suffix}",
            title=f"Install GCC stdint header ({arch})",
            dependencies=(arch_build("gcc-pass1"),),
            action=lambda run_ctx: run_ctx.copy_gcc_stdint(state),
        ),
        package_step(
            name=arch_name("libstrata"),
            display_name=f"libstrata ({arch})",
            dependencies=(f"install-gcc-stdint-{arch_suffix}",),
            cwd=lambda run_ctx: run_ctx.build_subdir(f"libstrata-{arch}"),
            env=arch_env,
            configure_script="""
            "$CMAKE" -S../../libstrata -B. \
                -DCMAKE_BUILD_TYPE=Debug \
                -DCMAKE_TOOLCHAIN_FILE="$ROOT/cmake/$TARGET_TRIPLET.cmake" \
                -DCMAKE_FIND_ROOT_PATH="$PKGBUILDDIR/$ARCH_PREFIX" \
                -DCMAKE_INSTALL_PREFIX="/usr" \
                -DCMAKE_C_FLAGS="-ffreestanding -nostdlib" \
                -DCMAKE_SYSROOT="$PKGBUILDDIR/$SYSROOT" \
                -DBUILD_SHARED_LIBS=ON
            """,
            build_script="""
            "$CMAKE" --build . --parallel="$PARALLEL"
            DESTDIR="$PKGBUILDDIR/$SYSROOT" "$CMAKE" --install .
            """,
        ),
        package_step(
            name=arch_name("musl-pass1"),
            display_name=f"musl pass1 ({arch})",
            dependencies=(arch_build("libstrata"),),
            cwd=lambda run_ctx: run_ctx.build_subdir(f"musl-pass1-{arch}"),
            env=arch_env,
            configure_script="""
            CROSS_COMPILE="$TARGET_TRIPLET-" \
            ../../musl-strata/configure \
                --build="$BUILD_TRIPLET" \
                --target="$TARGET_TRIPLET" \
                --prefix="/usr" \
                --disable-shared \
                --disable-gcc-wrapper \
                CFLAGS="-I$PKGBUILDDIR/$SYSROOT/usr/include" \
                SIDLC_LIBDIR="$ROOT/sidlc"
            """,
            build_script="""
            make SIDLC_LIBDIR="$ROOT/sidlc" install-headers DESTDIR="$PKGBUILDDIR/$SYSROOT"
            make SIDLC_LIBDIR="$ROOT/sidlc" -j"$PARALLEL"
            make SIDLC_LIBDIR="$ROOT/sidlc" install DESTDIR="$PKGBUILDDIR/$SYSROOT"
            """,
        ),
        package_step(
            name=arch_name("libtool-pass1"),
            display_name=f"libtool pass1 ({arch})",
            dependencies=(arch_build("musl-pass1"),),
            cwd=lambda run_ctx: run_ctx.build_subdir(f"libtool-pass1-{arch}"),
            env=arch_env,
            configure_script="""
            OLD_PATH="$PATH"
            if [ "$OSNAME" = "Darwin" ]; then
                export PATH="$PATH:/opt/homebrew/bin"
            fi
            CC="$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-gcc" \
            CXX="$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-gcc" \
            AR="$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-ld -r -o" \
            NM="$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-nm" \
            AS="$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-as" \
            LD="$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-ld" \
            STRIP="$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-strip" \
            RANLIB="true" \
            ../../libtool-strata/configure \
                --build="$BUILD_TRIPLET" \
                --prefix="$PKGBUILDDIR/$ARCH_PREFIX" \
                --exec-prefix="$PKGBUILDDIR/$ARCH_PREFIX/$TARGET_TRIPLET" \
                --host="$TARGET_TRIPLET" \
                --enable-ltdl-install \
                --enable-shared \
                --enable-static
            export PATH="$OLD_PATH"
            """,
            build_script="""
            OLD_PATH="$PATH"
            if [ "$OSNAME" = "Darwin" ]; then
                export PATH="$PATH:/opt/homebrew/bin"
            fi
            make -j"$PARALLEL" V=1
            export PATH="$OLD_PATH"
            make install
            ln -s "../$TARGET_TRIPLET/bin/libtool" "$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-libtool"
            ln -s "../$TARGET_TRIPLET/bin/libtoolize" "$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-libtoolize"
            """,
        ),
        RuntimeStep(
            name="set-arch-libtool-tools",
            dependencies=(arch_build("libtool-pass1"),),
            action=lambda run_ctx: run_ctx.set_arch_libtool_env(state),
        ),
        package_step(
            name=arch_name("gcc-pass2"),
            display_name=f"GCC pass2 ({arch})",
            dependencies=("set-arch-libtool-tools",),
            cwd=lambda run_ctx: run_ctx.build_subdir(f"gcc-pass2-{arch}"),
            env=arch_env,
            configure_script="""
            AR_FOR_TARGET="$PKGBUILDDIR/$ARCH_PREFIX/bin/$TARGET_TRIPLET-ld -r -o" \
            RANLIB_FOR_TARGET="true" \
            CFLAGS="$ROOT_CPPFLAGS" \
            CXXFLAGS="$ROOT_CPPFLAGS" \
            LDFLAGS="$ROOT_LDFLAGS -s" \
            ../../gcc-strata/configure \
                --build="$BUILD_TRIPLET" \
                --target="$TARGET_TRIPLET" \
                --prefix="$ARCH_PREFIX" \
                --with-gmp="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-mpfr="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-mpc="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-isl="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-libintl-prefix="$PKGBUILDDIR/$HOST_PREFIX" \
                --with-build-sysroot="$PKGBUILDDIR/$SYSROOT" \
                --with-sysroot="$SYSROOT" \
                --with-native-system-header-dir="/usr/include" \
                --with-system-zlib \
                --enable-languages=c,c++,objc,obj-c++,fortran \
                --enable-lto \
                --enable-shared \
                --enable-threads=posix \
                --enable-nls \
                --disable-libsanitizer \
                --disable-werror \
                --disable-multilib \
                --enable-libgomp \
                --enable-libssp \
                --enable-default-ssp \
                --enable-cet \
                --enable-secureplt \
                --enable-libatomic \
                --enable-libquadmath \
                --enable-__cxa_atexit \
                --enable-tls \
                --enable-linker-build-id
            """,
            build_script="""
            make -j"$PARALLEL" all-gcc
            make -j"$PARALLEL" all-target-libgcc
            make install-gcc DESTDIR="$PKGBUILDDIR"
            make install-target-libgcc DESTDIR="$PKGBUILDDIR"
            make -j"$PARALLEL" all-target-libstdc++-v3 \
                LDFLAGS_FOR_TARGET="-L$PKGBUILDDIR/$ARCH_PREFIX/$TARGET_TRIPLET/lib"
            make install-target-libstdc++-v3 DESTDIR="$PKGBUILDDIR"
            """,
        ),
        ScriptStep(
            name=f"cleanup-pass1-{arch_suffix}",
            title=f"Cleanup pass1 ({arch})",
            dependencies=(arch_build("gcc-pass2"),),
            cwd=lambda run_ctx: run_ctx.pkgbuilddir,
            env=arch_env,
            script="""
            find "./$SYSROOT/usr/lib/" -name "*.a" -delete
            find "./$SYSROOT/usr/lib/" -name "*.la" -delete
            find "./$SYSROOT/usr/lib/" -name "*.so*" -delete
            """,
        ),
        package_step(
            name=arch_name("musl-pass2"),
            display_name=f"musl pass2 ({arch})",
            dependencies=(f"cleanup-pass1-{arch_suffix}",),
            cwd=lambda run_ctx: run_ctx.build_subdir(f"musl-pass2-{arch}"),
            env=arch_env,
            configure_script="""
            CROSS_COMPILE="$TARGET_TRIPLET-" \
            ../../musl-strata/configure \
                --build="$BUILD_TRIPLET" \
                --target="$TARGET_TRIPLET" \
                --prefix="/usr" \
                --disable-gcc-wrapper \
                --enable-debug \
                CFLAGS="-I$PKGBUILDDIR/$SYSROOT/usr/include" \
                SIDLC_LIBDIR="$ROOT/sidlc"
            """,
            build_script="""
            make SIDLC_LIBDIR="$ROOT/sidlc" install-headers DESTDIR="$PKGBUILDDIR/$SYSROOT"
            make SIDLC_LIBDIR="$ROOT/sidlc" -j"$PARALLEL"
            make SIDLC_LIBDIR="$ROOT/sidlc" install DESTDIR="$PKGBUILDDIR/$SYSROOT"
            """,
        ),
        RuntimeStep(
            name="set-arch-toolchain",
            dependencies=(arch_build("musl-pass2"),),
            action=lambda run_ctx: run_ctx.set_arch_compiler_env(state),
        ),
    ]

    target_step_names: list[str] = []

    def target_deps(*names: str) -> tuple[str, ...]:
        return ("set-arch-toolchain", *(arch_build(name) for name in names))

    if include_target_libs:
        # Keep target library edges aligned with the actual configure/build flags:
        # nettle uses libgmp unless mini-gmp/public-key support is disabled,
        # while editline stays independent because we do not enable curses/termcap.
        for name, dependencies in [
            ("gmp", ()),
            ("mpfr", ("gmp",)),
            ("mpc", ("gmp", "mpfr")),
            ("nettle", ("gmp",)),
            ("libsodium", ()),
            ("libffi", ()),
            ("libxml2", ("libiconv",)),
            ("libexpat", ()),
            ("xz", ()),
            ("editline", ()),
            ("readline", ("ncurses",)),
        ]:
            configure_head = f"../{name}-src/configure"
            if name == "gmp":
                configure_head = 'CFLAGS="-std=gnu11" \\\n../gmp-src/configure'
            if name == "editline":
                configure_head = 'CFLAGS="-std=gnu11" \\\n../editline-src/configure'
            if name == "readline":
                configure_head = 'CFLAGS="-std=gnu11" \\\n../readline-src/configure'

            extra = ""
            if name == "nettle":
                extra = " \\\n    --disable-openssl"
            elif name == "readline":
                extra = ' \\\n    --with-curses \\\n    --enable-shared \\\n    --enable-static \\\n    LIBS="-lncursesw"'
            else:
                extra = " \\\n    --enable-shared \\\n    --enable-static"

            build_script = """
            make -j"$PARALLEL"
            make install DESTDIR="$PKGBUILDDIR/$SYSROOT"
            """
            if name == "nettle":
                build_script = """
                make -j"$PARALLEL" AUTOHEADER="$AUTOHEADER_HOST"
                make install DESTDIR="$PKGBUILDDIR/$SYSROOT" AUTOHEADER="$AUTOHEADER_HOST"
                """

            steps.append(
                package_step(
                    name=arch_name(name),
                    display_name=f"{name} ({arch})",
                    dependencies=target_deps(*dependencies),
                    cwd=lambda run_ctx, name=name: run_ctx.build_subdir(f"{name}-{arch}"),
                    env=arch_env,
                    configure_script=f"""
                    {configure_head} \
                        --build="$BUILD_TRIPLET" \
                        --with-sysroot="$PKGBUILDDIR/$SYSROOT" \
                        --host="$TARGET_TRIPLET" \
                        --prefix="/usr"{extra}
                    """,
                    build_script=build_script,
                )
            )
            target_step_names.append(arch_build(name))

        explicit_targets = [
            package_step(
                name=arch_name("libuv"),
                display_name=f"libuv ({arch})",
                dependencies=target_deps(),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"libuv-{arch}"),
                env=arch_env,
                configure_script="""
                CPPFLAGS="-D_GNU_SOURCE" \
                ../libuv-src/configure \
                    --build="$BUILD_TRIPLET" \
                    --with-sysroot="$PKGBUILDDIR/$SYSROOT" \
                    --host="$TARGET_TRIPLET" \
                    --prefix="/usr" \
                    --enable-shared \
                    --enable-static
                """,
                build_script="""
                make -j"$PARALLEL"
                make install DESTDIR="$PKGBUILDDIR/$SYSROOT"
                """,
            ),
            package_step(
                name=arch_name("libxslt"),
                display_name=f"libxslt ({arch})",
                dependencies=target_deps("libxml2"),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"libxslt-{arch}"),
                env=arch_env,
                configure_script="""
                CPPFLAGS="-I$PKGBUILDDIR/$SYSROOT/usr/include/libxml2" \
                ../libxslt-src/configure \
                    --build="$BUILD_TRIPLET" \
                    --with-sysroot="$PKGBUILDDIR/$SYSROOT" \
                    --host="$TARGET_TRIPLET" \
                    --prefix="/usr" \
                    --enable-shared \
                    --enable-static \
                    --without-python \
                    --with-libxml-prefix="$PKGBUILDDIR/$SYSROOT/usr"
                """,
                build_script="""
                make -j"$PARALLEL"
                make install DESTDIR="$PKGBUILDDIR/$SYSROOT"
                """,
            ),
            package_step(
                name=arch_name("yyjson"),
                display_name=f"yyjson ({arch})",
                dependencies=target_deps(),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"yyjson-{arch}"),
                env=arch_env,
                configure_script="""
                "$CMAKE" -S../yyjson-src -B. \
                    -DCMAKE_TOOLCHAIN_FILE="$ROOT/cmake/$TARGET_TRIPLET.cmake" \
                    -DCMAKE_FIND_ROOT_PATH="$PKGBUILDDIR/$ARCH_PREFIX" \
                    -DCMAKE_INSTALL_PREFIX="/usr" \
                    -DCMAKE_SYSROOT="$PKGBUILDDIR/$SYSROOT" \
                    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
                    -DBUILD_SHARED_LIBS=OFF \
                    -DYYJSON_BUILD_TESTS=OFF
                """,
                build_script="""
                "$CMAKE" --build . --parallel="$PARALLEL"
                DESTDIR="$PKGBUILDDIR/$SYSROOT" "$CMAKE" --install .
                """,
            ),
            package_step(
                name=arch_name("zlib"),
                display_name=f"target zlib ({arch})",
                dependencies=target_deps(),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"zlib-{arch}"),
                env=arch_env,
                configure_script="""
                rm -rf -- *
                cp -r ../zlib-src/* .
                """,
                build_script="""
                make -f "./folios/Makefile.gcc" -j"$PARALLEL" \
                    CROSS_PREFIX="$TARGET_TRIPLET-" \
                    CFLAGS="-I."
                make -f "./folios/Makefile.gcc" install \
                    DESTDIR="$PKGBUILDDIR/$SYSROOT"
                """,
            ),
            package_step(
                name=arch_name("bzip2"),
                display_name=f"bzip2 ({arch})",
                dependencies=target_deps(),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"bzip2-{arch}"),
                env=arch_env,
                configure_script="""
                mkdir -p "$BUILDDIR/bzip2-$ARCH"
                rm -rf "$BUILDDIR/bzip2-$ARCH"/*
                cp -r "$BUILDDIR/bzip2-src/"* "$BUILDDIR/bzip2-$ARCH"
                """,
                build_script="""
                make bz2.sl bzip2.app bzip2recover.app -j"$PARALLEL" PREFIX="/usr" \
                    CC="$CC" \
                    LD="$LD"
                make -f Makefile-bz2_dl -j"$PARALLEL" PREFIX="/usr" \
                    CC="$CC" \
                    LD="$LD"
                make install PREFIX="$PKGBUILDDIR/$SYSROOT/usr" \
                    CC="$CC" \
                    LD="$LD"
                cp -f bz2.dl* "$PKGBUILDDIR/$SYSROOT/usr/lib/"
                """,
            ),
            package_step(
                name=arch_name("lz4"),
                display_name=f"lz4 ({arch})",
                dependencies=target_deps(),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"lz4-{arch}"),
                env=arch_env,
                configure_script="""
                mkdir -p "$BUILDDIR/lz4-$ARCH"
                rm -rf "$BUILDDIR/lz4-$ARCH"/*
                cp -r "$BUILDDIR/lz4-src/"* "$BUILDDIR/lz4-$ARCH"
                """,
                build_script="""
                make -j"$PARALLEL" \
                    PREFIX="/usr" \
                    TARGET_OS=foliOS \
                    CC="$CC" \
                    NM="$NM" \
                    LD="$LD" V=1
                make install \
                    PREFIX="/usr" \
                    TARGET_OS=foliOS \
                    DESTDIR="$PKGBUILDDIR/$SYSROOT" V=1
                """,
            ),
            package_step(
                name=arch_name("zstd"),
                display_name=f"zstd ({arch})",
                dependencies=target_deps("zlib", "xz", "lz4"),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"zstd-{arch}"),
                env=arch_env,
                configure_script="""
                mkdir -p "$BUILDDIR/zstd-$ARCH"
                rm -rf "$BUILDDIR/zstd-$ARCH"/*
                cp -r "$BUILDDIR/zstd-src/"* "$BUILDDIR/zstd-$ARCH"
                """,
                build_script="""
                make -j"$PARALLEL" \
                    PREFIX="/usr" \
                    OS=foliOS \
                    TARGET_SYSTEM=foliOS \
                    UNAME_TARGET_SYSTEM=foliOS \
                    CC="$CC" \
                    NM="$NM" \
                    LD="$LD" \
                    CPPFLAGS="-fPIC" \
                    LDFLAGS="-shared" \
                    ZSTD_LIB_ZLIB=1 \
                    ZSTD_LIB_LZMA=1 \
                    ZSTD_LIB_LZ4=1
                make install -j"$PARALLEL" \
                    PREFIX="/usr" \
                    OS=foliOS \
                    TARGET_SYSTEM=foliOS \
                    UNAME_TARGET_SYSTEM=foliOS \
                    DESTDIR="$PKGBUILDDIR/$SYSROOT"
                """,
            ),
            package_step(
                name=arch_name("libarchive"),
                display_name=f"libarchive ({arch})",
                dependencies=target_deps("zlib", "bzip2", "xz", "lz4", "zstd", "nettle", "libexpat"),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"libarchive-{arch}"),
                env=arch_env,
                configure_script="""
                CPPFLAGS="-DAES_MAX_KEY_SIZE=AES256_KEY_SIZE" \
                ../libarchive-src/configure \
                    --build="$BUILD_TRIPLET" \
                    --with-sysroot="$PKGBUILDDIR/$SYSROOT" \
                    --host="$TARGET_TRIPLET" \
                    --prefix="/usr" \
                    --enable-shared \
                    --enable-static \
                    --with-zlib \
                    --with-bz2lib \
                    --with-lzma \
                    --with-lz4 \
                    --with-zstd \
                    --with-nettle \
                    --without-openssl \
                    --with-expat \
                    --without-xml2 \
                    LIBS="-lz -lbz2 -llzma -llz4 -lzstd -lnettle -lexpat"
                """,
                build_script="""
                make -j"$PARALLEL"
                make install DESTDIR="$PKGBUILDDIR/$SYSROOT"
                """,
            ),
            package_step(
                name=arch_name("libiconv"),
                display_name=f"target libiconv ({arch})",
                dependencies=target_deps(),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"libiconv-{arch}"),
                env=arch_env,
                configure_script="""
                ../libiconv-src/configure \
                    --build="$BUILD_TRIPLET" \
                    --with-sysroot="$PKGBUILDDIR/$SYSROOT" \
                    --host="$TARGET_TRIPLET" \
                    --prefix="/usr" \
                    --enable-shared \
                    --enable-static

                cp "$BUILDDIR/libtool-pass1-$ARCH/libtool" "./libtool"
                cp "$BUILDDIR/libtool-pass1-$ARCH/libtool" "./libcharset/libtool"
                """,
                build_script="""
                make -j"$PARALLEL" ARFLAGS="" RANLIB="true"
                make install DESTDIR="$PKGBUILDDIR/$SYSROOT"
                """,
            ),
            package_step(
                name=arch_name("ncurses"),
                display_name=f"target ncurses ({arch})",
                dependencies=target_deps(),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"ncurses-{arch}"),
                env=arch_env,
                configure_script="""
                BUILD_EXEEXT="" \
                ../ncurses-src/configure \
                    --build="$BUILD_TRIPLET" \
                    --with-sysroot="$PKGBUILDDIR/$SYSROOT" \
                    --host="$TARGET_TRIPLET" \
                    --prefix="/usr" \
                    --with-libtool \
                    --with-pkg-config-libdir="/usr/lib/pkgconfig" \
                    --with-strip-program="$STRIP" \
                    --with-tic-path="$TIC" \
                    --with-build-cc="/usr/bin/cc" \
                    --with-build-cflags="-std=gnu99" \
                    --without-ada \
                    --disable-mixed-case \
                    --disable-db-install \
                    --enable-shared \
                    --enable-static \
                    --enable-widec \
                    --enable-pc-files \
                    --enable-overwrite \
                    ac_build_exeext="" \
                    ac_exeext="" \
                    ac_cv_exeext="" \
                    ac_cv_build_exeext="" \
                    cf_cv_build_cc_works=yes
                """,
                build_script="""
                make -j"$PARALLEL" ARFLAGS="" RANLIB="true" LIBTOOL="$LIBTOOL"
                if [ "$SED_TYPE" = "bsd" ]; then
                    find lib -type f -name "*.la" -exec sed -i '' "s|^relink_command=.*|relink_command=''|" {} +
                else
                    find lib -type f -name "*.la" -exec sed -i "s|^relink_command=.*|relink_command=''|" {} +
                fi
                make install DESTDIR="$PKGBUILDDIR/$SYSROOT"
                ln -sf libncursesw.dl "$PKGBUILDDIR/$SYSROOT/usr/lib/libncurses.dl"
                """,
            ),
            package_step(
                name=arch_name("sqlite3"),
                display_name=f"sqlite3 ({arch})",
                dependencies=target_deps("readline", "ncurses"),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"sqlite3-{arch}"),
                env=arch_env,
                configure_script="""
                CFLAGS="-std=gnu11" \
                autosetup_tclsh="$TCLSH" \
                ../sqlite3-src/configure \
                    --build="$BUILD_TRIPLET" \
                    --host="$TARGET_TRIPLET" \
                    --prefix="/usr" \
                    --all \
                    --soname=sqlite3.dl \
                    --with-readline-ldflags="-L$PKGBUILDDIR/$SYSROOT/usr/lib -lreadline -lncursesw" \
                    --with-readline-cflags="-I$PKGBUILDDIR/$SYSROOT/usr/include" \
                    --dll-basename="sqlite3" \
                    AR="$LD -r -o" \
                    RANLIB="true" \
                    LIBS="-lm -ldl"
                """,
                build_script="""
                make -j"$PARALLEL" AR.flags="" T.exe=".app" T.dll=".dl" T.lib=".sl"
                make install DESTDIR="$PKGBUILDDIR/$SYSROOT" \
                    AR.flags="" T.exe=".app" T.dll=".dl" T.lib=".sl"
                """,
            ),
        ]

        steps.extend(explicit_targets)
        target_step_names.extend(
            [
                arch_build("libuv"),
                arch_build("libxslt"),
                arch_build("yyjson"),
                arch_build("zlib"),
                arch_build("bzip2"),
                arch_build("lz4"),
                arch_build("zstd"),
                arch_build("libarchive"),
                arch_build("libiconv"),
                arch_build("ncurses"),
                arch_build("sqlite3"),
            ]
        )

    uninstall_dependencies = tuple(target_step_names) if target_step_names else ("set-arch-toolchain",)

    steps.extend(
        [
            ScriptStep(
                name=f"uninstall-libtool-pass1-{arch_suffix}",
                title=f"Uninstall libtool pass1 ({arch})",
                dependencies=uninstall_dependencies,
                cwd=lambda run_ctx: run_ctx.build_subdir(f"libtool-pass1-{arch}"),
                env=arch_env,
                script="""
                make uninstall
                """,
            ),
            package_step(
                name=arch_name("libtool-pass2"),
                display_name=f"libtool pass2 ({arch})",
                dependencies=(f"uninstall-libtool-pass1-{arch_suffix}",),
                cwd=lambda run_ctx: run_ctx.build_subdir(f"libtool-pass2-{arch}"),
                env=arch_env,
                configure_script="""
                OLD_PATH="$PATH"
                if [ "$OSNAME" = "Darwin" ]; then
                    export PATH="$PATH:/opt/homebrew/bin"
                fi
                ../../libtool-strata/configure \
                    --build="$BUILD_TRIPLET" \
                    --prefix="$ARCH_PREFIX" \
                    --exec-prefix="$ARCH_PREFIX/$TARGET_TRIPLET" \
                    --host="$TARGET_TRIPLET" \
                    --enable-ltdl-install \
                    --enable-shared \
                    --enable-static
                export PATH="$OLD_PATH"
                """,
                build_script="""
                OLD_PATH="$PATH"
                if [ "$OSNAME" = "Darwin" ]; then
                    export PATH="$PATH:/opt/homebrew/bin"
                fi
                make -j"$PARALLEL" V=1
                export PATH="$OLD_PATH"
                make install DESTDIR="$PKGBUILDDIR"
                """,
            ),
            ScriptStep(
                name=f"cleanup-pass2-{arch_suffix}",
                title=f"Cleanup pass2 ({arch})",
                dependencies=(arch_build("libtool-pass2"),),
                cwd=lambda run_ctx: run_ctx.pkg_prefix_path(state.env["ARCH_PREFIX"]),
                env=arch_env,
                script="""
                find . -name "*.la" -delete
                if [ "$SED_TYPE" = "bsd" ]; then
                    LC_ALL=C \
                    find . -type f \\( -name "*.pc" -o -name "libtool" -o -name "libtoolize" -o -name "*-config" -o -name "*.h" \\) \
                        -exec sed -i '' "s|$PKGBUILDDIR||g" {} +
                else
                    find . -type f \\( -name "*.pc" -o -name "libtool" -o -name "libtoolize" -o -name "*-config" -o -name "*.h" \\) \
                        -exec sed -i "s|$PKGBUILDDIR||g" {} +
                fi
                """,
            ),
        ]
    )

    return StepGraph(f"arch-{arch}", steps)


def create_host_cleanup_graph(ctx: "BuildContext") -> StepGraph:
    uninstall_names: list[str] = []
    steps: list[BuildStep] = []
    
    steps.append(
        ScriptStep(
            name="cleanup-host",
            title="Cleanup host files",
            dependencies=tuple(uninstall_names),
            cwd=lambda run_ctx: run_ctx.pkg_prefix_path(run_ctx.host_prefix),
            env=_context_env,
            script="""
            find . -name "*.la" -delete
            if [ "$SED_TYPE" = "bsd" ]; then
                LC_ALL=C \
                find . -type f \\( -name "*.pc" -o -name "libtool" -o -name "libtoolize" -o -name "*-config" -o -name "*.h" \\) \
                    -exec sed -i '' "s|$PKGBUILDDIR||g" {} +
            else
                find . -type f \\( -name "*.pc" -o -name "libtool" -o -name "libtoolize" -o -name "*-config" -o -name "*.h" \\) \
                    -exec sed -i "s|$PKGBUILDDIR||g" {} +
            fi
            """,
        )
    )

    return StepGraph("host-cleanup", steps)
