# foliSDK

**foliSDK** is a comprehensive cross-compiler toolchain and system library package for **foliOS**. It defines the canonical build environment for the foliOS ABI, runtime model, and binary format.

Unlike generic cross-toolchains, foliSDK intentionally removes historical Unix toolchain artifacts and aligns the entire build pipeline with the architectural principles of the foliOS runtime.

## Features

* **Custom foliOS ABI & Toolchain**: A deeply patched GCC/Binutils toolchain (`x86_64-strata-folios`, `i686-strata-folios`) designed for a clean-room OS environment.
* **Unified ELF-Based Link Model**: Eliminates legacy `ar`/`ranlib` archives in favor of a fully ELF-native static and dynamic linking strategy.
* **Modern Library Formats**:
  * `.sl` — A static library format implemented as a single relocatable ELF object generated via `ld -r`. Unlike traditional `.a` archives, `.sl` preserves full relocation metadata and enables fine-grained linker garbage collection.
  * `.dl` — A dynamic library format comparable to traditional `.so`, including SONAME support, but governed under foliOS package-level ABI management.
* **Package-Level ABI Versioning**: foliOS does not implement per-library ABI negotiation. Compatibility is guaranteed and managed at the package level rather than at individual binary granularity.
* **Custom ELF Interpreter**: Binaries are natively linked against foliOS's context-aware runtime linker (`/System/Processes/Current/RuntimeLinker.app`).  
  The `Current` node is resolved through GNT (Global Namespace Tree), allowing the system to dynamically select the appropriate runtime for the active process context.
* **Linker GC as Default**: The compiler is configured by default to use `-ffunction-sections -fdata-sections` and `-Wl,--gc-sections`, enabling aggressive dead code elimination at function granularity.
* **Layered Syscall Architecture**: Clean separation between the Kernel RunTime layer (`libstrata.dl`) and the POSIX wrapper (`libc.dl`), seamlessly linked together via GCC's custom `LIB_SPEC`.

* **System Libraries Built-in**:
* *Core & Math*: `gmp`, `mpfr`, `mpc`, `isl`
* *Cryptography*: `nettle`, `libsodium`
* *Data & Parsing*: `libxml2`, `libxslt`, `libexpat`, `yyjson`
* *Compression*: `zlib`, `bzip2`, `xz`, `lz4`, `zstd`, `libarchive`
* *System & Utilities*: `libffi`, `libuv`, `libiconv`, `ncurses`, `editline`, `readline`, `sqlite3`

* **Development Tools**:
* **CMake Integration**: Includes a custom fork (`cmake-strata`) and built-in modules (e.g., `UseSIDLC.cmake`) tailored to natively support building `foliOS` projects.
* **sidlc (SIDL Compiler)**: A custom Interface Definition Language compiler. It parses `.sidl` descriptions and generates C type headers, client bindings, server dispatch headers/sources, and combined server-client bindings for foliOS handle-based interfaces.
* **sma (Strata Module Archiver)**: A host-side tool and CMake module for bundling module kernel/user ELF images into a Strata module archive.

* **Environment Management**: Includes a structured shell activator (`folisdk-env.sh`) for macOS or Linux shells to seamlessly enter the SDK environment without polluting the host environment.

## Design Rationale

foliSDK intentionally modernizes the traditional Unix toolchain model:

* Removes `ar` archive indirection in favor of ELF-native static link units.
* Enables deterministic and reproducible builds by reducing container-level metadata variance.
* Improves compatibility with function-level garbage collection.
* Simplifies the toolchain surface area by unifying around a single object model.

GCC was selected as the base compiler due to its mature spec override system, straightforward `LIB_SPEC` customization, and deep integration with Binutils — allowing precise control over ABI behavior and runtime linkage.

## Prerequisites

### macOS Requirements

You'll need a set of GNU tools explicitly installed on your host system:

```sh
brew install texinfo wget gnu-getopt automake libtool tcl-tk help2man
```

### Linux Requirements

Ensure standard GNU build utilities are installed:

```sh
sudo apt-get install build-essential bison flex texinfo wget tar tcl help2man autoconf automake autoconf-archive pkg-config autopoint libssl-dev
```

### FreeBSD Requirements

Install the GNU build utilities from packages. The build runner prefers GNU m4
as `gm4` and GNU make as `gmake` on FreeBSD, because `/usr/bin/m4` and
`/usr/bin/make` are the BSD implementations and cannot build several vendored
GNU packages.

```sh
sudo pkg install bash curl git gmake m4 bison flex texinfo wget tcl86 help2man autoconf automake autoconf-archive libtool pkgconf gettext-tools openssl
```

## Building the SDK

The SDK is built by the Python graph runner in `build.py`. It downloads and verifies upstream source archives, initializes missing submodules, prepares patched sources, builds host tools, then builds one graph per target architecture.

```sh
./build.py --arch x86_64 --jobs 8
```

By default, build products are staged under `./build/pkgroot`. The staged install destination is `/opt/homebrew/opt` on macOS and `/opt` on Linux, producing package roots such as:

* `./build/pkgroot/opt/homebrew/opt/folisdk-host`
* `./build/pkgroot/opt/homebrew/opt/folisdk-x86_64`
* `./build/pkgroot/opt/homebrew/opt/folisdk-i686`

Available `build.py` options:

* `-a, --arch <arch>[,...]` : Target architecture list (default: `x86_64`; e.g., `x86_64,i686`).
* `-b, --build-dir <path>` : Build directory path (default: `./build`).
* `-d, --destination <path>` : Install destination prefix used inside `pkgroot` (default: `/opt/homebrew/opt` on macOS, `/opt` on Linux).
* `--builddir-layout` : Put host and target prefixes directly under the build directory instead of `build/pkgroot`; cannot be combined with `--destination`.
* `-j, --jobs <number>` : Parallel build job count (default: total CPUs - 1).
* `-n, --no-libs` : Build the core cross toolchain and skip the additional target library set.
* `--direct-output` : Disable the virtual terminal status UI and stream child process output directly.
* `--rerun-step <step>` : Ignore the stamp for one stamped step. Repeatable; accepts either `step` or `graph:step`.
* `--dry-run` : Print the scheduled work without executing build commands.
* `-h, --help` : Show help.

### Build Script Status

* `build.py` is the active top-level build entry point. Its `argparse` program name is still `build.sh` for compatibility in help output, but the repository currently ships and uses `build.py`.
* `build_steps.py` contains the build graph definitions and step primitives used by `build.py`. The graph order is `global-prepare`, `host-build-targets`, one `arch-<arch>` graph per selected target, and `host-cleanup`.
* Each stamped step writes `.<step>.stamp` files in the build directory. Use `--rerun-step` when one completed step needs to be forced without deleting the whole build tree.
* The build writes DOT graph files to `./build/graphs`, which can be useful when debugging dependencies.
* `make_package.sh` is the packaging step, not the compiler build step. It expects a completed staged tree from `build.py`, creates per-root `tar.gz` archives, and can render Homebrew formulae and Debian packages.

### Packaging

After `build.py` finishes, generate local package artifacts from the staged roots:

```sh
./make_package.sh --build-dir ./build --format homebrew,deb --version 0.0.1
```

Available `make_package.sh` options:

* `-b, --build-dir <path>` : Build directory containing `pkgroot` (default: `./build`).
* `-d, --destination <path>` : Install destination used by `build.py` (default: `/opt/homebrew/opt` on macOS, `/opt` on Linux).
* `-f, --format <format>[,...]` : Package formats to generate (`homebrew`, `deb`; default: `homebrew,deb`).
* `-v, --version <version>` : Package version (default: `0.0.1`).
* `-h, --help` : Show help.

`make_package.sh` scans `$build_dir/pkgroot/$destination` for `folisdk-*` roots. For each package root it creates `$build_dir/<package>.tar.gz`; when templates are present, it also renders Homebrew formulae into the build directory and Debian control files/packages on Linux. Debian package generation is skipped outside Linux or when `dpkg-deb` is unavailable.

## Installation & Usage

### Via Homebrew (macOS Recommended)

The repository includes Homebrew formula templates under `dist/homebrew`. Generate formulas with `make_package.sh`, then install the generated package formula from the build directory.

1. Complete the build and packaging steps.
2. Install the generated host formula and the target package you need:

    ```sh
    brew install --build-from-source ./build/folisdk-host.rb
    brew install --build-from-source ./build/folisdk-x86_64.rb
    ```

3. Initialize the SDK framework in your shell environment (`~/.zshrc` or `~/.bash_profile`):

    ```sh
    source $(brew --prefix folisdk-host)/share/folisdk/folisdk-env.sh
    ```

**Activate SDK Environment:**

```sh
folisdk_activate x86_64
```

You can now freely call gcc commands (e.g., `x86_64-strata-folios-gcc`) or configure scripts directly inside the active framework.

**Deactivate SDK Environment:**

```sh
folisdk_deactivate
```

## Host Tools

The host tools are built as part of the `folisdk-host` package.

* `sidlc` compiles SIDL source into `.sif` interface artifacts and generates language bindings. See [`sidlc/README.md`](sidlc/README.md).
* `sma` packages module kernel/user ELF images and interface metadata into `.sma` Strata module archives. See [`sma/README.md`](sma/README.md).

## Developing foliOS Applications

With the SDK activated, you can write native applications using the modern `foliOS` ABI. The toolchain handles `.dl` (dynamic) and `.sl` (static) links seamlessly.

Use the SDK CMake toolchain files under `folisdk/cmake` when configuring
native projects. For interface binding generation, see
[`sidlc/README.md`](sidlc/README.md).

## Manual Usage (Linux / Raw extract)

Alternatively, extract the generated artifact and add the SDK's `bin` directory into your `$PATH`.

```sh
export PATH="/path/to/extracted/opt/folisdk/bin:$PATH"
x86_64-strata-folios-gcc main.c -o out.app
x86_64-strata-folios-strip out.app
```
