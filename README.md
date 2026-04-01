# Xet Shard Parser

Python bindings to parse Xet shards and extract reconstruction info. Built with Rust and PyO3 for high performance.

## Installation

### 1. From GitHub Release (Fast, No Rust Required)
Install the pre-built binary wheels directly from the GitHub Releases page. This is the recommended method as it does not require a Rust compiler on your machine.

```bash
# Template: Replace <tag> and <wheel_name> with actual values from the Release
pip install https://github.com/<user>/<repo>/releases/download/<tag>/<wheel_name>.whl
```

**Note:** Thanks to ABI3 support, a single wheel for your platform works across all Python versions (3.8 to 3.13+).

### 2. From Source (Requires Rust)
If you have Rust and Cargo installed, you can install directly from the repository:

```bash
pip install git+https://github.com/<user>/<repo>.git#subdirectory=xet_shard_parser
```

---

## Development

### Local Build
For local development, use `maturin`:

```bash
pip install maturin
cd xet_shard_parser
maturin develop
```

### Build for Release
To build manylinux wheels locally using Docker:

```bash
docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build \
  --release \
  --manylinux auto \
  --interpreter python3.8
```

## Release Process
1. Update version in `pyproject.toml` and `Cargo.toml`.
2. Push a tag following the pattern `xet-shard-parser-v*` (e.g., `git tag xet-shard-parser-v0.1.2 && git push --tags`).
3. GitHub Actions will automatically build the wheels and create a new GitHub Release.
