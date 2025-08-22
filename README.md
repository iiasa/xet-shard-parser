## Xet Shard parser

## Dev notes

# Build
```bash
docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build --release --manylinux 2014
```

# Push to pypi
```bash
    twine upload -r pypi -u __token__ -p <password-or-token> ./dist/*
```

