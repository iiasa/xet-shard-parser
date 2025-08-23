## Xet Shard parser

## Dev notes

# Build
```bash
docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build \
  --release \
  --manylinux 2014 \
  --interpreter python3.8 python3.9 python3.10 python3.11 python3.12

```

# Push to pypi
```bash
    twine upload -r pypi -u __token__ -p <password-or-token> ./dist/*
```

