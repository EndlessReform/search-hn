# Transform regression fixture

`transform.json` contains 4,643 `[raw_float32, expected_int8]` pairs generated with
NumPy's research expression:

```python
np.clip(np.rint(np.tanh(raw.astype(np.float32)) * np.float32(127)), -128, 127).astype(np.int8)
```

It includes every BF16-representable value with magnitude 0.001 through 8, saturation
and zero cases, and coordinates from live embeddings of `database indexes` and
`a short query` on the pinned BF16 server. Mean pooling can return float32 values
that are not themselves BF16-representable; the live coordinates cover that case.

This verifies the selected fixture, not universal bit identity between math libraries.
Synthetic arbitrary float32 values immediately adjacent to rounding boundaries can
produce a one-coordinate-step difference between NumPy's SIMD tanh and system tanhf
(for example, -1.6239616 on macOS). We do not claim otherwise. Production uses the
pinned Linux runtime; live comparisons check its final outputs against NumPy.
