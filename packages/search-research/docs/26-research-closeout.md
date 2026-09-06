# Research closeout: whitepaper, retention, and TEI reproduction

The [Typst whitepaper](../whitepaper/search-hn.typ) is the primary synthesis of
notes 00–25. Its PDF is `output/pdf/search-hn-retrieval-whitepaper.pdf` from the
repository root. Earlier notes remain supporting provenance, including superseded
plans; the whitepaper's conclusion is the current research recommendation.

## TEI setup retained before removal

TEI is retired from this VM in favor of the validated stock vLLM BF16 setup.
The original launch files `compose.pplx.yaml`, `compose.qwen.yaml`, and
`compose.jina.yaml` are retained in source, along with the detailed diagnostics
in [CUDA preflight evidence](evidence/20260905-cuda-preflight.md) and
[the original setup record](10-sovereign-embeddings.md#vm-access-and-environment).
The closeout archive also includes the VM's actual compose files, reference
scripts/UV lockfiles, reference-check outputs, and server logs.

The non-obvious fix was **bypassing TEI's shell entrypoint**, not rebuilding TEI:

- Ampere image: `ghcr.io/huggingface/text-embeddings-inference:86-1.9.3`, digest
  `sha256:a7d82dfef16c3bf1a95e93f5b226f358312512dbb0d585b48c3cf886f9d470a9`.
- Operator updated the VM NVIDIA driver to 610.57.04; the reported CUDA UMD was
  13.3. These host driver/container-toolkit components remain installed because
  vLLM needs GPU access too. No host CUDA development toolkit was needed for TEI.
- That driver's `nvidia-smi` prints `CUDA UMD Version`; TEI's shell script expected
  `CUDA Version`, misparsed it, and selected `/usr/local/cuda/compat` unnecessarily.
  The selected compatibility shim produced `cuInit=803`.
- Use `--entrypoint /usr/local/bin/text-embeddings-router` (or the compose
  `entrypoint` already recorded), leaving the image's normal library path intact.
  GPU initialization, allocation and write/readback then passed. NVIDIA's
  container prestart compatibility checks were **not disabled**.
- VM workspace `/opt/searchhn-embeddings` is maya-owned; Docker is available to
  maya. Mount the HF cache, bind to loopback, and expose it through an SSH tunnel.
- Pplx required FP32 in this TEI backend; Qwen and the premerged Jina retrieval
  model used FP16. Keep their original prefixes/pooling from the saved recipes.

These are the observed fixes for that pinned image/driver combination, not a
recommendation to downgrade a future installation or apply the workaround blindly.
No system driver packages are removed during research cleanup.

## Retention contract

Archive raw rollouts (including failed attempts), eval generation/review evidence,
frozen corpus/query sets, per-case scores, candidate rankings, timing records,
model/serving recipes, source snapshots, and the whitepaper. Preserve selected
Pplx arrays in Garage. Delete rejected-model arrays and rebuildable DB fixtures;
older immutable Garage releases are not modified. Original hosted-control arrays
remain available in historical release v3, without republishing them here.

Keep the selected Pplx weights, vLLM image and BF16 server on the VM. Reap TEI
containers/image, rejected Qwen/Jina/Nemotron weights, rejected reference/server
UV environments and the stopped FP32 vLLM control container. Remove the dedicated
local scratch PostgreSQL container/volume after durable evidence verification.
Unrelated data files, real unit tests, and the production DB are out of scope.

## Completed verification and cleanup

Main release `research-20260906-v4`: 8,987 files, 871,885,024 logical bytes;
all 8,922 distinct objects were downloaded and byte-verified. Manifest SHA256:
`404830d1204129aff6866cb52d5061de2be9fd8704138d6660675d4b242fb136`.
Companion release `research-20260906-closeout` retains this final receipt/source
state and the six-page whitepaper. Exact evidence is in
`docs/evidence/closeout-20260906/` and the archived companion tree.

Companion verified: 163 files / 1,587,840 bytes, 155 distinct downloaded blobs;
manifest SHA256 `ce39eb92f30830cc4dd836b9a59caef694342b99556721c6c6a9c6316af44f4f`.
Its temporary staging tree was also removed after byte verification.

Removed 13 local experiment trees containing 3,726,463,861 logical bytes, plus
the dedicated scratch PG container and its approximately 8.79 GB data volume,
and the now-unused pgvector image. Host APFS/VM sparse-disk reclamation may not
immediately equal deleted logical bytes. The selected Pplx arrays are in Garage;
no bulk experiment arrays or rollouts are left on the laptop.

The inference VM's free disk increased by 21,885,259,776 bytes (about 21.9 GB)
after scoped cleanup. Removed all seven stopped experiment containers, TEI and
CUDA diagnostic images, Qwen/Jina/Nemotron model caches and lock remnants,
reference/Nemotron UV environments, old compose/reference files and the old
58081/58082 SSH tunnel. All diagnostic/setup files were archived first.

Retained only the active Pplx BF16 vLLM container/image, its Pplx weights and
runtime cache, and UV tooling under the embedding workspace. HTTP health returns
200. Host NVIDIA driver/container-toolkit packages remain intact. Unrelated
local Docker resources, existing non-experiment data files and unit tests were
left alone. Research cleanup is complete.
