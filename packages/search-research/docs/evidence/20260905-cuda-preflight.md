# Inference VM CUDA preflight receipts — 2026-09-05

Target: `maya@magi06-inference` only. These were temporary diagnostics, not an
embedding service launch. No weights, system packages, sudo, kernel changes,
parent-host access or public listener. Temporary C binaries were removed by an
EXIT trap; containers used `--rm`, `--network none` and read-only probe mounts.
The pulled TEI image remains cached in Docker.

## Results

`docker run --rm --pull never --gpus all --network none
nvidia/cuda:12.0.0-base-ubuntu22.04 nvidia-smi` succeeds, reporting RTX 3060,
12,288 MiB, driver 550.163.01 and CUDA N/A. The image declares
`NVIDIA_DRIVER_CAPABILITIES=compute,utility`; this was not a utility-only test.

Actual CUDA runtime calls inside that same image:

```text
driver dlopen: libcuda.so.1: cannot open shared object file: No such file or directory
cudaGetDeviceCount: code=35 (CUDA driver version is insufficient for CUDA runtime version), devices=-1
cudaMalloc(4): code=35 (CUDA driver version is insufficient for CUDA runtime version)
```

The error text is generic; the failed driver library load is the specific evidence
here. VM `dpkg-query` reports no installed `libcuda1`; `apt-cache policy libcuda1`
offers 550.163.01-2, matching installed `libnvidia-ml1`. Both VM and diagnostic
container library caches lack `libcuda.so.1`. The latter does contain
`libcudart.so.12`, so the runtime itself is present. The loaded-module inventory
and device nodes contain no nvidia-uvm.

Pull succeeded:

```text
ghcr.io/huggingface/text-embeddings-inference:86-1.9.3
sha256:a7d82dfef16c3bf1a95e93f5b226f358312512dbb0d585b48c3cf886f9d470a9
```

Running the diagnostic in that actual image with `--gpus all` and the standard
NVIDIA checks fails before the diagnostic or entrypoint runs:

```text
docker: Error response from daemon: failed to create task for container:
failed to create shim task: OCI runtime create failed: runc create failed:
unable to start container process: error during container init:
error running prestart hook #0: exit status 1, stdout: , stderr:
Auto-detected mode as 'legacy'
nvidia-container-cli: requirement error: unsatisfied condition: cuda>=12.9,
please update your driver to a newer version, or use an earlier cuda container
```

TEI's [runtime Dockerfile](https://github.com/huggingface/text-embeddings-inference/blob/v1.9.3/Dockerfile-cuda)
does install `cuda-compat-12-9`, which includes a user-mode CUDA driver. Its
[entrypoint](https://github.com/huggingface/text-embeddings-inference/blob/v1.9.3/cuda-entrypoint.sh)
attempts to select that library path. The attempted diagnostic covered both the
default path and `/usr/local/cuda/compat`, but **neither ran** due to the prestart
failure. Do not claim an observed forward-compatibility error such as 804.

## Reproducible probe

The following is the expanded probe used for the TEI attempt; the initial CUDA
12.0 probe performed the same runtime calls without the additional `cuInit` call.
Compile with the VM's existing `cc -x c -o probe probe.c -ldl` in a temporary
directory, then mount that directory read-only as `/probe`. No CUDA headers or
development toolkit are needed: the probe resolves the public functions dynamically.

```c
#include <dlfcn.h>
#include <stdio.h>
#include <stddef.h>
int main(void) {
  void *d = dlopen("libcuda.so.1", RTLD_NOW);
  printf("libcuda.so.1: %s\n", d ? "loaded" : dlerror());
  if (d) {
    int (*init)(unsigned) = (int (*)(unsigned))dlsym(d, "cuInit");
    int (*name)(int, const char **) = dlsym(d, "cuGetErrorName");
    int (*text)(int, const char **) = dlsym(d, "cuGetErrorString");
    if (!init || !name || !text) return 3;
    int rc = init(0);
    const char *n = "", *s = "";
    name(rc, &n); text(rc, &s);
    printf("cuInit: %d %s (%s)\n", rc, n, s);
  }
  void *r = dlopen("libcudart.so.12", RTLD_NOW);
  if (!r) { printf("libcudart.so.12: %s\n", dlerror()); return 2; }
  int (*count)(int *) = dlsym(r, "cudaGetDeviceCount");
  const char *(*error)(int) = dlsym(r, "cudaGetErrorString");
  int (*alloc)(void **, size_t) = dlsym(r, "cudaMalloc");
  if (!count || !error || !alloc) return 3;
  int n = -1, rc = count(&n);
  printf("cudaGetDeviceCount: %d (%s), devices=%d\n", rc, error(rc), n);
  void *p = NULL;
  int a = alloc(&p, 4);
  printf("cudaMalloc(4): %d (%s)\n", a, error(a));
  return rc || a ? 1 : 0;
}
```

Invocation (substitute the temporary probe directory):

```sh
docker run --rm --pull never --gpus all --network none \
  -e NVIDIA_DRIVER_CAPABILITIES=compute,utility \
  -v "$probe_dir:/probe:ro" --entrypoint /bin/bash \
  ghcr.io/huggingface/text-embeddings-inference:86-1.9.3 \
  -c '/probe/probe; LD_LIBRARY_PATH=/usr/local/cuda/compat:$LD_LIBRARY_PATH /probe/probe'
```

Use the CUDA 12.0 image and `/probe/probe` directly to reproduce the first result.
No `NVIDIA_DISABLE_REQUIRE`, privileged mode, or host filesystem write mount was
used. The evidence establishes two failing configurations; it does not establish
that every TEI build or every container-only workaround is impossible.

## Initial minimal operator proposal (superseded by full driver update)

`apt-get --simulate install --no-install-recommends libcuda1=550.163.01-2
nvidia-modprobe` was checked: three new packages (`libcuda1`,
`libnvidia-pkcs11-openssl3`, `nvidia-support`), zero upgrades/removals.
`nvidia-modprobe` is already installed. Proposed commands in a root shell inside
the VM, for the user to execute:

```sh
apt-get install --no-install-recommends libcuda1=550.163.01-2
nvidia-modprobe -u -c 0
install -d -o maya -g maya -m 0755 /opt/searchhn-embeddings
```

The UVM command is an attempted module/device initialization, not a verified fix;
retain any failure output for the next diagnostic. This package transaction does
not upgrade the active kernel driver or establish stock CUDA-12.9 TEI compatibility.
Afterward, rerun the compute probe and investigate a compatible TEI image/build
before proposing any broader driver upgrade.

## After the operator's full driver update

Rechecked on 2026-09-05: driver and KMD 610.57.04, CUDA UMD 13.3.
`/opt/searchhn-embeddings` exists with owner/group maya and is writable.
No GPU workloads or running Docker containers remained after the diagnostics.

The same pinned TEI image now passes its NVIDIA prestart checks. An expanded C
probe uses `dlopen`/`dlsym` as above and additionally calls `cudaMemset(p,42,4)`,
`cudaMemcpy(result,p,4,2)` (device-to-host), and `cudaFree(p)`. With Docker's
`--entrypoint /probe/probe` and the default image library path, output is:

```text
LD_LIBRARY_PATH=/usr/local/cuda/lib64:/usr/local/cuda/lib64
cuInit=0
cudaGetDeviceCount=0 devices=1
cudaMalloc=0
cudaMemset=0
cudaMemcpyDeviceToHost=0 bytes=42,42,42,42
```

To exercise the real shell entrypoint without starting a service, the same binary
was mounted read-only over `/usr/local/bin/text-embeddings-router`. Result:

```text
LD_LIBRARY_PATH=/usr/local/cuda/compat:/usr/local/cuda/lib64:/usr/local/cuda/lib64
cuInit=803
```

Reading `/entrypoint.sh` from the actual image confirms its `awk '/CUDA Version/'`
pattern does not match the new `CUDA UMD Version` label. It defaults to zero and
selects the older compatibility driver incorrectly. The operational workaround is
to launch the real router directly with
`--entrypoint /usr/local/bin/text-embeddings-router`, retaining the default library
path. This leaves NVIDIA prestart checks enabled and changes no image contents.
The real router executable was also checked:

```sh
docker run --rm --pull never --gpus all --network none \
  --entrypoint /usr/local/bin/text-embeddings-router \
  ghcr.io/huggingface/text-embeddings-inference:86-1.9.3 --version
# text-embeddings-router 1.9.3
```

The temporary probe directory under `/opt/searchhn-embeddings` was removed. No
model weights or embedding server were started. CUDA operations and executable
startup are verified; model inference and throughput remain unmeasured.
