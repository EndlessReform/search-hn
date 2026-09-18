# Shared inference deployment

This directory owns the homelab embedding deployment. It is independent of the
applications that consume it. Client agents should read the server's
[usage guide](https://magi06-inference.tail7a3eb.ts.net/embeddings/usage.md) or
[llms.txt](https://magi06-inference.tail7a3eb.ts.net/llms.txt), not operate this stack.
**Other applications depend on it: do not take it down for client development.**

## Hosts and layout

- SSH: `maya@magi06-inference`; Debian 13, x86_64, RTX 3060 12 GiB.
- HTTPS: `https://magi06-inference.tail7a3eb.ts.net` (`100.78.181.8`).
- Config: `/opt/homelab-inference/{compose.yaml,.env,Caddyfile}`.
- Host systemd Caddy: `/etc/caddy/Caddyfile`; automatic certificates from tailscaled.
- Compose project: `homelab-inference`; services `vllm` and `embedding-proxy`.
- Existing model/cache mounts remain `/opt/searchhn-embeddings/hf-cache` and
  `/opt/searchhn-embeddings/vllm`. These names are historical; retaining them avoids
  copying weights. Only vLLM mounts them. Do not delete them during cleanup.
- Zot: `magi07-registry.tail7a3eb.ts.net/homelab/embedding-proxy`.
  `https://magi07-registry.tail7a3eb.ts.net/home` is its UI, not an image name.

Caddy is the single VM entrypoint, bound only to the tailnet IPv4 address on 443.
It strips `/embeddings` before forwarding to the proxy on `127.0.0.1:8081`, or
`/vllm/embeddings` before forwarding to stock vLLM on `127.0.0.1:8080`.
The proxy reaches `http://vllm:8000` over Compose's private network. Neither backend
port is published to the LAN or tailnet. Tailnet ACLs control client access; there
is no additional API key. Raw access deliberately bypasses the proxy contract.

## Initial host setup (administrator)

These are the commands used for the initial installation. Docker with Compose and
NVIDIA GPU support, tailscaled, and HTTPS certificates enabled for the tailnet are
prerequisites. Installing Caddy requires sudo; ordinary Compose operations use
maya's existing Docker group membership.

```sh
sudo apt-get update
sudo apt-get install -y caddy
sudo install -d -o maya -g maya /opt/homelab-inference
```

Set `TS_PERMIT_CERT_UID=caddy` once in `/etc/default/tailscaled` (edit an existing
entry if present; do not keep appending duplicates):

```sh
printf '\nTS_PERMIT_CERT_UID=caddy\n' | sudo tee -a /etc/default/tailscaled
sudo systemctl restart tailscaled
```

Caddy uses this to request/renew the node's `.ts.net` certificate. There is no copied
certificate or manual renewal job. See [Tailscale's Caddy integration](https://tailscale.com/docs/integrations/web-servers/caddy/caddy-certificates).

From the checkout, stage the configuration:

```sh
scp deploy/inference/compose.yaml deploy/inference/Caddyfile maya@magi06-inference:/opt/homelab-inference/
scp deploy/inference/.env.example maya@magi06-inference:/opt/homelab-inference/.env
```

On the VM, validate and install Caddy:

```sh
caddy validate --config /opt/homelab-inference/Caddyfile --adapter caddyfile
sudo cp -a /etc/caddy/Caddyfile /etc/caddy/Caddyfile.before-inference
sudo install -m 644 /opt/homelab-inference/Caddyfile /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

The backup command above is for initial setup. On subsequent changes, keep the
original backup and take a newly named backup before replacing the configuration.
Do not overwrite unrelated future inference routes. Run validation before reload.

## Publish and deploy the proxy

From the checkout:

```sh
crates/embedding_proxy/publish.sh magi07-registry.tail7a3eb.ts.net/homelab/embedding-proxy YOUR_VERSION_TAG
```

The helper prints an immutable image reference. It builds on the build machine;
only the runtime image is pulled to the VM. If authentication becomes required,
run `docker login magi07-registry.tail7a3eb.ts.net` on the pushing/pulling host.
No credentials belong in these files. Initial publishing succeeded without supplying
a new key. A successful local push does not establish VM DNS/network/pull access.

On the VM, preserve the prior `.env`, then edit `EMBEDDING_PROXY_IMAGE`
to the new printed digest:

```sh
cd /opt/homelab-inference
cp .env .env.previous
# Edit .env to the reviewed new repository@sha256:... reference.
docker compose config --quiet
docker compose pull embedding-proxy
docker compose up -d --no-deps embedding-proxy
curl --fail http://127.0.0.1:8081/readyz
curl --fail http://127.0.0.1:8081/usage.md
curl --fail http://127.0.0.1:8081/llms.txt
```

The client guide and `llms.txt` automatically use the HTTPS origin supplied by
Caddy's standard forwarding headers. No URL variable or Caddy change is required.
Check the docs through the external HTTPS address to see the public URLs; direct
loopback inspection shows the loopback origin instead. The source templates contain
no deployment hostname, and a hostname change does not require an image rebuild.

The proxy can be restarted or upgraded without stopping vLLM. Do not use
`docker compose down` for this. Its SIGTERM handler drains requests, with a 35-second
Compose grace period and a 30-second upstream deadline. In-flight callers may still
see a connection error and should retry according to their own deadlines.

For initial model startup only: `docker compose up -d vllm`, wait for its health
check, then start the proxy. Both services use `restart: unless-stopped` so a Docker/
VM restart brings them back. The proxy can start before vLLM is ready; readiness
returns an error until the engine has loaded. `systemctl enable caddy` is normally
handled by the Debian package; check `systemctl is-enabled caddy docker tailscaled`.

## Verify and inspect

From a tailnet client:

```sh
curl --fail https://magi06-inference.tail7a3eb.ts.net/llms.txt
curl --fail https://magi06-inference.tail7a3eb.ts.net/embeddings/readyz
curl --fail https://magi06-inference.tail7a3eb.ts.net/embeddings/v1/models
```

For an actual embedding request, use the self-contained usage guide. `checks/`
contains explicit operator verification scripts; the priority check deliberately
creates queued GPU work and is not a routine health check. Run it only during an
agreed quiet interval. Evidence from the initial rollout is in `evidence/`.

On the VM:

```sh
cd /opt/homelab-inference
docker compose ps
docker compose logs --tail 50 embedding-proxy
docker compose logs --tail 50 vllm
docker stats --no-stream
journalctl -u caddy --since '10 minutes ago'
```

Proxy health with failing readiness usually means vLLM is loading/unavailable.
429 means the workload's slots are full; reduce concurrency and back off. A 4xx
for a document means fix/split that document. Do not restart the shared service for
these client errors. A changed recipe identifier requires deliberate index handling.

## Rollback

For a proxy regression, restore the previous digest in `.env`, pull it if necessary,
and `docker compose up -d --no-deps embedding-proxy`. vLLM stays running. Never
silently switch clients to the raw route; its output is incompatible without the
client-side transform.

The initial migration retained the stopped original container
`searchhn-pplx-vllm-bfloat16-FLASH_ATTN-20260905` and saved its inspect record in
`/opt/homelab-inference/rollback/original-container.json`. If the new model setup
fails, stop **only** the new vLLM service before starting the old container (both
need port 8080 and the same GPU). The old service lacks priority mode; this restores
the historical raw endpoint, not a fully functional priority proxy. Do not claim
proxy service restored until the priority-compatible backend is healthy.

```sh
cd /opt/homelab-inference
docker compose stop vllm
docker start searchhn-pplx-vllm-bfloat16-FLASH_ATTN-20260905
```

Caddy rollback is an administrator operation: validate the saved prior Caddyfile,
restore it to `/etc/caddy/Caddyfile`, and reload Caddy. No database or client search
configuration is changed by this deployment.

## Rollout receipts and next slice

Tranches 1–2 are ready for the database work in tranche 3. Proxy readiness and
vLLM health were rechecked after observability setup; Alloy, Caddy, Docker and
tailscaled were active and enabled. The administrator confirmed Loki delivery,
Prometheus scraping and accepted the dashboard.

Local Docker log rotation is configured as three 20 MB files per service. The proxy
has these limits following its documentation update; the original vLLM container
still has Docker's unlimited default until its next planned recreation. Do not restart inference just for this
handoff. During that maintenance, run `docker compose up -d` from this directory
and verify each container's `HostConfig.LogConfig` reflects the limits. Remote Loki
retention is managed separately. This pending housekeeping does not block tranche 3.

The [initial deployment record](evidence/README.md) contains the published digest,
measured disk/memory footprint, tests and lifecycle check. The administrator added
`registry-consumer` to the inference node; keep that tag and its registry TCP 443
grant so future image pulls work. No API key was needed for the successful push/pull.

The [tranche 2.1 instructions](observability/README.md) use local environment variables
for Loki and the Prometheus target, with copy-paste configuration. The administrator
confirmed both services' logs in Loki and the Prometheus target reporting `up=1`.
Import the [Shared Embeddings dashboard](../../grafana/embeddings_dashboard.json)
and select the existing datasources; import instructions are in the observability guide.
