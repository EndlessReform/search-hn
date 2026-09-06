# Restore inference observability

This runbook installs **Alloy as a host systemd service** and collects the `vllm`
and `embedding-proxy` containers' logs into your existing Loki. Your existing
Prometheus scrapes vLLM over HTTPS; Grafana uses those existing datasources.

- **Power outage, VM disk intact:** use [After a power outage](#after-a-power-outage).
- **VM deleted or disk replaced:** follow steps 1–5 below.
- **Change only the Loki address:** use [Change the Loki URL](#change-the-loki-url).

Alloy gets Docker-group access. This trusts it with root-equivalent control of the
inference VM. Its HTTP listener stays on localhost. There is no additional public
port, logging driver, or collector container to configure.

## 1. Restore the inference service and copy these files

**On a replacement VM:** restore Docker/NVIDIA support, tailnet connectivity and
Caddy/Compose using the [inference deployment runbook](../README.md) first. This
runbook restores monitoring; it does not provision the VM, GPU drivers, Loki,
Prometheus or Grafana. The inference stack must retain Compose project name
`homelab-inference` and service names `vllm` and `embedding-proxy`.

**On your workstation, from the root of a checkout of this repository:** replace
`YOUR_USER@YOUR_INFERENCE_HOST`, then paste:

```sh
export INFERENCE_SSH='YOUR_USER@YOUR_INFERENCE_HOST'
ssh "$INFERENCE_SSH" 'mkdir -p ~/inference-observability'
scp deploy/inference/observability/{README.md,logs.alloy,render-prometheus.sh} \
  "$INFERENCE_SSH":inference-observability/
ssh "$INFERENCE_SSH"
```

**You are now on the inference VM.** Open Bash and run the remaining VM steps in
that same terminal. Commands stop on failure; fix the reported error before continuing.

```sh
bash
set -euo pipefail
sudo install -d -m 755 /opt/homelab-inference/observability
sudo install -m 644 ~/inference-observability/README.md \
  ~/inference-observability/logs.alloy /opt/homelab-inference/observability/
sudo install -m 755 ~/inference-observability/render-prometheus.sh \
  /opt/homelab-inference/observability/
cd /opt/homelab-inference
sudo docker compose ps
curl --fail --max-time 10 http://127.0.0.1:8081/readyz
```

Expected: both services running, vLLM healthy, and `ready` from the last command.
If the model is still loading, inspect `sudo docker compose logs --tail 50 vllm`
and retry readiness after it finishes. Resolve inference startup failures before
continuing; installing Alloy will not fix a model that cannot load.

## 2. Install Alloy on the inference VM

These are the Debian/Ubuntu package installation commands. They also work when
Alloy is already installed, in which case apt may update it. The configuration below
was validated with Alloy 1.19.2.

```sh
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -d -m 755 /etc/apt/keyrings
curl --fail --silent --show-error https://apt.grafana.com/gpg-full.key \
  | sudo tee /etc/apt/keyrings/grafana.asc >/dev/null
sudo chmod 644 /etc/apt/keyrings/grafana.asc
printf '%s\n' 'deb [signed-by=/etc/apt/keyrings/grafana.asc] https://apt.grafana.com stable main' \
  | sudo tee /etc/apt/sources.list.d/grafana.list >/dev/null
sudo apt-get update
sudo apt-get install -y alloy
alloy --version
```

Source: [Grafana's Linux package installation](https://grafana.com/docs/alloy/latest/set-up/install/linux/).

## 3. Save the URL and install the collector

**Still on the inference VM:** replace the URL on the first line and paste the block.
Use your Loki **push endpoint**, ending in `/loki/api/v1/push`.

This writes a complete configuration for the inference VM's Alloy service. If
rerun, it backs up the previous files and replaces them; it does not append duplicate
collectors. If you later add unrelated collectors, preserve them when changing this
configuration. The URL-only writer assumes Loki accepts connections from this VM
without separate authentication, matching the existing worker setup.

```sh
export LOKI_PUSH_URL='https://YOUR_LOKI_ENDPOINT/loki/api/v1/push'

# Validate before changing the installed configuration.
alloy validate /opt/homelab-inference/observability/logs.alloy

sudo install -d -m 755 /etc/alloy /etc/systemd/system/alloy.service.d
backup_dir="/etc/alloy/backup-$(date +%Y%m%dT%H%M%S)"
sudo install -d -m 700 "$backup_dir"
for config_file in /etc/alloy/config.alloy /etc/alloy/inference.env \
  /etc/systemd/system/alloy.service.d/inference-env.conf; do
  if sudo test -f "$config_file"; then
    sudo cp -a "$config_file" "$backup_dir/"
  fi
done

# Save the URL on the VM, outside the repository.
printf 'LOKI_PUSH_URL=%s\nCONFIG_FILE=/etc/alloy/config.alloy\nCUSTOM_ARGS=--server.http.listen-addr=127.0.0.1:12345\n' \
  "${LOKI_PUSH_URL:?Set the Loki push URL}" \
  | sudo sh -c 'umask 077; cat > /etc/alloy/inference.env'
sudo chmod 600 /etc/alloy/inference.env

sudo tee /etc/systemd/system/alloy.service.d/inference-env.conf >/dev/null <<'UNIT'
[Service]
EnvironmentFile=/etc/alloy/inference.env
UNIT

sudo install -m 644 /opt/homelab-inference/observability/logs.alloy /etc/alloy/config.alloy
sudo usermod -aG docker alloy
sudo systemctl daemon-reload
sudo systemctl enable alloy
sudo systemctl restart alloy
```

**The export was needed only for setup and validation.** Systemd reads the saved
`/etc/alloy/inference.env` every time Alloy starts. You can close this terminal;
reboots need no export or manual configuration. The package keeps collection state
in `/var/lib/alloy/data`. Restarting Alloy preserves it.

## 4. Verify that logs reach Loki

**On the inference VM:**

```sh
sudo systemctl is-active alloy
curl --fail --max-time 5 http://127.0.0.1:12345/-/ready
sudo -u alloy curl --fail --silent --unix-socket /var/run/docker.sock http://localhost/_ping
sudo journalctl -u alloy -n 50 --no-pager
```

Expected: `active`, a successful readiness response, and `OK` from Docker. These
check local operation; they do not prove delivery to Loki. Look for connection,
authentication or retry errors in the journal.

Generate a real request, which produces a proxy log and a vLLM access log:

```sh
curl --fail-with-body --max-time 40 http://127.0.0.1:8081/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{"model":"pplx-embed-v1-0.6b","input":"observability recovery check"}' \
  -o /dev/null
```

**In Grafana → Explore:** choose your existing Loki datasource, set the time range
to **Last 15 minutes**, and run:

```logql
{job="homelab-inference",service_name=~"vllm|embedding-proxy"}
```

Refresh after a few seconds. Confirm entries from **both** service names. This is
the log-delivery completion check; an active Alloy service alone is not enough.

## 5. Restore the Prometheus target if necessary

Skip editing Prometheus if its existing target still points at the restored VM's
hostname. Verify `up` below instead. Changing Alloy does not change the metrics path.

**On a machine with this checkout and jq installed:** use the full DNS name from
the Caddy site address / HTTPS certificate, including the tailnet suffix. A short
MagicDNS name or the node's IP can resolve and connect but fail the TLS handshake.
Use the same full name in both curl and the Prometheus target; do not disable TLS
verification to compensate. Replace the placeholder below, then paste:

```sh
export EMBEDDING_METRICS_TARGET='YOUR_INFERENCE_HOST.YOUR_TAILNET.ts.net:443'
curl --fail --max-time 10 "https://$EMBEDDING_METRICS_TARGET/vllm/embeddings/metrics" \
  -o /tmp/inference-metrics-check.txt
bash deploy/inference/observability/render-prometheus.sh > /tmp/inference-scrape.yaml
cat /tmp/inference-scrape.yaml
```

Replace the existing `homelab-embeddings-vllm` job in your Prometheus configuration
with this output, or append it under `scrape_configs:` if absent. Do not create two
jobs with that name. The generated file contains your address and stays outside Git.
The hostname must also resolve and be reachable from the **Prometheus server**.

**On the Prometheus server:** validate the full configuration before reloading it.
For a systemd installation at the standard config path:

```sh
sudo promtool check config /etc/prometheus/prometheus.yml && \
  sudo systemctl kill --kill-whom=main --signal=HUP prometheus.service
```

If your existing Prometheus runs in Docker, use its container name instead:

```sh
export PROMETHEUS_CONTAINER='YOUR_EXISTING_PROMETHEUS_CONTAINER'
sudo docker exec "$PROMETHEUS_CONTAINER" promtool check config /etc/prometheus/prometheus.yml && \
  sudo docker kill --signal=HUP "$PROMETHEUS_CONTAINER"
```

Use the actual config path if your Prometheus was installed with a different one.
These commands reload Prometheus; they do not recreate it or change its stored data.

**In Grafana → Explore:** choose the existing Prometheus datasource. After two scrape
intervals (about 30 seconds), run:

```promql
up{job="homelab-embeddings-vllm"}
```

Expected: **1**. An empty result means the job is missing or you selected the wrong
datasource. A value of 0 means scraping is failing; inspect this target's error on
the Prometheus **Status → Targets** page.

For throughput and queue depth:

```promql
sum(rate(vllm:request_success_total{job="homelab-embeddings-vllm",finished_reason="stop"}[5m]))
sum(rate(vllm:prompt_tokens_total{job="homelab-embeddings-vllm"}[5m]))
sum(vllm:num_requests_waiting{job="homelab-embeddings-vllm"})
```

These are completed embedding inputs/sec, input tokens/sec, and waiting inputs.
Zero throughput while idle is normal. `up=1` confirms the metrics endpoint answers;
`/embeddings/readyz` additionally checks the inference backend. This runbook restores
collection. Import the dashboard below for the overview; notification policies
remain in your existing monitoring setup.

## Import the service dashboard

On your workstation, use
[`grafana/embeddings_dashboard.json`](../../../grafana/embeddings_dashboard.json)
from this checkout. In Grafana:

1. Open **Dashboards → New → Import**.
2. Upload that JSON file, then click **Import**.
3. At the top of the dashboard, choose your existing **Prometheus** and **Loki**
   datasources from the dropdowns, then save the dashboard.

The dashboard is named **Shared Embeddings**. It includes scrape status, input/token
throughput, latency percentiles, queue depth, engine failures/cancellations, and both
services' logs. Successful vLLM health probes are hidden from its log panel; errors
remain visible. No hostnames or datasource IDs are embedded in the file.

It uses the job/service labels installed by this runbook. Give a newly configured
scrape at least two samples before expecting rates. Latency panels can be blank
when idle. `UP` describes the metrics scrape, not a successful embedding probe.
GPU telemetry and per-workload proxy metrics are not collected by this setup.

## After a power outage

**On the inference VM, with its disk intact:** no URL export or reinstallation is
needed. Services normally start automatically. Check and start any stopped services:

```sh
sudo systemctl start tailscaled docker caddy alloy
cd /opt/homelab-inference
sudo docker compose up -d
sudo docker compose ps
sudo systemctl is-active alloy
curl --fail --max-time 10 http://127.0.0.1:8081/readyz
```

If vLLM is still loading, wait for its health check before retrying readiness. Repeat
step 4's test request and Grafana check, then check Prometheus `up` from step 5.
Do not clear `/var/lib/alloy/data` to fix a temporary outage.

## What must survive a deleted VM

Keep your deployment settings in your existing private backup or password manager:

- `/etc/alloy/inference.env`: the Loki destination.
- `/var/lib/alloy/data`: optional restore of collector state; without it, Alloy starts
  fresh and may resend retained logs. Deleted VM-local logs cannot be recovered by Alloy.
- Any local authentication additions, if your deployment later needs them.

The repository supplies the collector config and commands. Existing logs already
sent to Loki remain on the Loki server; they are not stored exclusively on this VM.
If only this VM was lost, central Prometheus/Grafana configuration usually survives.

## Change the Loki URL

On the inference VM, run step 3 again with the new URL, then verify with step 4.
That updates the saved setting, takes a backup, and restarts only Alloy.

## If a check fails

| Symptom | Action |
|---|---|
| Alloy fails to start | `sudo journalctl -u alloy -n 50 --no-pager`; verify `/etc/alloy/inference.env` exists and the URL is filled in |
| Docker socket permission denied | `sudo usermod -aG docker alloy` followed by `sudo systemctl restart alloy` |
| Loki DNS/connect timeout | Fix VM-to-Loki DNS/network access; do not restart the model |
| Loki returns 401/403 | Fix endpoint authentication/access in your local Loki writer settings |
| Alloy runs but no matching logs | Check `sudo docker ps --filter label=com.docker.compose.project=homelab-inference`; the collector selects `vllm` and `embedding-proxy` in that project |
| Prometheus target is down | Read its Targets-page error; check DNS, HTTPS and reachability from the Prometheus host |
| curl reports a TLS alert for the short hostname or IP | Use the full `.ts.net` DNS name configured in Caddy, including the tailnet suffix |
| Loki rejects old timestamps | Check `timedatectl status` and inspect the reported timestamps; do not reset collector state blindly |

Updating/restarting the embedding containers does not require changing Alloy. The
collector follows their stable Compose labels, not disposable container IDs.
