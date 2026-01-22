---
description: How to set up a Sui full node using snapshot restore
---

# Sui Node Setup by Snapshot

This guide covers setting up a Sui full node using formal snapshots (the recommended approach for our use case).

## Prerequisites

### Hardware Requirements
- **CPUs:** 8 physical cores / 16 vCPUs
- **RAM:** 128 GB
- **Storage:** 4TB+ NVMe drive (mainnet)

### Software Requirements
- Linux (Ubuntu 22.04 recommended)
- `sui-tool` binary

### Install Dependencies
```bash
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    tzdata libprotobuf-dev ca-certificates build-essential \
    libssl-dev libclang-dev libpq-dev pkg-config openssl \
    protobuf-compiler git clang cmake
```

---

## Step 1: Create Directory Structure

```bash
sudo mkdir -p /opt/sui/config
sudo mkdir -p /opt/sui/db/authorities_db/full_node_db
sudo mkdir -p /opt/sui/key-pairs
sudo useradd -r -s /bin/false sui
sudo chown -R sui:sui /opt/sui
```

---

## Step 2: Download Configuration Files

```bash
# Download fullnode config template
wget https://github.com/MystenLabs/sui/raw/main/crates/sui-config/data/fullnode-template.yaml \
    -O /opt/sui/config/fullnode.yaml

# Download genesis blob (mainnet)
wget https://github.com/MystenLabs/sui-genesis/raw/main/mainnet/genesis.blob \
    -O /opt/sui/config/genesis.blob
```

---

## Step 3: Configure fullnode.yaml

Edit `/opt/sui/config/fullnode.yaml`:

```yaml
db-path: "/opt/sui/db/authorities_db/full_node_db"

network-address: "/dns/localhost/tcp/8080/http"
metrics-address: "0.0.0.0:9184"
json-rpc-address: "0.0.0.0:9000"
enable-event-processing: true

p2p-config:
  listen-address: "0.0.0.0:8084"

genesis:
  genesis-file-location: "/opt/sui/config/genesis.blob"

authority-store-pruning-config:
  num-latest-epoch-dbs-to-retain: 3
  epoch-db-pruning-period-secs: 3600
  num-epochs-to-retain: 0
  num-epochs-to-retain-for-checkpoints: 2
  max-checkpoints-in-batch: 10
  max-transactions-in-batch: 1000
  periodic-compaction-threshold-days: 1

state-archive-read-config:
  - ingestion-url: "https://checkpoints.mainnet.sui.io"
    concurrency: 5
```

---

## Step 4: Download Snapshot

> [!IMPORTANT]
> Before downloading, ensure no leftover directories from previous attempts:
> ```bash
> rm -rf /opt/sui/db/authorities_db/full_node_db/staging
> rm -rf /opt/sui/db/authorities_db/full_node_db/snapshot
> rm -rf /opt/sui/db/authorities_db/full_node_db/live
> ```

Run the snapshot download:

```bash
sui-tool download-formal-snapshot \
    --latest \
    --network mainnet \
    --genesis "/opt/sui/config/genesis.blob" \
    --path "/opt/sui/db/authorities_db/full_node_db" \
    --num-parallel-downloads 50 \
    --no-sign-request
```

> [!WARNING]
> **Staging Directory Behavior**
> 
> The tool downloads to `staging/` first, then renames to `live/` on success.
> If interrupted, data remains in `staging/` and the node cannot start.
> 
> To check status after download:
> ```bash
> ls -la /opt/sui/db/authorities_db/full_node_db/
> # SUCCESS: You should see 'live/' directory
> # FAILED: You see 'staging/' but no 'live/'
> ```

### If Download Was Interrupted

Clean up and re-run:

```bash
rm -rf /opt/sui/db/authorities_db/full_node_db/staging
rm -rf /opt/sui/db/authorities_db/full_node_db/snapshot

# Re-run the download command above
sui-tool download-formal-snapshot \
    --latest \
    --network mainnet \
    --genesis "/opt/sui/config/genesis.blob" \
    --path "/opt/sui/db/authorities_db/full_node_db" \
    --num-parallel-downloads 50 \
    --no-sign-request
```

---

## Step 5: Set Permissions

```bash
sudo chown -R sui:sui /opt/sui/db/authorities_db/full_node_db/live
```

---

## Step 6: Start the Node

### Method 1: Using cargo run (Development)

From the sui repository root:

```bash
cargo run --release --bin sui-node -- --config-path /opt/sui/config/fullnode.yaml
```

### Method 2: Using systemd (Production)

Create `/etc/systemd/system/sui-node.service`:

```ini
[Unit]
Description=Sui Full Node
After=network.target

[Service]
Type=simple
User=sui
ExecStart=/usr/local/bin/sui-node --config-path /opt/sui/config/fullnode.yaml
Restart=always
RestartSec=10
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Then start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable sui-node
sudo systemctl start sui-node
```

---

## Step 7: Verify Node is Running

```bash
# Check service status
sudo systemctl status sui-node

# Monitor logs
sudo journalctl -u sui-node -f

# Check sync status
curl -s http://localhost:9000 -d '{"jsonrpc":"2.0","id":1,"method":"sui_getLatestCheckpointSequenceNumber","params":[]}' | jq

# Check metrics
curl -s http://localhost:9184/metrics | grep sui_
```

---

## Directory Structure Reference

```
/opt/sui/
├── config/
│   ├── fullnode.yaml
│   └── genesis.blob
└── db/
    └── authorities_db/
        └── full_node_db/
            └── live/              # Database files (created after successful download)
                ├── store/
                ├── epochs/
                └── checkpoints/
```

During download:
```
full_node_db/
├── staging/           # Active download (becomes 'live' on success)
├── snapshot/          # Temporary files (deleted on success)
└── live/              # Final directory (used by node)
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `staging/` exists but no `live/` | Download was interrupted - clean up and re-run |
| Node won't start | Check `live/` directory exists and has correct permissions |
| Slow sync | Check network connectivity, add seed peers |
| Permission denied | Run `chown -R sui:sui /opt/sui/` |
| Out of disk space | Enable aggressive pruning or use larger disk |

### Common Commands

```bash
# Stop node
sudo systemctl stop sui-node

# Check logs for errors
sudo journalctl -u sui-node --since "1 hour ago" | grep -i error

# Completely reset and re-download
sudo systemctl stop sui-node
rm -rf /opt/sui/db/authorities_db/full_node_db/*
# Then run Step 4 again
```
