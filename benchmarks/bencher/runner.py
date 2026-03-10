"""runner.py: Full end-to-end benchmark orchestration.

Replaces run-search-bench.sh (and the now-deleted run-es-bench.sh and
run-tantylla-bench.sh which skipped wait-for-services and seed steps).

Each run follows this exact sequence (mirroring run-search-bench.sh):
  1. tofu apply
  2. sleep 5s  (containers finish entrypoint init)
  3. wait_for_all
  4. setup connectors (competitor only) + sleep 5s
  5. seed 100 k real products
  6. ingest benchmark (drain sentinel batch, guaranteeing seed is indexed)
  7. start resource monitor
  8. search benchmark
  9. stop resource monitor
  10. tofu destroy
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from bencher import connectors, infra, monitor, search, seed
from bencher.ingest import run as run_ingest
from bencher.monitor import ResourceMonitor
from bencher.services import wait_for_all

# =========================================================================
# Workspace configuration table
# =========================================================================


# Each entry defines everything runner.py needs to drive a workspace without
# any conditional logic scattered through the loop.  New workspaces only
# require adding a row here.
@dataclass
class _WorkspaceConfig:
    tfvars: str
    scylla_port: int
    gateway_port: Optional[int]  # None → Tantylla not deployed
    es_port: Optional[int]  # None → Elasticsearch not deployed
    needs_connectors: bool
    # Output filename prefixes (no extension, no run-ID).
    ingest_prefix: str
    search_prefix: str
    resource_prefix: str


_WORKSPACE_CONFIGS: dict[str, _WorkspaceConfig] = {
    "competitor": _WorkspaceConfig(
        tfvars="workspaces/competitor.tfvars",
        scylla_port=9043,
        gateway_port=None,
        es_port=9200,
        needs_connectors=True,
        ingest_prefix="ingest-results-search-setup-competitor",
        search_prefix="benchmark-results-competitor",
        resource_prefix="resources-search-competitor",
    ),
    "tantylla-multi": _WorkspaceConfig(
        tfvars="workspaces/tantylla-multi.tfvars",
        scylla_port=9043,
        gateway_port=8080,
        es_port=None,
        needs_connectors=False,
        ingest_prefix="ingest-results-search-setup-tantylla-multi",
        search_prefix="benchmark-results-tantylla-multi",
        resource_prefix="resources-search-tantylla-multi",
    ),
    "tantylla-single": _WorkspaceConfig(
        tfvars="workspaces/tantylla-single.tfvars",
        scylla_port=9043,
        gateway_port=8080,
        es_port=None,
        needs_connectors=False,
        ingest_prefix="ingest-results-search-setup-tantylla-single",
        search_prefix="benchmark-results-tantylla-single",
        resource_prefix="resources-search-tantylla-single",
    ),
}

VALID_WORKSPACES: list[str] = list(_WORKSPACE_CONFIGS.keys())


def _log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# =========================================================================
# Single-run helper
# =========================================================================


def _run_one(
    workspace: str,
    cfg: _WorkspaceConfig,
    run_id: int,
    output_dir: Path,
    cwd: Path,
    seed_count: int,
    ingest_count: int,
) -> None:
    """Execute one full benchmark iteration."""
    _log(f"{'=' * 38}")
    _log(f"  Run {run_id}  [{workspace}]")
    _log(f"{'=' * 38}")

    # ------------------------------------------------------------------
    # 1. Spin up infrastructure
    # ------------------------------------------------------------------
    _log(f"Applying OpenTofu ({workspace})...")
    infra.apply(cfg.tfvars, cwd=cwd)

    # Give containers a moment to finish entrypoint initialisation before
    # the health-check loop starts — avoids spurious early failures.
    time.sleep(5)

    # ------------------------------------------------------------------
    # 2. Wait until all required endpoints are healthy
    # ------------------------------------------------------------------
    _log("Waiting for services...")
    wait_for_all(
        scylla_port=cfg.scylla_port,
        gateway_port=cfg.gateway_port,
        es_port=cfg.es_port,
    )

    # ------------------------------------------------------------------
    # 3. Register Kafka Connect connectors (competitor stack only)
    # ------------------------------------------------------------------
    if cfg.needs_connectors:
        _log("Setting up connectors...")
        bench_prefix = f"bench-{workspace}"
        connectors.setup(
            connect_host="localhost:8083",
            scylla_addr=f"{bench_prefix}-scylla:9042",
            es_addr=f"{bench_prefix}-elasticsearch:9200",
        )
        # Allow connectors to complete initial handshake before seeding.
        time.sleep(5)

    # ------------------------------------------------------------------
    # 4. Seed real product data (brand names match search benchmark queries)
    # ------------------------------------------------------------------
    _log(f"Seeding {seed_count:,} products...")
    seed.run(host="localhost", port=cfg.scylla_port, count=seed_count)

    # ------------------------------------------------------------------
    # 5. Drain the indexing pipeline
    #
    # run_ingest inserts a sentinel-branded batch and polls until every
    # document is visible.  Because CDC events are ordered, once the
    # sentinel batch appears all seed-data documents are also indexed.
    # ------------------------------------------------------------------
    _log("Draining indexing pipeline (ingest benchmark)...")
    ingest_out = output_dir / f"{cfg.ingest_prefix}-{run_id}.json"
    run_ingest(
        host="localhost",
        port=cfg.scylla_port,
        count=ingest_count,
        tantylla_url=f"http://localhost:{cfg.gateway_port}"
        if cfg.gateway_port
        else None,
        es_url=f"http://localhost:{cfg.es_port}" if cfg.es_port else None,
        output=ingest_out,
        tantylla_stack=f"{workspace}-stack",
        es_stack="elasticsearch-stack",
    )
    _log(f"Ingest results → {ingest_out.name}")

    # ------------------------------------------------------------------
    # 6. Start resource monitor
    # ------------------------------------------------------------------
    resource_file = output_dir / f"{cfg.resource_prefix}-{run_id}.ndjson"
    _log(f"Starting resource monitor → {resource_file.name}")
    mon = ResourceMonitor(resource_file, name_prefix=f"bench-{workspace}-")
    mon.start()

    # ------------------------------------------------------------------
    # 7. Run the search latency + QPS benchmark
    # ------------------------------------------------------------------
    _log("Running search benchmark...")
    search_out = output_dir / f"{cfg.search_prefix}-{run_id}.json"
    search.run(
        tantylla_url=f"http://localhost:{cfg.gateway_port}"
        if cfg.gateway_port
        else None,
        es_url=f"http://localhost:{cfg.es_port}" if cfg.es_port else None,
        output=search_out,
        tantylla_stack=f"{workspace}-stack",
        es_stack="elasticsearch-stack",
    )
    _log(f"Search results → {search_out.name}")

    # ------------------------------------------------------------------
    # 8. Stop resource monitor
    # ------------------------------------------------------------------
    _log("Stopping resource monitor...")
    mon.stop()

    # ------------------------------------------------------------------
    # 9. Tear down for a clean next iteration
    # ------------------------------------------------------------------
    _log("Destroying OpenTofu infrastructure...")
    infra.destroy(cwd=cwd)

    _log(f"Run {run_id} complete.")


# =========================================================================
# Public entry point
# =========================================================================


def run_all(
    workspace: str,
    total_runs: int = 10,
    output_dir: Path = Path("data/output"),
    cwd: Optional[Path] = None,
    seed_count: int = 100_000,
    ingest_count: int = 100_000,
) -> None:
    """Run `total_runs` isolated full-lifecycle benchmark iterations.

    Args:
        workspace:    One of VALID_WORKSPACES.
        total_runs:   How many apply/destroy cycles to complete.
        output_dir:   Directory for all result files.
        cwd:          Working directory for tofu commands (defaults to CWD).
        seed_count:   Number of real products to seed per run.
        ingest_count: Sentinel batch size for the pipeline-drain benchmark.
    """
    if workspace not in _WORKSPACE_CONFIGS:
        raise ValueError(
            f"Unknown workspace '{workspace}'. "
            f"Valid choices: {', '.join(VALID_WORKSPACES)}"
        )

    cfg = _WORKSPACE_CONFIGS[workspace]
    output_dir.mkdir(parents=True, exist_ok=True)

    _log(f"Workspace  : {workspace}")
    _log(f"Total runs : {total_runs}")
    _log(f"Output dir : {output_dir}")

    # Create/select the Terraform workspace once before the loop so each
    # iteration simply applies into the already-selected workspace.
    infra.select_workspace(workspace, cwd=cwd)

    for run_id in range(1, total_runs + 1):
        _run_one(workspace, cfg, run_id, output_dir, cwd, seed_count, ingest_count)

    _log(f"All {total_runs} runs finished. Results in: {output_dir}")
