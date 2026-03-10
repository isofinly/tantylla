"""cli.py: Command-line entry point for the bench framework.

Subcommand structure:
    bench run   <workspace> [--runs N] [--output-dir DIR]
    bench infra apply   <workspace>
    bench infra destroy
    bench infra outputs
    bench seed  [--host H] [--port P] [--count N]
    bench ingest --tantylla-url URL | --elasticsearch-url URL [options]
    bench search --tantylla-url URL | --elasticsearch-url URL [options]
"""

import argparse
import sys
from pathlib import Path

from bencher import infra
from bencher.runner import VALID_WORKSPACES, run_all

# =========================================================================
# Subcommand handlers
# =========================================================================


def _cmd_run(args: argparse.Namespace) -> None:
    run_all(
        workspace=args.workspace,
        total_runs=args.runs,
        output_dir=Path(args.output_dir),
        seed_count=args.seed_count,
        ingest_count=args.ingest_count,
    )


def _cmd_infra_apply(args: argparse.Namespace) -> None:
    from bencher.runner import _WORKSPACE_CONFIGS

    if args.workspace not in _WORKSPACE_CONFIGS:
        print(
            f"Unknown workspace '{args.workspace}'. "
            f"Valid: {', '.join(VALID_WORKSPACES)}",
            file=sys.stderr,
        )
        sys.exit(1)
    cfg = _WORKSPACE_CONFIGS[args.workspace]
    infra.select_workspace(args.workspace)
    infra.apply(cfg.tfvars)


def _cmd_infra_destroy(_args: argparse.Namespace) -> None:
    infra.destroy()


def _cmd_infra_outputs(_args: argparse.Namespace) -> None:
    import json

    print(json.dumps(infra.outputs(), indent=2))


def _cmd_seed(args: argparse.Namespace) -> None:
    from bencher import seed

    seed.run(
        host=args.host, port=args.port, count=args.count, batch_size=args.batch_size
    )


def _cmd_ingest(args: argparse.Namespace) -> None:
    from bencher.ingest import run as run_ingest

    if not args.tantylla_url and not args.elasticsearch_url:
        print(
            "ERROR: at least one of --tantylla-url or --elasticsearch-url is required",
            file=sys.stderr,
        )
        sys.exit(1)

    run_ingest(
        host=args.host,
        port=args.port,
        count=args.count,
        tantylla_url=args.tantylla_url,
        es_url=args.elasticsearch_url,
        output=Path(args.output),
        concurrency=args.concurrency,
        poll_interval=args.poll_interval,
        window_secs=args.window_secs,
        timeout_secs=args.timeout,
        seed=args.seed,
    )


def _cmd_search(args: argparse.Namespace) -> None:
    from bencher import search

    if not args.tantylla_url and not args.elasticsearch_url:
        print(
            "ERROR: at least one of --tantylla-url or --elasticsearch-url is required",
            file=sys.stderr,
        )
        sys.exit(1)

    search.run(
        tantylla_url=args.tantylla_url,
        es_url=args.elasticsearch_url,
        output=Path(args.output),
        query_count=args.queries,
        concurrency=args.concurrency,
        throughput_duration=args.throughput_duration,
        limit=args.limit,
        warmup=args.warmup,
        seed=args.seed,
    )


# =========================================================================
# Argument parser construction
# =========================================================================


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench",
        description="FTS benchmark framework: Tantylla vs Elasticsearch",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")
    sub.required = True

    # ------------------------------------------------------------------
    # bench run
    # ------------------------------------------------------------------
    p_run = sub.add_parser("run", help="Full end-to-end benchmark lifecycle")
    p_run.add_argument("workspace", choices=VALID_WORKSPACES, help="Target stack")
    p_run.add_argument(
        "--runs",
        type=int,
        default=10,
        metavar="N",
        help="Number of isolated iterations (default: 10)",
    )
    p_run.add_argument(
        "--output-dir",
        default="data/output",
        metavar="DIR",
        help="Result output directory (default: data/output)",
    )
    p_run.add_argument(
        "--seed-count",
        type=int,
        default=100_000,
        metavar="N",
        help="Real products to seed per run (default: 100000)",
    )
    p_run.add_argument(
        "--ingest-count",
        type=int,
        default=100_000,
        metavar="N",
        help="Sentinel batch size for pipeline drain (default: 100000)",
    )
    p_run.set_defaults(func=_cmd_run)

    # ------------------------------------------------------------------
    # bench infra
    # ------------------------------------------------------------------
    p_infra = sub.add_parser("infra", help="OpenTofu infrastructure commands")
    infra_sub = p_infra.add_subparsers(dest="infra_command", metavar="<subcommand>")
    infra_sub.required = True

    pi_apply = infra_sub.add_parser(
        "apply", help="Apply infrastructure for a workspace"
    )
    pi_apply.add_argument("workspace", choices=VALID_WORKSPACES + ["head-to-head"])
    pi_apply.set_defaults(func=_cmd_infra_apply)

    pi_destroy = infra_sub.add_parser(
        "destroy", help="Destroy current workspace infrastructure"
    )
    pi_destroy.set_defaults(func=_cmd_infra_destroy)

    pi_outputs = infra_sub.add_parser("outputs", help="Print workspace outputs as JSON")
    pi_outputs.set_defaults(func=_cmd_infra_outputs)

    # ------------------------------------------------------------------
    # bench seed
    # ------------------------------------------------------------------
    p_seed = sub.add_parser("seed", help="Seed product data into ScyllaDB")
    p_seed.add_argument("--host", default="localhost")
    p_seed.add_argument("--port", type=int, default=9043)
    p_seed.add_argument("--count", type=int, default=100_000, metavar="N")
    p_seed.add_argument("--batch-size", type=int, default=50, dest="batch_size")
    p_seed.set_defaults(func=_cmd_seed)

    # ------------------------------------------------------------------
    # bench ingest
    # ------------------------------------------------------------------
    p_ingest = sub.add_parser("ingest", help="Peak ingestion throughput benchmark")
    p_ingest.add_argument("--host", default="localhost")
    p_ingest.add_argument("--port", type=int, default=9043)
    p_ingest.add_argument("--tantylla-url", default=None, dest="tantylla_url")
    p_ingest.add_argument("--elasticsearch-url", default=None, dest="elasticsearch_url")
    p_ingest.add_argument("--count", type=int, default=100_000, metavar="N")
    p_ingest.add_argument("--concurrency", type=int, default=100)
    p_ingest.add_argument(
        "--poll-interval", type=float, default=0.5, dest="poll_interval"
    )
    p_ingest.add_argument("--window-secs", type=float, default=15.0, dest="window_secs")
    p_ingest.add_argument("--timeout", type=float, default=600.0)
    p_ingest.add_argument("--output", default="data/output/ingest-results.json")
    p_ingest.add_argument("--seed", type=int, default=42)
    p_ingest.set_defaults(func=_cmd_ingest)

    # ------------------------------------------------------------------
    # bench search
    # ------------------------------------------------------------------
    p_search = sub.add_parser("search", help="Search latency and throughput benchmark")
    p_search.add_argument("--tantylla-url", default=None, dest="tantylla_url")
    p_search.add_argument("--elasticsearch-url", default=None, dest="elasticsearch_url")
    p_search.add_argument("--queries", type=int, default=1000, metavar="N")
    p_search.add_argument("--concurrency", type=int, default=10)
    p_search.add_argument(
        "--throughput-duration",
        type=int,
        default=30,
        dest="throughput_duration",
        metavar="SECS",
    )
    p_search.add_argument("--limit", type=int, default=10)
    p_search.add_argument("--warmup", type=int, default=50)
    p_search.add_argument("--seed", type=int, default=42)
    p_search.add_argument("--output", default="data/output/benchmark-results.json")
    p_search.set_defaults(func=_cmd_search)

    return parser


# =========================================================================
# Entry point
# =========================================================================


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
