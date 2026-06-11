from faasmctl.experiments.correctness import (
    run as run_migration_correctness,
)
from faasmctl.experiments.steady_state import (
    run as run_steady_state,
)
from faasmctl.experiments.compose_migration import (
    run as run_compose_migration,
)

from invoke import task
from sys import exit


EXPERIMENTS = {
    "migration-correctness": run_migration_correctness,
    "steady-state": run_steady_state,
    "compose-migration": run_compose_migration,
}

@task(name="list")
def list_experiments(ctx):
    """List available experiments."""
    print("Available experiments:")
    for name in EXPERIMENTS:
        print("  {}".format(name))


@task
def run(
    ctx,
    name,
    ini_file=None,
    fan_out=4,
    num_workers=2,

    # Common / steady-state
    total_requests=1000,
    concurrencies="1,2,4,8,16,32,64",
    payload_bytes=64,
    method="echo",
    repeats=1,
    service_host=None,
    client_host=None,
    placement="unknown",
    out_dir="steady_state_results",

    # ComposePost migration
    scenario="live_migration",
    target_service="UserService",
    source_host=None,
    dest_host=None,
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    trigger_after_s=3.0,
):
    """
    Run a named experiment.

    Examples:

      inv experiment.run migration-correctness --fan-out 8

      inv experiment.run steady-state \
        --placement local \
        --service-host 10.0.0.10 \
        --client-host 10.0.0.10 \
        --total-requests 1000 \
        --concurrencies 1,2,4,8,16,32,64 \
        --repeats 3

      inv experiment.run steady-state \
        --placement remote \
        --service-host 10.0.0.10 \
        --client-host 10.0.0.11 \
        --total-requests 5000 \
        --concurrencies 64 \
        --repeats 3
    """
    if name not in EXPERIMENTS:
        print(
            "Unknown experiment '{}'. Run `inv experiment.list` to see "
            "available experiments.".format(name)
        )
        exit(1)

    if name == "migration-correctness":
        success = EXPERIMENTS[name](
            ini_file=ini_file,
            fan_out=int(fan_out),
            num_workers=int(num_workers),
        )
    elif name == "steady-state":
        success = EXPERIMENTS[name](
            ini_file=ini_file,
            num_workers=int(num_workers),
            total_requests=int(total_requests),
            concurrencies=concurrencies,
            payload_bytes=int(payload_bytes),
            method=method,
            repeats=int(repeats),
            service_host=service_host,
            client_host=client_host,
            placement=placement,
            out_dir=out_dir,
        )
    elif name == "compose-migration":
        success = EXPERIMENTS[name](
            ini_file=ini_file,
            num_workers=int(num_workers),
            total_requests=int(total_requests),
            concurrencies=concurrencies,
            text_bytes=int(text_bytes),
            mention_count=int(mention_count),
            url_count=int(url_count),
            user_count=int(user_count),
            seed=int(seed),
            warmup_requests=int(warmup_requests),
            verify_storage=verify_storage,
            trigger_after_s=float(trigger_after_s),
            scenario=scenario,
            target_service=target_service,
            source_host=source_host,
            dest_host=dest_host,
            client_host=client_host,
            repeats=int(repeats),
            out_dir=out_dir,
        )
    else:
        raise RuntimeError("Unhandled experiment '{}'".format(name))

    print("")
    if success:
        print("Result: PASS")
    else:
        print("Result: FAIL")
        exit(1)