from faasmctl.experiments.correctness import (
    run as run_migration_correctness,
)
from faasmctl.experiments.steady_state import (
    run as run_steady_state,
)

from invoke import task
from sys import exit


EXPERIMENTS = {
    "migration-correctness": run_migration_correctness,
    "steady-state": run_steady_state,
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
    total_requests=1000,
    concurrencies="1,2,4,8,16,32,64",
    payload_bytes=64,
    method="echo",
    repeats=1,
    service_host=None,
    client_host=None,
    placement="unknown",
    out_dir="steady_state_results",
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
        --total-requests 1000 \
        --concurrencies 1,2,4,8,16,32,64 \
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
    else:
        raise RuntimeError("Unhandled experiment '{}'".format(name))

    print("")
    if success:
        print("Result: PASS")
    else:
        print("Result: FAIL")
        exit(1)