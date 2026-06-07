from faasmctl.experiments.correctness import (
    run as run_migration_correctness,
)
from invoke import task
from sys import exit

# Registry of available experiments.
# Each entry maps a name to a callable with signature:
#   run(ini_file=None, **kwargs) -> bool
EXPERIMENTS = {
    "migration-correctness": run_migration_correctness,
}


@task(name="list")
def list_experiments(ctx):
    """List available experiments."""
    print("Available experiments:")
    for name in EXPERIMENTS:
        print("  {}".format(name))


@task
def run(ctx, name, ini_file=None, fan_out=4, num_workers=2):
    """
    Run a named experiment.

    Usage: inv experiment.run <name> [--ini-file FILE] [--fan-out N] [--num-workers N]

    Example:
      inv experiment.run migration-correctness --fan-out 8
    """
    if name not in EXPERIMENTS:
        print(
            "Unknown experiment '{}'. Run `inv experiment.list` to see "
            "available experiments.".format(name)
        )
        exit(1)

    success = EXPERIMENTS[name](
        ini_file=ini_file,
        fan_out=int(fan_out),
        num_workers=int(num_workers),
    )

    print("")
    if success:
        print("Result: PASS")
    else:
        print("Result: FAIL")
        exit(1)