from faasmctl.util.invoke import invoke_wasm, invoke_wasm_no_wait
from faasmctl.util.planner import (
    discover_service,
    reset,
    set_planner_policy,
    shutdown_service,
)
from time import sleep

# The long-running service under test
SERVICE_USER = "rpc"
SERVICE_FUNC = "PingSvc"

# The benchmark client
BENCHMARK_USER = "rpc"
BENCHMARK_FUNC = "StressTest"

FORCED_POLICY = "force"
DISCOVER_POLL_PERIOD_S = 2


def run(ini_file=None, fan_out=2, num_workers=2):
    """
    Migration correctness experiment.

    Sequence:
      1. Reset planner, wait for workers.
      2. Set scheduling policy to FORCED_MIGRATION.
      3. Start the Ping long-running service.
      4. Poll until the service is discoverable (ready).
      5. Invoke the stress-test benchmark with the given fan-out.
      6. Parse CSV output, assert received == total.
      7. Shut down the service.

    Returns True on pass, False on any failure.
    """
    passed = False

    print("=== Migration Correctness Experiment ===")
    print("fan-out={} num_workers={}".format(fan_out, num_workers))

    # 1. Reset
    print("[1/6] Resetting planner and waiting for {} workers...".format(num_workers))
    reset(expected_num_workers=num_workers, verbose=True)

    # 2. Start the service
    print("[2/6] Starting {}/{} service...".format(SERVICE_USER, SERVICE_FUNC))
    app_id, msg_id = invoke_wasm_no_wait(
        {"user": SERVICE_USER, "function": SERVICE_FUNC, "isRpc": True, "is_long_running": True},
        ini_file=ini_file,
    )
    print("      appId={} messageId={}".format(app_id, msg_id))

        # 3. Set policy
    # print("[3/6] Setting scheduler policy to {}...".format(FORCED_POLICY))
    set_planner_policy("spot")

    # 4. Wait for service to register as ready
    print("[4/6] Polling until service is discoverable...")
    endpoint = None
    while endpoint is None:
        endpoint = discover_service(SERVICE_USER, SERVICE_FUNC, ini_file=ini_file)
        if endpoint is None:
            print("      Not ready, retrying in {}s...".format(DISCOVER_POLL_PERIOD_S))
            sleep(DISCOVER_POLL_PERIOD_S)
    print("      Service ready at {}".format(endpoint))

    # 3. Set policy
    print("[3/6] Setting scheduler policy to {}...".format(FORCED_POLICY))
    set_planner_policy(FORCED_POLICY)


    # 5. Invoke benchmark
    print("[5/6] Invoking benchmark (fan-out={})...".format(fan_out))
    result = invoke_wasm(
        {
            "user": BENCHMARK_USER,
            "function": BENCHMARK_FUNC,
            "cmdline": str(fan_out),
            "isRpc": True,
        },
        ini_file=ini_file,
    )
    output = result.messageResults[0].outputData
    ret_code = result.messageResults[0].returnValue
    print("      ret={} output={}".format(ret_code, output))

    # 6. Validate
    print("[6/6] Validating output...")
    if ret_code != 0:
        print("      FAIL: non-zero return code {}".format(ret_code))
    else:
        try:
            fields = output.split(",")
            received = int(fields[0])
            total = int(fields[1])
            if received == total == (1 + fan_out):
                print("      PASS: received={}/{} responses".format(received, total))
                passed = True
            else:
                print(
                    "      FAIL: received={} total={} expected={}".format(
                        received, total, 1 + fan_out
                    )
                )
        except (ValueError, IndexError) as e:
            print("      FAIL: could not parse output '{}': {}".format(output, e))

    # 7. Shutdown (always, even on failure)
    print("[cleanup] Shutting down {}/{}...".format(SERVICE_USER, SERVICE_FUNC))
    try:
        shutdown_service(SERVICE_USER, SERVICE_FUNC, ini_file=ini_file)
        print("         Done.")
    except RuntimeError as e:
        print("         Warning: {}".format(e))

    return passed