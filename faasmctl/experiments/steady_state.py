import csv
import os
from pathlib import Path
from time import sleep

from faasmctl.util.batch import batch_exec_factory
from faasmctl.util.config import (
    get_faasm_ini_file,
    get_faasm_planner_host_port,
)
from faasmctl.util.docker import in_docker
from faasmctl.util.invoke import invoke_wasm
from faasmctl.util.planner import (
    discover_service,
    prepare_planner_msg,
    reset,
    set_planner_policy,
    shutdown_service,
)
from google.protobuf.json_format import MessageToJson
from requests import post


SERVICE_USER = "rpc"
SERVICE_FUNC = "BenchSvc"

BENCHMARK_USER = "rpc"
BENCHMARK_FUNC = "SteadyStateBench"

DISCOVER_POLL_PERIOD_S = 2

# Give async planner messages from the long-running service time to settle
# before another reset. This avoids reset racing with late SetMessageResult.
SERVICE_QUIESCE_PERIOD_S = 5

# Small pause between client repeats so we do not immediately hammer the
# planner after each result.
CLIENT_REPEAT_PAUSE_S = 1

# Use a normal placement policy. Do not use the forced migration policy here.
STEADY_STATE_POLICY = "spot"


def _normalise_output(output):
    if isinstance(output, bytes):
        return output.decode("utf-8")
    return str(output)


def _percentile(values, q):
    """
    Linear percentile. q in [0, 100].
    """
    if not values:
        return None

    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]

    pos = (q / 100.0) * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    frac = pos - lo

    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def _parse_benchmark_csv(output):
    """
    Parses output from SteadyStateBench.

    Expected columns:
      request_idx,batch_idx,slot_idx,concurrency,payload_bytes,
      start_ns,end_ns,latency_ns,ok,status

    Lines beginning with '#' are ignored.
    """
    output = _normalise_output(output)

    data_lines = [
        line for line in output.splitlines()
        if line.strip() and not line.startswith("#")
    ]

    if not data_lines:
        raise RuntimeError("Benchmark produced no CSV data rows")

    reader = csv.DictReader(data_lines)
    rows = list(reader)

    latencies_ns = []
    start_ns = []
    end_ns = []
    successes = 0
    failures = 0

    for row in rows:
        ok = int(row["ok"])
        if ok:
            successes += 1
            latencies_ns.append(int(row["latency_ns"]))
        else:
            failures += 1

        start_ns.append(int(row["start_ns"]))
        end_ns.append(int(row["end_ns"]))

    duration_ns = max(end_ns) - min(start_ns) if end_ns and start_ns else 0
    duration_s = duration_ns / 1e9 if duration_ns > 0 else 0.0
    throughput_rps = successes / duration_s if duration_s > 0 else 0.0

    return {
        "rows": rows,
        "total": len(rows),
        "successes": successes,
        "failures": failures,
        "duration_ns": duration_ns,
        "throughput_rps": throughput_rps,
        "p50_ns": _percentile(latencies_ns, 50),
        "p99_ns": _percentile(latencies_ns, 99),
        "p999_ns": _percentile(latencies_ns, 99.9),
        "max_ns": max(latencies_ns) if latencies_ns else None,
    }


def _write_raw_csv(path, output):
    output = _normalise_output(output)
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="") as f:
        f.write(output)


def _append_summary_csv(path, row):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    exists = os.path.exists(path)
    fieldnames = [
        "placement",
        "repeat",
        "service_host",
        "client_host",
        "total_requests",
        "concurrency",
        "payload_bytes",
        "method",
        "successes",
        "failures",
        "duration_ns",
        "throughput_rps",
        "p50_ns",
        "p99_ns",
        "p999_ns",
        "max_ns",
        "return_code",
    ]

    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def invoke_wasm_no_wait_placed(
    msg_dict,
    req_dict=None,
    ini_file=None,
    host=None,
):
    """
    Like invoke_wasm_no_wait, but optionally preloads a scheduling decision
    so the long-running service starts on a specific worker.

    Returns:
      (app_id, message_id)
    """
    if req_dict is None:
        req_dict = {
            "user": msg_dict["user"],
            "function": msg_dict["function"],
        }

    req = batch_exec_factory(req_dict, msg_dict, 1)
    app_id = req.appId
    msg_id = req.messages[0].id

    # Prepare EXECUTE_BATCH before mutating executedHost, matching the existing
    # invoke_wasm host_list behaviour.
    exec_msg = prepare_planner_msg(
        "EXECUTE_BATCH",
        MessageToJson(req, indent=None),
    )

    if not ini_file:
        ini_file = get_faasm_ini_file()

    planner_host, planner_port = get_faasm_planner_host_port(
        ini_file,
        in_docker(),
    )
    url = "http://{}:{}".format(planner_host, planner_port)

    if host is not None:
        req.messages[0].groupIdx = 0
        req.messages[0].executedHost = host

        preload_msg = prepare_planner_msg(
            "PRELOAD_SCHEDULING_DECISION",
            MessageToJson(req, indent=None),
        )

        response = post(url, data=preload_msg, timeout=None)
        if response.status_code != 200:
            raise RuntimeError(
                "Error preloading service scheduling decision "
                "(code={}): {}".format(response.status_code, response.text)
            )

    response = post(url, data=exec_msg, timeout=None)
    if response.status_code != 200:
        raise RuntimeError(
            "Failed to schedule service (code={}): {}".format(
                response.status_code,
                response.text,
            )
        )

    return app_id, msg_id


def _wait_for_service(ini_file=None):
    endpoint = None
    while endpoint is None:
        endpoint = discover_service(SERVICE_USER, SERVICE_FUNC, ini_file=ini_file)
        if endpoint is None:
            print(
                "      Service not ready, retrying in {}s...".format(
                    DISCOVER_POLL_PERIOD_S
                )
            )
            sleep(DISCOVER_POLL_PERIOD_S)

    return endpoint


def _shutdown_service_quietly(ini_file=None):
    print("[cleanup] Shutting down {}/{}...".format(SERVICE_USER, SERVICE_FUNC))

    try:
        shutdown_service(SERVICE_USER, SERVICE_FUNC, ini_file=ini_file)
        print("         Done.")
    except Exception as e:
        # Catch connection errors as well as RuntimeError. Cleanup should not
        # hide the original failure with a second traceback.
        print("         Warning: {}".format(e))

    sleep(SERVICE_QUIESCE_PERIOD_S)


def _start_service_once(
    ini_file=None,
    num_workers=2,
    service_host=None,
):
    print("[1/5] Resetting planner and waiting for {} workers...".format(num_workers))
    reset(expected_num_workers=num_workers, verbose=True)

    print("[2/5] Setting scheduler policy to {}...".format(STEADY_STATE_POLICY))
    set_planner_policy(STEADY_STATE_POLICY)

    print("[3/5] Starting {}/{} service...".format(SERVICE_USER, SERVICE_FUNC))
    app_id, msg_id = invoke_wasm_no_wait_placed(
        {
            "user": SERVICE_USER,
            "function": SERVICE_FUNC,
            "isRpc": True,
            "is_long_running": True,
        },
        ini_file=ini_file,
        host=service_host,
    )
    print("      appId={} messageId={}".format(app_id, msg_id))

    print("[4/5] Polling until service is discoverable...")
    endpoint = _wait_for_service(ini_file=ini_file)
    print("      Service ready at {}".format(endpoint))

    if service_host is not None and service_host not in str(endpoint):
        raise RuntimeError(
            "Expected service on host {}, but discovered endpoint is {}".format(
                service_host,
                endpoint,
            )
        )

    return app_id, msg_id, endpoint


def _run_client_once(
    ini_file=None,
    total_requests=1000,
    concurrency=1,
    payload_bytes=64,
    method="echo",
    service_host=None,
    client_host=None,
    placement="unknown",
    repeat=0,
    out_dir="steady_state_results",
):
    print(
        "[client] placement={} repeat={} total={} concurrency={} "
        "payload={} method={}".format(
            placement,
            repeat,
            total_requests,
            concurrency,
            payload_bytes,
            method,
        )
    )

    cmdline = "{} {} {} {}".format(
        total_requests,
        concurrency,
        payload_bytes,
        method,
    )

    host_list = [client_host] if client_host is not None else None

    result = invoke_wasm(
        {
            "user": BENCHMARK_USER,
            "function": BENCHMARK_FUNC,
            "cmdline": cmdline,
            "isRpc": True,
        },
        ini_file=ini_file,
        host_list=host_list,
        num_retries=30,
    )

    message_result = result.messageResults[0]
    output = _normalise_output(message_result.outputData)
    ret_code = message_result.returnValue

    raw_path = os.path.join(
        out_dir,
        "raw",
        "raw_{}_c{}_r{}.csv".format(placement, concurrency, repeat),
    )
    _write_raw_csv(raw_path, output)
    print("         Raw CSV written to {}".format(raw_path))

    parsed = _parse_benchmark_csv(output)

    result_row = {
        "placement": placement,
        "repeat": repeat,
        "service_host": service_host or "",
        "client_host": client_host or "",
        "total_requests": total_requests,
        "concurrency": concurrency,
        "payload_bytes": payload_bytes,
        "method": method,
        "successes": parsed["successes"],
        "failures": parsed["failures"],
        "duration_ns": parsed["duration_ns"],
        "throughput_rps": parsed["throughput_rps"],
        "p50_ns": parsed["p50_ns"],
        "p99_ns": parsed["p99_ns"],
        "p999_ns": parsed["p999_ns"],
        "max_ns": parsed["max_ns"],
        "return_code": ret_code,
    }

    print(
        "         successes={} failures={} throughput={:.2f} rps "
        "p50={:.0f}ns p99={:.0f}ns p999={:.0f}ns".format(
            parsed["successes"],
            parsed["failures"],
            parsed["throughput_rps"],
            parsed["p50_ns"] or 0,
            parsed["p99_ns"] or 0,
            parsed["p999_ns"] or 0,
        )
    )

    return result_row


def run_once(
    ini_file=None,
    num_workers=2,
    total_requests=1000,
    concurrency=1,
    payload_bytes=64,
    method="echo",
    service_host=None,
    client_host=None,
    placement="unknown",
    repeat=0,
    out_dir="steady_state_results",
):
    """
    Runs one steady-state RPC benchmark configuration.

    This is retained for one-off debugging. The normal benchmark path should
    use run_sweep(), which starts the service once and runs all client repeats
    before shutdown.
    """
    print("=== Steady-State RPC Benchmark ===")
    print(
        "placement={} repeat={} total={} concurrency={} payload={} method={}".format(
            placement,
            repeat,
            total_requests,
            concurrency,
            payload_bytes,
            method,
        )
    )
    print("service_host={} client_host={}".format(service_host, client_host))

    result_row = None

    try:
        _start_service_once(
            ini_file=ini_file,
            num_workers=num_workers,
            service_host=service_host,
        )

        print("[5/5] Invoking benchmark client...")
        result_row = _run_client_once(
            ini_file=ini_file,
            total_requests=total_requests,
            concurrency=concurrency,
            payload_bytes=payload_bytes,
            method=method,
            service_host=service_host,
            client_host=client_host,
            placement=placement,
            repeat=repeat,
            out_dir=out_dir,
        )

        summary_path = os.path.join(out_dir, "summary.csv")
        _append_summary_csv(summary_path, result_row)
        print("         Summary appended to {}".format(summary_path))

    finally:
        _shutdown_service_quietly(ini_file=ini_file)

    return result_row


def run_sweep(
    ini_file=None,
    num_workers=2,
    total_requests=1000,
    concurrencies=(1, 2, 4, 8, 16, 32, 64),
    payload_bytes=64,
    method="echo",
    repeats=1,
    service_host=None,
    client_host=None,
    placement="unknown",
    out_dir="steady_state_results",
):
    """
    Runs the concurrency sweep for one placement.

    For local placement:
      service_host == client_host

    For remote placement:
      service_host != client_host

    The service is started once for the whole sweep. This avoids racing planner
    reset against late async messages from the previous long-running service.
    """
    rows = []

    print("=== Steady-State RPC Benchmark Sweep ===")
    print(
        "placement={} total={} concurrencies={} repeats={} payload={} method={}".format(
            placement,
            total_requests,
            concurrencies,
            repeats,
            payload_bytes,
            method,
        )
    )
    print("service_host={} client_host={}".format(service_host, client_host))

    try:
        _start_service_once(
            ini_file=ini_file,
            num_workers=num_workers,
            service_host=service_host,
        )

        print("[5/5] Running client repeats...")
        summary_path = os.path.join(out_dir, "summary.csv")

        for concurrency in concurrencies:
            for repeat in range(repeats):
                row = _run_client_once(
                    ini_file=ini_file,
                    total_requests=total_requests,
                    concurrency=concurrency,
                    payload_bytes=payload_bytes,
                    method=method,
                    service_host=service_host,
                    client_host=client_host,
                    placement=placement,
                    repeat=repeat,
                    out_dir=out_dir,
                )

                _append_summary_csv(summary_path, row)
                print("         Summary appended to {}".format(summary_path))

                rows.append(row)

                sleep(CLIENT_REPEAT_PAUSE_S)

    finally:
        _shutdown_service_quietly(ini_file=ini_file)

    return rows


def run(
    ini_file=None,
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
    Entry point used by `inv experiment.run steady-state`.

    Returns True if all runs completed with zero failures.
    """
    if isinstance(concurrencies, str):
        concurrencies = tuple(
            int(x.strip())
            for x in concurrencies.split(",")
            if x.strip()
        )

    if placement == "local":
        if service_host is None or client_host is None:
            raise ValueError("local placement requires service_host and client_host")
        if service_host != client_host:
            raise ValueError("local placement requires service_host == client_host")

    if placement == "remote":
        if service_host is None or client_host is None:
            raise ValueError("remote placement requires service_host and client_host")
        if service_host == client_host:
            raise ValueError("remote placement requires service_host != client_host")

    rows = run_sweep(
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

    ok = True
    for row in rows:
        if row is None:
            ok = False
        elif int(row["return_code"]) != 0:
            ok = False
        elif int(row["failures"]) != 0:
            ok = False

    return ok