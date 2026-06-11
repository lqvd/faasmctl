import csv
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import sleep, time

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
    set_next_evicted_host,
    set_planner_policy,
    shutdown_service,
)
from google.protobuf.json_format import MessageToJson
from requests import post


SERVICE_USER = "snb"

SERVICES = [
    "UserDbService",
    "PostStorageService",
    "TextService",
    "UniqueIdService",
    "UserService",
    "ComposePostService",
]

TARGET_SERVICE = "UserService"

BENCHMARK_USER = "snb"
BENCHMARK_FUNC = "benchmark_snb"

DISCOVER_POLL_PERIOD_S = 2

# Give async planner messages from long-running services time to settle before
# another reset. This avoids reset racing with late SetMessageResult.
SERVICE_QUIESCE_PERIOD_S = 5

# How long to wait for DiscoverService to stop finding a service after shutdown.
SERVICE_STOP_TIMEOUT_S = 30

# How long to wait for a live-migrated service discovery entry to move.
SERVICE_MOVE_TIMEOUT_S = 120

COMPOSE_MIGRATION_POLICY = "spot"


def _as_bool(value):
    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value != 0

    if value is None:
        return False

    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _normalise_output(output):
    if isinstance(output, bytes):
        return output.decode("utf-8")
    return str(output)


def _endpoint_host(endpoint):
    if endpoint is None:
        return None

    if hasattr(endpoint, "host"):
        return endpoint.host

    return str(endpoint)


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
    Parses output from benchmark_snb.

    Expected columns:
      request_idx,batch_idx,slot_idx,concurrency,text_bytes,
      mention_count,url_count,user_count,seed,
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
        "p90_ns": _percentile(latencies_ns, 90),
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
        "scenario",
        "repeat",
        "success",
        "zero_failures",
        "event_success",
        "target_service",
        "source_host",
        "dest_host",
        "client_host",
        "trigger_after_s",
        "event_start_rel_ns_approx",
        "event_duration_s",
        "event_endpoint",
        "target_app_id",
        "target_msg_id",
        "total_requests",
        "concurrency",
        "text_bytes",
        "mention_count",
        "url_count",
        "user_count",
        "seed",
        "warmup_requests",
        "verify_storage",
        "successes",
        "failures",
        "duration_ns",
        "throughput_rps",
        "p50_ns",
        "p90_ns",
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


def _planner_url(ini_file=None):
    if not ini_file:
        ini_file = get_faasm_ini_file()

    planner_host, planner_port = get_faasm_planner_host_port(
        ini_file,
        in_docker(),
    )

    return "http://{}:{}".format(planner_host, planner_port)


def invoke_wasm_no_wait_placed(
    user,
    func,
    ini_file=None,
    host=None,
):
    """
    Like invoke_wasm_no_wait, but optionally preloads a scheduling decision so
    the long-running service starts on a specific worker.

    Returns:
      (app_id, group_id, message_id)
    """
    req_dict = {
        "user": user,
        "function": func,
    }

    msg_dict = {
        "user": user,
        "function": func,
        "isRpc": True,
        "is_long_running": True,
    }

    req = batch_exec_factory(req_dict, msg_dict, 1)

    app_id = req.appId
    group_id = req.groupId
    msg_id = req.messages[0].id

    # Prepare EXECUTE_BATCH before mutating executedHost, matching the existing
    # invoke_wasm host_list behaviour and the distributed migration tests.
    exec_msg = prepare_planner_msg(
        "EXECUTE_BATCH",
        MessageToJson(req, indent=None),
    )

    url = _planner_url(ini_file=ini_file)

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
                "Error preloading service scheduling decision for {}/{} "
                "on host {} (code={}): {}".format(
                    user,
                    func,
                    host,
                    response.status_code,
                    response.text,
                )
            )

    response = post(url, data=exec_msg, timeout=None)
    if response.status_code != 200:
        raise RuntimeError(
            "Failed to schedule service {}/{} (code={}): {}".format(
                user,
                func,
                response.status_code,
                response.text,
            )
        )

    return app_id, group_id, msg_id


def _wait_for_service(user, func, ini_file=None):
    endpoint = None

    while endpoint is None:
        endpoint = discover_service(user, func, ini_file=ini_file)

        if endpoint is None:
            print(
                "      {}/{} not ready, retrying in {}s...".format(
                    user,
                    func,
                    DISCOVER_POLL_PERIOD_S,
                )
            )
            sleep(DISCOVER_POLL_PERIOD_S)

    return endpoint


def _wait_for_service_gone(user, func, ini_file=None):
    deadline = time() + SERVICE_STOP_TIMEOUT_S

    while time() < deadline:
        try:
            endpoint = discover_service(user, func, ini_file=ini_file)
        except Exception as e:
            print(
                "         Warning while checking {}/{} shutdown: {}".format(
                    user,
                    func,
                    e,
                )
            )
            return

        if endpoint is None:
            print("         {}/{} no longer discoverable.".format(user, func))
            return

        print(
            "         {}/{} still discoverable, waiting...".format(
                user,
                func,
            )
        )
        sleep(DISCOVER_POLL_PERIOD_S)

    print(
        "         Warning: {}/{} still discoverable after {}s".format(
            user,
            func,
            SERVICE_STOP_TIMEOUT_S,
        )
    )


def _wait_for_service_on_host(user, func, expected_host, ini_file=None):
    deadline = time() + SERVICE_MOVE_TIMEOUT_S

    while time() < deadline:
        endpoint = discover_service(user, func, ini_file=ini_file)
        endpoint_host = _endpoint_host(endpoint)

        if endpoint is not None and endpoint_host == expected_host:
            print(
                "         {}/{} now discoverable at {}".format(
                    user,
                    func,
                    endpoint,
                )
            )
            return endpoint

        # Fallback for endpoint string representations.
        if endpoint is not None and expected_host in str(endpoint):
            print(
                "         {}/{} now discoverable at {}".format(
                    user,
                    func,
                    endpoint,
                )
            )
            return endpoint

        print(
            "         Waiting for {}/{} to move to {} "
            "(current endpoint={})...".format(
                user,
                func,
                expected_host,
                endpoint,
            )
        )
        sleep(DISCOVER_POLL_PERIOD_S)

    raise RuntimeError(
        "Timed out waiting for {}/{} to move to {}".format(
            user,
            func,
            expected_host,
        )
    )


def _shutdown_service_quietly(user, func, ini_file=None):
    print("[cleanup] Shutting down {}/{}...".format(user, func))

    try:
        shutdown_service(user, func, ini_file=ini_file)
        print("         Shutdown request sent.")
    except Exception as e:
        print("         Warning: {}".format(e))

    _wait_for_service_gone(user, func, ini_file=ini_file)


def _default_placements(source_host, dest_host, target_service):
    """
    Isolate target_service on source_host, and put the rest of the SNB stack on
    dest_host. This matters because SET_NEXT_EVICTED_VM evicts a host, not a
    single service.
    """
    if source_host is None or dest_host is None:
        return {}

    placements = {}

    for service in SERVICES:
        placements[service] = dest_host

    placements[target_service] = source_host

    return placements


def _start_service_once(
    service_func,
    ini_file=None,
    service_host=None,
):
    print(
        "[service] Starting {}/{} service...".format(
            SERVICE_USER,
            service_func,
        )
    )

    app_id, group_id, msg_id = invoke_wasm_no_wait_placed(
        SERVICE_USER,
        service_func,
        ini_file=ini_file,
        host=service_host,
    )

    print(
        "      appId={} groupId={} messageId={}".format(
            app_id,
            group_id,
            msg_id,
        )
    )

    print("      Polling until service is discoverable...")
    endpoint = _wait_for_service(
        SERVICE_USER,
        service_func,
        ini_file=ini_file,
    )
    print("      Service ready at {}".format(endpoint))

    if service_host is not None:
        endpoint_host = _endpoint_host(endpoint)

        if endpoint_host != service_host and service_host not in str(endpoint):
            raise RuntimeError(
                "Expected {}/{} on host {}, but discovered endpoint is {}".format(
                    SERVICE_USER,
                    service_func,
                    service_host,
                    endpoint,
                )
            )

    return {
        "user": SERVICE_USER,
        "function": service_func,
        "app_id": app_id,
        "group_id": group_id,
        "msg_id": msg_id,
        "host": service_host or "",
        "endpoint": str(endpoint),
    }


def _start_service_stack(
    ini_file=None,
    num_workers=2,
    placements=None,
):
    print("[1/6] Resetting planner and waiting for {} workers...".format(num_workers))
    reset(expected_num_workers=num_workers, verbose=True)

    print("[2/6] Setting scheduler policy to {}...".format(COMPOSE_MIGRATION_POLICY))
    set_planner_policy(COMPOSE_MIGRATION_POLICY)

    print("[3/6] Starting SNB services...")

    if placements is None:
        placements = {}

    started = {}

    for service_func in SERVICES:
        service_host = placements.get(service_func)

        started[service_func] = _start_service_once(
            service_func,
            ini_file=ini_file,
            service_host=service_host,
        )

    return started


def _shutdown_service_stack(ini_file=None):
    for service_func in reversed(SERVICES):
        _shutdown_service_quietly(
            SERVICE_USER,
            service_func,
            ini_file=ini_file,
        )

    print(
        "         Quiescing for {}s before next reset...".format(
            SERVICE_QUIESCE_PERIOD_S,
        )
    )
    sleep(SERVICE_QUIESCE_PERIOD_S)


def _trigger_live_migration(
    target_info,
    source_host,
    dest_host,
    ini_file=None,
):
    """
    Live migration scenario.

    The target service has already been started on source_host through an
    initial preloaded scheduling decision. This event only evicts source_host
    and waits for service discovery to move to dest_host.
    """
    service_func = target_info["function"]

    print(
        "[event] Live migration by eviction: {}/{} appId={} msgId={} {} -> {}".format(
            SERVICE_USER,
            service_func,
            target_info["app_id"],
            target_info["msg_id"],
            source_host,
            dest_host,
        )
    )

    event_start_s = time()
    event_success = 0
    endpoint = ""

    set_next_evicted_host([source_host])

    moved_endpoint = _wait_for_service_on_host(
        SERVICE_USER,
        service_func,
        dest_host,
        ini_file=ini_file,
    )

    endpoint = str(moved_endpoint)
    event_success = 1
    event_end_s = time()

    return {
        "event_success": event_success,
        "event_start_s": event_start_s,
        "event_end_s": event_end_s,
        "event_duration_s": event_end_s - event_start_s,
        "event_endpoint": endpoint,
        "target_app_id": target_info["app_id"],
        "target_msg_id": target_info["msg_id"],
    }


def _trigger_drain_restart(
    target_service,
    source_host,
    dest_host,
    ini_file=None,
):
    """
    Non-migrating baseline.

    Shut down the target service, wait for it to disappear from service
    discovery, then start a fresh instance on dest_host.
    """
    print(
        "[event] Drain/restart: {}/{} {} -> {}".format(
            SERVICE_USER,
            target_service,
            source_host,
            dest_host,
        )
    )

    event_start_s = time()
    event_success = 0

    shutdown_service(SERVICE_USER, target_service, ini_file=ini_file)
    _wait_for_service_gone(SERVICE_USER, target_service, ini_file=ini_file)

    restarted = _start_service_once(
        target_service,
        ini_file=ini_file,
        service_host=dest_host,
    )

    event_success = 1
    event_end_s = time()

    return {
        "event_success": event_success,
        "event_start_s": event_start_s,
        "event_end_s": event_end_s,
        "event_duration_s": event_end_s - event_start_s,
        "event_endpoint": restarted["endpoint"],
        "target_app_id": restarted["app_id"],
        "target_msg_id": restarted["msg_id"],
    }


def _run_client_once(
    ini_file=None,
    total_requests=1000,
    concurrency=1,
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    source_host=None,
    dest_host=None,
    client_host=None,
    scenario="unknown",
    target_service=TARGET_SERVICE,
    repeat=0,
    out_dir="compose_migration_results",
):
    print(
        "[client] scenario={} repeat={} total={} concurrency={} "
        "text={} mentions={} urls={} users={} seed={} warmup={} verify={}".format(
            scenario,
            repeat,
            total_requests,
            concurrency,
            text_bytes,
            mention_count,
            url_count,
            user_count,
            seed,
            warmup_requests,
            1 if _as_bool(verify_storage) else 0,
        )
    )

    cmdline = "{} {} {} {} {} {} {} {} {}".format(
        total_requests,
        concurrency,
        text_bytes,
        mention_count,
        url_count,
        user_count,
        seed,
        warmup_requests,
        1 if _as_bool(verify_storage) else 0,
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
        "raw_{}_{}_c{}_r{}.csv".format(
            scenario,
            target_service,
            concurrency,
            repeat,
        ),
    )
    _write_raw_csv(raw_path, output)
    print("         Raw CSV written to {}".format(raw_path))

    parsed = _parse_benchmark_csv(output)

    result_row = {
        "scenario": scenario,
        "repeat": repeat,
        "target_service": target_service,
        "source_host": source_host or "",
        "dest_host": dest_host or "",
        "client_host": client_host or "",
        "total_requests": total_requests,
        "concurrency": concurrency,
        "text_bytes": text_bytes,
        "mention_count": mention_count,
        "url_count": url_count,
        "user_count": user_count,
        "seed": seed,
        "warmup_requests": warmup_requests,
        "verify_storage": 1 if _as_bool(verify_storage) else 0,
        "successes": parsed["successes"],
        "failures": parsed["failures"],
        "duration_ns": parsed["duration_ns"],
        "throughput_rps": parsed["throughput_rps"],
        "p50_ns": parsed["p50_ns"],
        "p90_ns": parsed["p90_ns"],
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
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    trigger_after_s=3.0,
    scenario="live_migration",
    target_service=TARGET_SERVICE,
    source_host=None,
    dest_host=None,
    client_host=None,
    repeat=0,
    out_dir="compose_migration_results",
):
    """
    Runs one ComposePost disruption benchmark.

    Lifecycle:
      reset planner
      start SNB service stack
      invoke benchmark client in the background
      trigger migration/drain event while client is running
      collect benchmark result
      shutdown services
      wait for stale async planner messages to settle
    """
    print("=== ComposePost Migration Benchmark ===")
    print(
        "scenario={} repeat={} target={} total={} concurrency={} trigger={}s".format(
            scenario,
            repeat,
            target_service,
            total_requests,
            concurrency,
            trigger_after_s,
        )
    )
    print(
        "source_host={} dest_host={} client_host={}".format(
            source_host,
            dest_host,
            client_host,
        )
    )

    if scenario in ("live_migration", "drain_restart"):
        if source_host is None or dest_host is None:
            raise ValueError(
                "{} requires source_host and dest_host".format(scenario)
            )

        if source_host == dest_host:
            raise ValueError(
                "{} requires source_host != dest_host".format(scenario)
            )

    result_row = None

    event_info = {
        "event_success": 1 if scenario == "none" else 0,
        "event_start_s": 0,
        "event_end_s": 0,
        "event_duration_s": 0,
        "event_endpoint": "",
        "target_app_id": "",
        "target_msg_id": "",
    }

    placements = _default_placements(
        source_host=source_host,
        dest_host=dest_host,
        target_service=target_service,
    )

    try:
        started = _start_service_stack(
            ini_file=ini_file,
            num_workers=num_workers,
            placements=placements,
        )

        if target_service not in started:
            raise RuntimeError("Target service {} was not started".format(target_service))

        target_info = started[target_service]

        print("[4/6] Invoking benchmark client in background...")

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _run_client_once,
                ini_file,
                total_requests,
                concurrency,
                text_bytes,
                mention_count,
                url_count,
                user_count,
                seed,
                verify_storage,
                source_host,
                dest_host,
                client_host,
                scenario,
                target_service,
                repeat,
                out_dir,
            )

            print("[5/6] Waiting {}s before event...".format(trigger_after_s))
            sleep(float(trigger_after_s))

            if scenario == "live_migration":
                event_info = _trigger_live_migration(
                    target_info,
                    source_host=source_host,
                    dest_host=dest_host,
                    ini_file=ini_file,
                )
            elif scenario == "drain_restart":
                event_info = _trigger_drain_restart(
                    target_service,
                    source_host=source_host,
                    dest_host=dest_host,
                    ini_file=ini_file,
                )
            elif scenario == "none":
                event_info = {
                    "event_success": 1,
                    "event_start_s": time(),
                    "event_end_s": time(),
                    "event_duration_s": 0,
                    "event_endpoint": target_info["endpoint"],
                    "target_app_id": target_info["app_id"],
                    "target_msg_id": target_info["msg_id"],
                }
            else:
                raise ValueError("Unsupported scenario: {}".format(scenario))

            print("[6/6] Waiting for benchmark client...")
            result_row = future.result()

        result_row.update(
            {
                "trigger_after_s": float(trigger_after_s),
                "event_start_rel_ns_approx": int(float(trigger_after_s) * 1e9),
                "event_success": int(event_info["event_success"]),
                "event_duration_s": event_info["event_duration_s"],
                "event_endpoint": event_info["event_endpoint"],
                "target_app_id": event_info["target_app_id"],
                "target_msg_id": event_info["target_msg_id"],
                "zero_failures": 1 if int(result_row["failures"]) == 0 else 0,
                # This means the harness/event/client completed. Per-request
                # failures are reported separately.
                "success": 1
                if int(result_row["return_code"]) == 0
                and int(event_info["event_success"]) == 1
                else 0,
            }
        )

        summary_path = os.path.join(out_dir, "summary.csv")
        _append_summary_csv(summary_path, result_row)
        print("         Summary appended to {}".format(summary_path))

    finally:
        _shutdown_service_stack(ini_file=ini_file)

    return result_row


def run_sweep(
    ini_file=None,
    num_workers=2,
    total_requests=1000,
    concurrencies=(1, 2, 4, 8, 16, 32),
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    trigger_after_s=3.0,
    scenario="live_migration",
    target_service=TARGET_SERVICE,
    source_host=None,
    dest_host=None,
    client_host=None,
    repeats=1,
    out_dir="compose_migration_results",
):
    """
    Runs the concurrency sweep for one scenario.

    Scenarios:
      none:
        no disruption event
      live_migration:
        evict source_host and wait for target_service to migrate to dest_host
      drain_restart:
        shutdown target_service and start a fresh instance on dest_host
    """
    rows = []

    print("=== ComposePost Migration Benchmark Sweep ===")
    print(
        "scenario={} target={} total={} concurrencies={} repeats={}".format(
            scenario,
            target_service,
            total_requests,
            concurrencies,
            repeats,
        )
    )
    print(
        "source_host={} dest_host={} client_host={}".format(
            source_host,
            dest_host,
            client_host,
        )
    )

    for concurrency in concurrencies:
        for repeat in range(int(repeats)):
            row = run_once(
                ini_file=ini_file,
                num_workers=int(num_workers),
                total_requests=int(total_requests),
                concurrency=int(concurrency),
                text_bytes=int(text_bytes),
                mention_count=int(mention_count),
                url_count=int(url_count),
                user_count=int(user_count),
                seed=int(seed) + repeat,
                warmup_requests=int(warmup_requests),
                verify_storage=_as_bool(verify_storage),
                trigger_after_s=float(trigger_after_s),
                scenario=scenario,
                target_service=target_service,
                source_host=source_host,
                dest_host=dest_host,
                client_host=client_host,
                repeat=repeat,
                out_dir=out_dir,
            )
            rows.append(row)

    return rows


def run(
    ini_file=None,
    num_workers=2,
    total_requests=1000,
    concurrencies="1,2,4,8,16,32",
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    trigger_after_s=3.0,
    scenario="live_migration",
    target_service=TARGET_SERVICE,
    source_host=None,
    dest_host=None,
    client_host=None,
    repeats=1,
    out_dir="compose_migration_results",
):
    """
    Entry point used by `inv experiment.run compose-migration`.

    Returns True if all runs completed successfully at the harness level.
    Per-request RPC failures are still recorded in summary.csv.
    """
    if isinstance(concurrencies, str):
        concurrencies = tuple(
            int(x.strip())
            for x in concurrencies.split(",")
            if x.strip()
        )

    if scenario in ("live_migration", "drain_restart"):
        if source_host is None or dest_host is None:
            raise ValueError(
                "{} requires source_host and dest_host".format(scenario)
            )

        if source_host == dest_host:
            raise ValueError(
                "{} requires source_host != dest_host".format(scenario)
            )

    rows = run_sweep(
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
        verify_storage=_as_bool(verify_storage),
        trigger_after_s=float(trigger_after_s),
        scenario=scenario,
        target_service=target_service,
        source_host=source_host,
        dest_host=dest_host,
        client_host=client_host,
        repeats=int(repeats),
        out_dir=out_dir,
    )

    ok = True

    for row in rows:
        if row is None:
            ok = False
        elif int(row["success"]) != 1:
            ok = False

    return ok