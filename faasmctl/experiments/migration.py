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
SERVICE_STOP_TIMEOUT_S = 30
SERVICE_MOVE_TIMEOUT_S = 120
SERVICE_QUIESCE_PERIOD_S = 5

POLICY = "spot"


def _as_bool(value):
    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value != 0

    if value is None:
        return False

    s = str(value).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _normalise_output(output):
    if isinstance(output, bytes):
        return output.decode("utf-8")
    return str(output)


def _endpoint_host(endpoint):
    if endpoint is None:
        return None

    # DiscoverServiceResponse.endpoint is usually a protobuf with a host field.
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


def _parse_compose_csv(output):
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

    file_exists = os.path.exists(path)

    fieldnames = [
        "scenario",
        "repeat",
        "target_service",
        "source_host",
        "dest_host",
        "client_host",
        "trigger_after_s",
        "event_start_rel_ns_approx",
        "event_duration_s",
        "event_start_s",
        "event_end_s",
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

        if not file_exists:
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
                "         Warning while checking shutdown of {}/{}: {}".format(
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
            "         {}/{} still discoverable at {}, waiting...".format(
                user,
                func,
                endpoint,
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
        host = _endpoint_host(endpoint)

        if endpoint is not None and host == expected_host:
            print(
                "         {}/{} now discoverable on {}".format(
                    user,
                    func,
                    endpoint,
                )
            )
            return endpoint

        # Fall back to substring matching in case endpoint is represented
        # differently by the Python protobuf wrapper.
        if endpoint is not None and expected_host in str(endpoint):
            print(
                "         {}/{} now discoverable on {}".format(
                    user,
                    func,
                    endpoint,
                )
            )
            return endpoint

        print(
            "         waiting for {}/{} to move to {} "
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


def _start_service(user, func, host=None, ini_file=None):
    """
    Start one long-running RPC service.

    If host is provided, this preloads the initial scheduling decision for the
    same request that is then sent via EXECUTE_BATCH. This mirrors the
    distributed migration tests: one service request, one app/message identity.
    """
    print("[service] starting {}/{} host={}".format(user, func, host or ""))

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
    # host_list/preload behaviour.
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
                "Error preloading initial placement for {}/{} "
                "appId={} groupId={} msgId={} host={} "
                "(code={}): {}".format(
                    user,
                    func,
                    app_id,
                    group_id,
                    msg_id,
                    host,
                    response.status_code,
                    response.text,
                )
            )

    response = post(url, data=exec_msg, timeout=None)

    if response.status_code != 200:
        raise RuntimeError(
            "Failed to schedule service {}/{} "
            "appId={} groupId={} msgId={} "
            "(code={}): {}".format(
                user,
                func,
                app_id,
                group_id,
                msg_id,
                response.status_code,
                response.text,
            )
        )

    endpoint = _wait_for_service(user, func, ini_file=ini_file)

    if host is not None:
        endpoint_host = _endpoint_host(endpoint)

        if endpoint_host != host and host not in str(endpoint):
            raise RuntimeError(
                "Expected {}/{} on host {}, discovered {}".format(
                    user,
                    func,
                    host,
                    endpoint,
                )
            )

    print("          ready endpoint={}".format(endpoint))

    return {
        "user": user,
        "func": func,
        "app_id": app_id,
        "group_id": group_id,
        "msg_id": msg_id,
        "endpoint": str(endpoint),
        "host": host or "",
    }


def _shutdown_service_quietly(user, func, ini_file=None):
    print("[cleanup] shutting down {}/{}...".format(user, func))

    try:
        shutdown_service(user, func, ini_file=ini_file)
        print("          shutdown request sent")
    except Exception as e:
        print("          warning: {}".format(e))

    _wait_for_service_gone(user, func, ini_file=ini_file)


def _default_placements(source_host, dest_host, target_service):
    """
    For clean UserService migration/restart experiments, isolate the target
    service on source_host and place the rest of the SNB stack on dest_host.

    This matters because SET_NEXT_EVICTED_VM evicts a host, not one service.
    """
    if source_host is None or dest_host is None:
        return {}

    placements = {}

    for service in SERVICES:
        placements[service] = dest_host

    placements[target_service] = source_host

    return placements


def _start_snb_stack(
    ini_file=None,
    num_workers=2,
    placements=None,
):
    if placements is None:
        placements = {}

    print("[1/6] resetting planner and waiting for {} workers...".format(num_workers))
    reset(expected_num_workers=num_workers, verbose=True)

    print("[2/6] setting scheduler policy to {}...".format(POLICY))
    set_planner_policy(POLICY)

    print("[3/6] starting SNB services...")

    started = {}

    for func in SERVICES:
        host = placements.get(func)

        info = _start_service(
            SERVICE_USER,
            func,
            host=host,
            ini_file=ini_file,
        )

        started[func] = info

    return started


def _shutdown_snb_stack(ini_file=None):
    for func in reversed(SERVICES):
        _shutdown_service_quietly(SERVICE_USER, func, ini_file=ini_file)

    print("[cleanup] quiescing for {}s...".format(SERVICE_QUIESCE_PERIOD_S))
    sleep(SERVICE_QUIESCE_PERIOD_S)


def _migrate_service_by_eviction(
    service_info,
    source_host,
    dest_host,
    ini_file=None,
):
    """
    Trigger live migration through the SPOT path.

    The service has already been started using a preloaded initial placement.
    This function only evicts the source host and waits for the service
    discovery entry to move.
    """
    user = service_info["user"]
    func = service_info["func"]

    print(
        "[event] live migration by eviction: {}/{} "
        "appId={} msgId={} {} -> {}".format(
            user,
            func,
            service_info["app_id"],
            service_info["msg_id"],
            source_host,
            dest_host,
        )
    )

    event_start_s = time()

    set_next_evicted_host([source_host])

    endpoint = _wait_for_service_on_host(
        user,
        func,
        dest_host,
        ini_file=ini_file,
    )

    event_end_s = time()

    return {
        "event": "live_migration",
        "service": func,
        "app_id": service_info["app_id"],
        "msg_id": service_info["msg_id"],
        "source_host": source_host,
        "dest_host": dest_host,
        "event_start_s": event_start_s,
        "event_end_s": event_end_s,
        "event_duration_s": event_end_s - event_start_s,
        "endpoint": str(endpoint),
    }


def _drain_restart_service(
    service_func,
    old_host=None,
    new_host=None,
    ini_file=None,
):
    """
    Non-migrating baseline.

    The old long-running service is shut down, waited for, and then a fresh
    instance is started on the destination host.
    """
    print(
        "[event] drain/restart {} old_host={} new_host={}".format(
            service_func,
            old_host or "",
            new_host or "",
        )
    )

    event_start_s = time()

    shutdown_service(SERVICE_USER, service_func, ini_file=ini_file)
    _wait_for_service_gone(SERVICE_USER, service_func, ini_file=ini_file)

    event_mid_s = time()

    restarted = _start_service(
        SERVICE_USER,
        service_func,
        host=new_host,
        ini_file=ini_file,
    )

    event_end_s = time()

    return {
        "event": "drain_restart",
        "service": service_func,
        "app_id": restarted["app_id"],
        "msg_id": restarted["msg_id"],
        "source_host": old_host or "",
        "dest_host": new_host or "",
        "event_start_s": event_start_s,
        "event_mid_s": event_mid_s,
        "event_end_s": event_end_s,
        "event_duration_s": event_end_s - event_start_s,
        "restart_duration_s": event_end_s - event_mid_s,
        "endpoint": restarted["endpoint"],
    }


def _run_compose_client_sync(
    ini_file=None,
    total_requests=1000,
    concurrency=8,
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    client_host=None,
):
    cmdline = "{} {} {} {} {} {} {} {} {}".format(
        total_requests,
        concurrency,
        text_bytes,
        mention_count,
        url_count,
        user_count,
        seed,
        warmup_requests,
        1 if verify_storage else 0,
    )

    host_list = [client_host] if client_host is not None else None

    print(
        "[client] invoking {}/{} cmdline='{}' host={}".format(
            BENCHMARK_USER,
            BENCHMARK_FUNC,
            cmdline,
            client_host or "",
        )
    )

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

    return {
        "return_code": message_result.returnValue,
        "output": _normalise_output(message_result.outputData),
    }


def run_once(
    ini_file=None,
    num_workers=2,
    scenario="drain_restart",
    repeat=0,
    total_requests=1000,
    concurrency=8,
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    trigger_after_s=3.0,
    source_host=None,
    dest_host=None,
    client_host=None,
    target_service=TARGET_SERVICE,
    out_dir="compose_migration_results",
):
    """
    Run one ComposePost disruption benchmark.

    Scenarios:
      - none:
          start stack, run benchmark, no disruption
      - live_migration:
          start target service on source_host, start other services on dest_host,
          run benchmark, evict source_host, wait for service discovery on dest_host
      - drain_restart:
          start target service on source_host, start other services on dest_host,
          run benchmark, shutdown target service, restart it on dest_host
    """
    scenario = str(scenario)

    print("=== SNB ComposePost disruption benchmark ===")
    print(
        "scenario={} repeat={} target={} total={} concurrency={} "
        "trigger_after={}s".format(
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
            source_host or "",
            dest_host or "",
            client_host or "",
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

    placements = _default_placements(
        source_host=source_host,
        dest_host=dest_host,
        target_service=target_service,
    )

    started = None
    client_result = None
    event_info = {
        "event": "none",
        "event_duration_s": 0.0,
        "event_start_s": 0.0,
        "event_end_s": 0.0,
        "endpoint": "",
        "app_id": "",
        "msg_id": "",
    }

    try:
        started = _start_snb_stack(
            ini_file=ini_file,
            num_workers=int(num_workers),
            placements=placements,
        )

        if target_service not in started:
            raise RuntimeError(
                "Target service {} was not started".format(target_service)
            )

        print("[4/6] starting benchmark client in background...")

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _run_compose_client_sync,
                ini_file,
                int(total_requests),
                int(concurrency),
                int(text_bytes),
                int(mention_count),
                int(url_count),
                int(user_count),
                int(seed),
                int(warmup_requests),
                _as_bool(verify_storage),
                client_host,
            )

            print("[5/6] waiting {}s before event...".format(trigger_after_s))
            sleep(float(trigger_after_s))

            if scenario == "live_migration":
                event_info = _migrate_service_by_eviction(
                    started[target_service],
                    source_host=source_host,
                    dest_host=dest_host,
                    ini_file=ini_file,
                )
            elif scenario == "drain_restart":
                event_info = _drain_restart_service(
                    target_service,
                    old_host=source_host,
                    new_host=dest_host,
                    ini_file=ini_file,
                )
            elif scenario == "none":
                event_start_s = time()
                event_info = {
                    "event": "none",
                    "service": target_service,
                    "app_id": started[target_service]["app_id"],
                    "msg_id": started[target_service]["msg_id"],
                    "source_host": source_host or "",
                    "dest_host": dest_host or "",
                    "event_start_s": event_start_s,
                    "event_end_s": event_start_s,
                    "event_duration_s": 0.0,
                    "endpoint": started[target_service]["endpoint"],
                }
            else:
                raise ValueError("Unsupported scenario: {}".format(scenario))

            print("[6/6] waiting for benchmark client result...")
            client_result = future.result()

        output = client_result["output"]
        return_code = client_result["return_code"]

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
        print("      raw CSV written to {}".format(raw_path))

        parsed = _parse_compose_csv(output)

        summary_row = {
            "scenario": scenario,
            "repeat": repeat,
            "target_service": target_service,
            "source_host": source_host or "",
            "dest_host": dest_host or "",
            "client_host": client_host or "",
            "trigger_after_s": float(trigger_after_s),
            "event_start_rel_ns_approx": int(float(trigger_after_s) * 1e9),
            "event_duration_s": event_info.get("event_duration_s", 0.0),
            "event_start_s": event_info.get("event_start_s", 0.0),
            "event_end_s": event_info.get("event_end_s", 0.0),
            "event_endpoint": event_info.get("endpoint", ""),
            "target_app_id": event_info.get(
                "app_id",
                started[target_service]["app_id"],
            ),
            "target_msg_id": event_info.get(
                "msg_id",
                started[target_service]["msg_id"],
            ),
            "total_requests": int(total_requests),
            "concurrency": int(concurrency),
            "text_bytes": int(text_bytes),
            "mention_count": int(mention_count),
            "url_count": int(url_count),
            "user_count": int(user_count),
            "seed": int(seed),
            "warmup_requests": int(warmup_requests),
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
            "return_code": return_code,
        }

        summary_path = os.path.join(out_dir, "summary.csv")
        _append_summary_csv(summary_path, summary_row)
        print("      summary appended to {}".format(summary_path))

        print(
            "      successes={} failures={} throughput={:.2f} rps "
            "p50={:.0f}ns p99={:.0f}ns max={:.0f}ns return_code={}".format(
                parsed["successes"],
                parsed["failures"],
                parsed["throughput_rps"],
                parsed["p50_ns"] or 0,
                parsed["p99_ns"] or 0,
                parsed["max_ns"] or 0,
                return_code,
            )
        )

        return summary_row

    finally:
        _shutdown_snb_stack(ini_file=ini_file)


def run_sweep(
    ini_file=None,
    num_workers=2,
    scenario="drain_restart",
    repeats=3,
    total_requests=1000,
    concurrencies=(1, 2, 4, 8, 16),
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    trigger_after_s=3.0,
    source_host=None,
    dest_host=None,
    client_host=None,
    target_service=TARGET_SERVICE,
    out_dir="compose_migration_results",
):
    rows = []

    for concurrency in concurrencies:
        for repeat in range(int(repeats)):
            row = run_once(
                ini_file=ini_file,
                num_workers=int(num_workers),
                scenario=scenario,
                repeat=repeat,
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
                source_host=source_host,
                dest_host=dest_host,
                client_host=client_host,
                target_service=target_service,
                out_dir=out_dir,
            )

            rows.append(row)

    return rows


def run(
    ini_file=None,
    num_workers=2,
    scenario="drain_restart",
    repeats=3,
    total_requests=1000,
    concurrencies="1,2,4,8,16",
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=100,
    verify_storage=False,
    trigger_after_s=3.0,
    source_host=None,
    dest_host=None,
    client_host=None,
    target_service=TARGET_SERVICE,
    out_dir="compose_migration_results",
):
    """
    Entry point used by experiment runner.

    Returns True if the harness completed and the benchmark functions returned
    zero. Per-request failures are not treated as harness failures because they
    are a measured outcome for drain/restart.
    """
    if isinstance(concurrencies, str):
        concurrencies = tuple(
            int(x.strip())
            for x in concurrencies.split(",")
            if x.strip()
        )

    rows = run_sweep(
        ini_file=ini_file,
        num_workers=int(num_workers),
        scenario=scenario,
        repeats=int(repeats),
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
        source_host=source_host,
        dest_host=dest_host,
        client_host=client_host,
        target_service=target_service,
        out_dir=out_dir,
    )

    ok = True

    for row in rows:
        if row is None:
            ok = False
        elif int(row["return_code"]) != 0:
            ok = False

    return ok