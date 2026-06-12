import csv
import os
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
SERVICE_QUIESCE_PERIOD_S = 5
SERVICE_STOP_TIMEOUT_S = 30
SERVICE_MOVE_TIMEOUT_S = 120

# Policy used for fixed placement / warmup. Preloaded decisions still force the
# exact placement, but using spot avoids the service policy interfering before
# the policy event.
LOCALITY_STATIC_POLICY = "spot"

# Policy under test.
LOCALITY_POLICY = "service"

LOCALITY_SCENARIOS = ("static_bad", "static_good", "policy")


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
        line
        for line in output.splitlines()
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
        "target_service",

        "client_host",
        "affinity_host",
        "bad_host",
        "aux_host",

        "initial_endpoint",
        "final_endpoint",
        "event_success",
        "event_duration_s",
        "moved_host",
        "moved_to_expected",
        "target_app_id",
        "target_msg_id",

        "telemetry_requests",
        "telemetry_successes",
        "telemetry_failures",

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
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

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


def _parse_worker_hosts(worker_hosts):
    """
    Expected order:
      worker_hosts[0] = client host and ComposePostService host
      worker_hosts[1] = affinity/good host for UserDbService
      worker_hosts[2] = bad initial UserService host
      worker_hosts[3] = auxiliary services host

    With four total workers, the benchmark client is not isolated. It runs on
    worker_hosts[0], which also hosts ComposePostService.
    """
    if isinstance(worker_hosts, str):
        hosts = tuple(
            h.strip()
            for h in worker_hosts.split(",")
            if h.strip()
        )
    else:
        hosts = tuple(worker_hosts)

    if len(hosts) != 4:
        raise ValueError(
            "compose-locality requires exactly 4 worker hosts, got {}: {}".format(
                len(hosts),
                hosts,
            )
        )

    if len(set(hosts)) != 4:
        raise ValueError("worker hosts must be distinct: {}".format(hosts))

    return hosts


def _locality_roles(worker_hosts):
    hosts = _parse_worker_hosts(worker_hosts)

    return {
        "client_host": hosts[0],
        "compose_host": hosts[0],
        "affinity_host": hosts[1],
        "bad_host": hosts[2],
        "aux_host": hosts[3],
    }


def _locality_placements(scenario, roles):
    """
    Four-worker placement design:

      compose_host:
        ComposePostService
        benchmark client also runs here

      affinity_host:
        UserDbService
        UserService after successful policy migration

      bad_host:
        UserService initially, for static_bad and policy

      aux_host:
        TextService
        UniqueIdService
        PostStorageService

    The expected policy decision is to migrate UserService from bad_host to
    affinity_host after observing the UserService -> UserDbService dependency.
    """
    if scenario not in LOCALITY_SCENARIOS:
        raise ValueError("Unsupported locality scenario: {}".format(scenario))

    placements = {
        "ComposePostService": roles["compose_host"],
        "UserDbService": roles["affinity_host"],
        "TextService": roles["aux_host"],
        "UniqueIdService": roles["aux_host"],
        "PostStorageService": roles["aux_host"],
    }

    if scenario == "static_good":
        placements["UserService"] = roles["affinity_host"]
    else:
        placements["UserService"] = roles["bad_host"]

    return placements


def invoke_wasm_no_wait_placed(
    user,
    func,
    ini_file=None,
    host=None,
):
    """
    Like invoke_wasm_no_wait, but optionally preloads a scheduling decision so
    the long-running service starts on a specific worker.
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

    return {
        "app_id": app_id,
        "group_id": group_id,
        "msg_id": msg_id,
        "req": req,
    }


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


def _wait_for_service_moved_from(user, func, source_host, ini_file=None):
    deadline = time() + SERVICE_MOVE_TIMEOUT_S

    while time() < deadline:
        endpoint = discover_service(user, func, ini_file=ini_file)
        endpoint_host = _endpoint_host(endpoint)

        if endpoint is not None and endpoint_host is not None:
            if endpoint_host != source_host:
                print(
                    "         {}/{} moved from {} to {}".format(
                        user,
                        func,
                        source_host,
                        endpoint,
                    )
                )
                return endpoint

        print(
            "         Waiting for {}/{} to move away from {} "
            "(current endpoint={})...".format(
                user,
                func,
                source_host,
                endpoint,
            )
        )
        sleep(DISCOVER_POLL_PERIOD_S)

    raise RuntimeError(
        "Timed out waiting for {}/{} to move away from {}".format(
            user,
            func,
            source_host,
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

    start_info = invoke_wasm_no_wait_placed(
        SERVICE_USER,
        service_func,
        ini_file=ini_file,
        host=service_host,
    )

    app_id = start_info["app_id"]
    group_id = start_info["group_id"]
    msg_id = start_info["msg_id"]

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
        "req": start_info["req"],
    }


def _start_service_stack(
    ini_file=None,
    num_workers=4,
    placements=None,
    planner_policy=LOCALITY_STATIC_POLICY,
):
    print("[1/5] Resetting planner and waiting for {} workers...".format(num_workers))
    reset(expected_num_workers=num_workers, verbose=True)

    print("[2/5] Setting scheduler policy to {}...".format(planner_policy))
    set_planner_policy(planner_policy)

    print("[3/5] Starting SNB services...")

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


def _trigger_locality_policy_migration(
    target_info,
    source_host,
    expected_host,
    ini_file=None,
):
    """
    Trigger migration and let the service locality scheduler choose the
    destination.

    This deliberately does not preload a destination. If the scheduler is
    working, it should choose expected_host based on collected RPC telemetry.
    """
    service_func = target_info["function"]

    print(
        "[event] Locality policy migration: {}/{} appId={} msgId={} "
        "source={} expected={}".format(
            SERVICE_USER,
            service_func,
            target_info["app_id"],
            target_info["msg_id"],
            source_host,
            expected_host,
        )
    )

    event_start_s = time()

    print("         Setting scheduler policy to {}...".format(LOCALITY_POLICY))
    set_planner_policy(LOCALITY_POLICY)
    sleep(0.5)

    print("         Evicting source host {}...".format(source_host))
    set_next_evicted_host([source_host])

    moved_endpoint = _wait_for_service_moved_from(
        SERVICE_USER,
        service_func,
        source_host,
        ini_file=ini_file,
    )

    moved_host = _endpoint_host(moved_endpoint)
    moved_to_expected = (
        moved_host == expected_host or expected_host in str(moved_endpoint)
    )

    if not moved_to_expected:
        raise RuntimeError(
            "Locality policy moved {}/{} to {}, expected {}".format(
                SERVICE_USER,
                service_func,
                moved_endpoint,
                expected_host,
            )
        )

    event_end_s = time()

    print(
        "         Policy moved {}/{} to expected host: {}".format(
            SERVICE_USER,
            service_func,
            moved_endpoint,
        )
    )

    return {
        "event_success": 1,
        "event_start_s": event_start_s,
        "event_end_s": event_end_s,
        "event_duration_s": event_end_s - event_start_s,
        "event_endpoint": str(moved_endpoint),
        "moved_host": moved_host,
        "moved_to_expected": 1 if moved_to_expected else 0,
        "target_app_id": target_info["app_id"],
        "target_msg_id": target_info["msg_id"],
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
    client_host=None,
    scenario="unknown",
    target_service=TARGET_SERVICE,
    repeat=0,
    out_dir="compose_locality_results",
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

    print("         return_code={} output_len={}".format(ret_code, len(output)))
    print("         output head: {!r}".format(output[:300]))

    parsed = _parse_benchmark_csv(output)

    result_row = {
        "scenario": scenario,
        "repeat": repeat,
        "target_service": target_service,
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
    worker_hosts=None,
    num_workers=4,
    scenario="static_bad",
    target_service=TARGET_SERVICE,
    total_requests=5000,
    concurrency=4,
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=50,
    verify_storage=False,
    telemetry_requests=1000,
    telemetry_concurrency=4,
    repeat=0,
    out_dir="compose_locality_results",
):
    """
    Runs one locality-policy experiment.

    Scenarios:
      static_bad:
        UserService starts on the bad host and stays there.

      static_good:
        UserService starts on the affinity host and stays there.

      policy:
        UserService starts on the bad host. A telemetry warmup is run, then the
        planner policy is switched to "service" and the bad host is evicted.
        The locality scheduler should migrate UserService to the affinity host.
    """
    if scenario not in LOCALITY_SCENARIOS:
        raise ValueError(
            "Unsupported locality scenario {}. Expected one of {}".format(
                scenario,
                LOCALITY_SCENARIOS,
            )
        )

    roles = _locality_roles(worker_hosts)
    placements = _locality_placements(scenario, roles)

    print("=== ComposePost Locality Policy Benchmark ===")
    print(
        "scenario={} repeat={} target={} total={} concurrency={}".format(
            scenario,
            repeat,
            target_service,
            total_requests,
            concurrency,
        )
    )
    print("roles={}".format(roles))
    print("placements={}".format(placements))

    telemetry_row = None

    event_info = {
        "event_success": 1,
        "event_duration_s": 0,
        "event_endpoint": "",
        "moved_host": "",
        "moved_to_expected": 0,
        "target_app_id": "",
        "target_msg_id": "",
    }

    # Start all scenarios under the static policy. For the policy scenario, we
    # switch to service mode only after collecting telemetry.
    planner_policy = LOCALITY_STATIC_POLICY

    try:
        started = _start_service_stack(
            ini_file=ini_file,
            num_workers=int(num_workers),
            placements=placements,
            planner_policy=planner_policy,
        )

        if target_service not in started:
            raise RuntimeError(
                "Target service {} was not started".format(target_service)
            )

        target_info = started[target_service]
        initial_endpoint = target_info["endpoint"]

        if scenario == "policy":
            print("[4/5] Running telemetry warmup before policy migration...")

            telemetry_row = _run_client_once(
                ini_file=ini_file,
                total_requests=int(telemetry_requests),
                concurrency=int(telemetry_concurrency),
                text_bytes=int(text_bytes),
                mention_count=int(mention_count),
                url_count=int(url_count),
                user_count=int(user_count),
                seed=int(seed),
                warmup_requests=0,
                verify_storage=_as_bool(verify_storage),
                client_host=roles["client_host"],
                scenario="policy_telemetry",
                target_service=target_service,
                repeat=repeat,
                out_dir=out_dir,
            )

            if int(telemetry_row["failures"]) != 0:
                raise RuntimeError(
                    "Telemetry warmup had {} failures".format(
                        telemetry_row["failures"]
                    )
                )

            print("[event] Triggering locality-policy migration...")

            event_info = _trigger_locality_policy_migration(
                target_info,
                source_host=roles["bad_host"],
                expected_host=roles["affinity_host"],
                ini_file=ini_file,
            )

            # Give discovery and any late planner messages time to settle.
            sleep(1.0)

        print("[5/5] Running measured ComposePost benchmark...")

        measured_row = _run_client_once(
            ini_file=ini_file,
            total_requests=int(total_requests),
            concurrency=int(concurrency),
            text_bytes=int(text_bytes),
            mention_count=int(mention_count),
            url_count=int(url_count),
            user_count=int(user_count),
            seed=int(seed) + int(repeat),
            warmup_requests=int(warmup_requests),
            verify_storage=_as_bool(verify_storage),
            client_host=roles["client_host"],
            scenario=scenario,
            target_service=target_service,
            repeat=repeat,
            out_dir=out_dir,
        )

        if scenario == "policy":
            final_endpoint = event_info["event_endpoint"]
        else:
            final_endpoint = discover_service(
                SERVICE_USER,
                target_service,
                ini_file=ini_file,
            )

        measured_row.update(
            {
                "success": 1 if int(measured_row["return_code"]) == 0 else 0,
                "zero_failures": 1 if int(measured_row["failures"]) == 0 else 0,

                "client_host": roles["client_host"],
                "compose_host": roles["compose_host"],
                "affinity_host": roles["affinity_host"],
                "bad_host": roles["bad_host"],
                "aux_host": roles["aux_host"],

                "initial_endpoint": initial_endpoint,
                "final_endpoint": str(final_endpoint),

                "event_success": int(event_info["event_success"]),
                "event_duration_s": event_info["event_duration_s"],
                "moved_host": event_info["moved_host"],
                "moved_to_expected": event_info["moved_to_expected"],
                "target_app_id": event_info["target_app_id"],
                "target_msg_id": event_info["target_msg_id"],

                "telemetry_requests": int(telemetry_requests)
                if scenario == "policy"
                else 0,
                "telemetry_successes": int(telemetry_row["successes"])
                if telemetry_row is not None
                else 0,
                "telemetry_failures": int(telemetry_row["failures"])
                if telemetry_row is not None
                else 0,
            }
        )

        summary_path = os.path.join(out_dir, "summary.csv")
        _append_summary_csv(summary_path, measured_row)
        print("         Locality summary appended to {}".format(summary_path))

    finally:
        _shutdown_service_stack(ini_file=ini_file)

    return measured_row


def run_sweep(
    ini_file=None,
    worker_hosts=None,
    num_workers=4,
    scenario="static_bad",
    target_service=TARGET_SERVICE,
    total_requests=5000,
    concurrencies=(4,),
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=50,
    verify_storage=False,
    telemetry_requests=1000,
    telemetry_concurrency=4,
    repeats=5,
    out_dir="compose_locality_results",
):
    rows = []

    print("=== ComposePost Locality Policy Benchmark Sweep ===")
    print(
        "scenario={} target={} total={} concurrencies={} repeats={}".format(
            scenario,
            target_service,
            total_requests,
            concurrencies,
            repeats,
        )
    )
    print("worker_hosts={}".format(worker_hosts))

    for concurrency in concurrencies:
        for repeat in range(int(repeats)):
            row = run_once(
                ini_file=ini_file,
                worker_hosts=worker_hosts,
                num_workers=int(num_workers),
                scenario=scenario,
                target_service=target_service,
                total_requests=int(total_requests),
                concurrency=int(concurrency),
                text_bytes=int(text_bytes),
                mention_count=int(mention_count),
                url_count=int(url_count),
                user_count=int(user_count),
                seed=int(seed),
                warmup_requests=int(warmup_requests),
                verify_storage=_as_bool(verify_storage),
                telemetry_requests=int(telemetry_requests),
                telemetry_concurrency=int(telemetry_concurrency),
                repeat=repeat,
                out_dir=out_dir,
            )
            rows.append(row)

    return rows


def run(
    ini_file=None,
    worker_hosts=None,
    num_workers=4,
    scenario="static_bad",
    target_service=TARGET_SERVICE,
    total_requests=5000,
    concurrencies="4",
    text_bytes=128,
    mention_count=2,
    url_count=1,
    user_count=100,
    seed=1,
    warmup_requests=50,
    verify_storage=False,
    telemetry_requests=1000,
    telemetry_concurrency=4,
    repeats=5,
    out_dir="compose_locality_results",
):
    """
    Entry point for:

      faasmctl experiment.run compose-locality

    Required:
      --worker-hosts h0,h1,h2,h3

    Host order:
      h0 = client host
      h1 = affinity/good host
      h2 = bad UserService host
      h3 = auxiliary services host
    """
    if worker_hosts is None:
        raise ValueError("compose-locality requires --worker-hosts h0,h1,h2,h3")

    if isinstance(concurrencies, str):
        concurrencies = tuple(
            int(x.strip())
            for x in concurrencies.split(",")
            if x.strip()
        )

    rows = run_sweep(
        ini_file=ini_file,
        worker_hosts=worker_hosts,
        num_workers=int(num_workers),
        scenario=scenario,
        target_service=target_service,
        total_requests=int(total_requests),
        concurrencies=concurrencies,
        text_bytes=int(text_bytes),
        mention_count=int(mention_count),
        url_count=int(url_count),
        user_count=int(user_count),
        seed=int(seed),
        warmup_requests=int(warmup_requests),
        verify_storage=_as_bool(verify_storage),
        telemetry_requests=int(telemetry_requests),
        telemetry_concurrency=int(telemetry_concurrency),
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