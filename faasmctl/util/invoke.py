from faasmctl.util.batch import batch_exec_factory
from faasmctl.util.config import (
    get_faasm_ini_file,
    get_faasm_planner_host_port,
)
from faasmctl.util.docker import in_docker
from faasmctl.util.gen_proto.faabric_pb2 import BatchExecuteRequestStatus
from faasmctl.util.message import message_factory
from faasmctl.util.planner import prepare_planner_msg
from google.protobuf.json_format import MessageToDict, MessageToJson, Parse
from requests import post
from time import sleep


def invoke_wasm(
    msg_dict,
    num_messages=1,
    req_dict=None,
    dict_out=False,
    ini_file=None,
    host_list=None,
    num_retries=30,
):
    """
    Main entrypoint to invoke an arbitrary message in a Faasm cluster

    Arguments:
    - msg_dict (dict): dict-like object to build a Message Protobuf from
    - num_messages (int): number of said messages to include in the BER
    - req_dict (dict): optional dict-like object to prototype the BER from
    - dict_out (bool): flag to indicate that we expect the result as a JSON
                       instead than as a Message class
    - host_list (array): list of (`num_message`s IPs) where to execute each
                         message. By providing a host list in advance, we
                         are bypassing the planner's scheduling.
                         WARNING: if the host_list breaks the planner's
                         state consistency, the planner will crash, so use
                         this optional argument at your own risk!
    - ini_file (str): path to the cluster's INI file

    Return:
    - The BERStatus result either in a Protobuf class or as a dict if dict_out
      is set
    """
    if req_dict is None:
        req_dict = {"user": msg_dict["user"], "function": msg_dict["function"]}

    req = batch_exec_factory(req_dict, msg_dict, num_messages)
    print(MessageToJson(req, indent=2))
    msg = prepare_planner_msg("EXECUTE_BATCH", MessageToJson(req, indent=None))

    if not ini_file:
        ini_file = get_faasm_ini_file()

    host, port = get_faasm_planner_host_port(ini_file, in_docker())
    url = "http://{}:{}".format(host, port)

    # Work out the number of messages to expect in the result basing on the
    # original message
    expected_num_messages = num_messages
    if "mpi_world_size" in msg_dict:
        expected_num_messages = msg_dict["mpi_world_size"]

    # If provided a host-list, preload the scheduling decision by sending
    # a BER with a rightly-populated messages.executedHost
    if host_list is not None:
        assert len(host_list) == expected_num_messages

        for _ in range(len(req.messages), expected_num_messages):
            req.messages.append(message_factory(msg_dict, req.appId))

        # We preload a scheduling decision by passing a BER with each group
        # index associated to one host in the host list
        for group_idx in range(len(req.messages)):
            req.messages[group_idx].groupIdx = group_idx
            req.messages[group_idx].executedHost = host_list[group_idx]
            group_idx += 1

        preload_msg = prepare_planner_msg(
            "PRELOAD_SCHEDULING_DECISION", MessageToJson(req, indent=None)
        )
        response = post(url, data=preload_msg, timeout=None)
        if response.status_code != 200:
            print(
                "Error preloading scheduling decision (code: {}): {}".format(
                    response.status_code, response.text
                )
            )
            raise RuntimeError("Error preloading scheduling decision!")

    result = invoke_and_await(url, msg, expected_num_messages, num_retries)

    if dict_out:
        return MessageToDict(result)

    return result


def invoke_and_await(url, json_msg, expected_num_messages, num_retries):
    poll_period = 2
    sleep_period_secs = 1.5

    i = 0
    while i < num_retries:
        i += 1
        response = post(url, data=json_msg, timeout=None)
        if response.status_code == 500 and response.text == "No available hosts":
            print("No available hosts, retrying... {}/{}".format(i + 1, num_retries))
            sleep(sleep_period_secs)
            continue
        break

    if response.status_code != 200:
        raise RuntimeError(
            "POST request failed (code: {}): {}".format(
                response.status_code, response.text
            )
        )

    # Response from EXECUTE_BATCH. This should contain at least the appId.
    initial_status = Parse(response.text, BatchExecuteRequestStatus())

    # The planner only needs appId for EXECUTE_BATCH_STATUS.
    poll_status = BatchExecuteRequestStatus()
    poll_status.appId = initial_status.appId

    status_msg = prepare_planner_msg(
        "EXECUTE_BATCH_STATUS",
        MessageToJson(poll_status, indent=None),
    )

    while True:
        sleep(poll_period)

        response = post(url, data=status_msg, timeout=None)
        if response.status_code != 200:
            if response.text == "App not registered in results":
                pass
            else:
                raise RuntimeError(
                    "POST request failed (code: {}): {}".format(
                        response.status_code, response.text
                    )
                )
        else:
            ber_status = Parse(response.text, BatchExecuteRequestStatus())

            # Keep this local/client-side assignment here, after parsing the
            # planner response. Do not send it to the planner.
            ber_status.expectedNumMessages = expected_num_messages

            if ber_status.finished:
                break

    return ber_status


def invoke_wasm_no_wait(msg_dict, req_dict=None, ini_file=None):
    """
    Schedule a long-running service and return (appId, messageId) immediately
    without polling for completion.
    """
    if req_dict is None:
        req_dict = {"user": msg_dict["user"], "function": msg_dict["function"]}

    req = batch_exec_factory(req_dict, msg_dict, 1)
    app_id = req.appId
    msg_id = req.messages[0].id

    msg = prepare_planner_msg("EXECUTE_BATCH", MessageToJson(req, indent=None))

    if not ini_file:
        ini_file = get_faasm_ini_file()

    host, port = get_faasm_planner_host_port(ini_file, in_docker())
    url = "http://{}:{}".format(host, port)

    response = post(url, data=msg, timeout=None)
    if response.status_code != 200:
        print("POST request failed (code: {}): {}".format(
            response.status_code, response.text
        ))
        raise RuntimeError("Failed to schedule service")

    return app_id, msg_id