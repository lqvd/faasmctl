from faasmctl.util.gen_proto.faabric_pb2 import RpcShutdownRequest
from faasmctl.util.planner import discover_service
from faasmctl.util.invoke import invoke_wasm_no_wait
from google.protobuf.json_format import MessageToJson
from invoke import task
from requests import post


@task
def start(ctx, user, func, ini_file=None):
    """
    Start a long-running RPC service and print its appId and messageId
    """
    msg_dict = {
        "user": user,
        "function": func,
        "rpc": True,
        "is_long_running": True,
    }

    app_id, msg_id = invoke_wasm_no_wait(msg_dict, ini_file=ini_file)
    print("appId:     {}".format(app_id))
    print("messageId: {}".format(msg_id))


@task
def shutdown(ctx, user, func, ini_file=None):
    """Shutdown a long-running RPC service"""
    endpoint = discover_service(user, func, ini_file)
    if endpoint is None:
        print("ERROR: service {}/{} not found".format(user, func))
        return 1

    req = RpcShutdownRequest()
    req.targetAppId = endpoint.appId
    req.targetMessageId = endpoint.messageId

    # Shutdown goes directly to the worker, not through the planner
    url = "http://{}:{}".format(endpoint.host, RPC_ASYNC_PORT)
    response = post(url, data=MessageToJson(req, indent=None), timeout=None)
    if response.status_code != 200:
        print("Shutdown failed: {}".format(response.text))
        return 1

    print("Shutdown sent to {}/{} on {}".format(user, func, endpoint.host))