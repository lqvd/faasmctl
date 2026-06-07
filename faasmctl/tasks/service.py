from faasmctl.util.config import (
    get_faasm_ini_file,
    get_faasm_planner_host_port,
)
from faasmctl.util.gen_proto.planner_pb2 import DiscoverServiceRequest
from faasmctl.util.planner import (
    shutdown_service,
    prepare_planner_msg
)
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
    shutdown_service(user, func, ini_file)