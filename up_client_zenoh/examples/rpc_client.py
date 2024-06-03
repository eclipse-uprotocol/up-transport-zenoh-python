"""
SPDX-FileCopyrightText: 2024 Contributors to the Eclipse Foundation

See the NOTICE file(s) distributed with this work for additional
information regarding copyright ownership.

This program and the accompanying materials are made available under the
terms of the Apache License Version 2.0 which is available at

    http://www.apache.org/licenses/LICENSE-2.0

SPDX-License-Identifier: Apache-2.0
"""

from uprotocol.proto.uattributes_pb2 import CallOptions
from uprotocol.proto.upayload_pb2 import UPayload, UPayloadFormat
from uprotocol.proto.uri_pb2 import UUri

from up_client_zenoh.examples import common_uuri
from up_client_zenoh.examples.common_uuri import ExampleType, authority, entity, get_zenoh_default_config, rpc_resource
from up_client_zenoh.upclientzenoh import UPClientZenoh

rpc_client = UPClientZenoh(get_zenoh_default_config(), authority(), entity(ExampleType.RPC_CLIENT))


def send_rpc_request_to_zenoh():
    # create uuri
    uuri = UUri(entity=entity(ExampleType.RPC_SERVER), resource=rpc_resource())
    # create UPayload
    data = "GetCurrentTime"
    payload = UPayload(length=0, format=UPayloadFormat.UPAYLOAD_FORMAT_TEXT, value=bytes([ord(c) for c in data]))
    # invoke RPC method
    common_uuri.logging.debug(f"Send request to {uuri.entity}/{uuri.resource}")
    response_future = rpc_client.invoke_method(uuri, payload, CallOptions(ttl=1000))
    # process the result
    result = response_future.result()
    if result and isinstance(result.payload.value, bytes):
        data = list(result.payload.value)
        value = ''.join(chr(c) for c in data)
        common_uuri.logging.debug(f"Receive rpc response {value}")
    else:
        common_uuri.logging.debug("Failed to get result from invoke_method.")


if __name__ == '__main__':
    send_rpc_request_to_zenoh()
