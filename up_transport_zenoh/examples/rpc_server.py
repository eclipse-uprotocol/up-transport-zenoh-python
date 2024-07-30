"""
SPDX-FileCopyrightText: 2024 Contributors to the Eclipse Foundation

See the NOTICE file(s) distributed with this work for additional
information regarding copyright ownership.

This program and the accompanying materials are made available under the
terms of the Apache License Version 2.0 which is available at

    http://www.apache.org/licenses/LICENSE-2.0

SPDX-License-Identifier: Apache-2.0
"""

import asyncio
import time
from datetime import datetime

from uprotocol.communication.inmemoryrpcserver import InMemoryRpcServer
from uprotocol.communication.requesthandler import RequestHandler
from uprotocol.communication.upayload import UPayload
from uprotocol.v1.uattributes_pb2 import (
    UPayloadFormat,
)
from uprotocol.v1.umessage_pb2 import UMessage
from uprotocol.v1.uri_pb2 import UUri

from up_transport_zenoh.examples import common_uuri
from up_transport_zenoh.examples.common_uuri import create_method_uri, get_zenoh_default_config
from up_transport_zenoh.uptransportzenoh import UPTransportZenoh

source = UUri(authority_name="vehicle1", ue_id=18)
transport = UPTransportZenoh.new(get_zenoh_default_config(), source)


class MyRequestHandler(RequestHandler):
    def handle_request(self, msg: UMessage) -> UPayload:
        common_uuri.logging.debug("Request Received by Service Request Handler")
        attributes = msg.attributes
        payload = msg.payload
        source = attributes.source
        sink = attributes.sink
        common_uuri.logging.debug(f"Receive {payload} from {source} to {sink}")
        response_payload = format(datetime.utcnow()).encode('utf-8')
        payload = UPayload(data=response_payload, format=UPayloadFormat.UPAYLOAD_FORMAT_TEXT)
        return payload


async def register_rpc():
    uuri = create_method_uri()
    rpc_server = InMemoryRpcServer(transport)
    status = await rpc_server.register_request_handler(uuri, MyRequestHandler())
    common_uuri.logging.debug(f"Request Handler Register status {status}")

    while True:
        time.sleep(1)


if __name__ == '__main__':
    asyncio.run(register_rpc())
