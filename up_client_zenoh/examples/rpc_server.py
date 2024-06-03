"""
SPDX-FileCopyrightText: 2024 Contributors to the Eclipse Foundation

See the NOTICE file(s) distributed with this work for additional
information regarding copyright ownership.

This program and the accompanying materials are made available under the
terms of the Apache License Version 2.0 which is available at

    http://www.apache.org/licenses/LICENSE-2.0

SPDX-License-Identifier: Apache-2.0
"""

import time
from datetime import datetime

from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.upayload_pb2 import UPayload, UPayloadFormat
from uprotocol.proto.uri_pb2 import UUri
from uprotocol.proto.ustatus_pb2 import UStatus
from uprotocol.transport.builder.uattributesbuilder import UAttributesBuilder
from uprotocol.transport.ulistener import UListener

from up_client_zenoh.examples import common_uuri
from up_client_zenoh.examples.common_uuri import ExampleType, authority, entity, get_zenoh_default_config, rpc_resource
from up_client_zenoh.upclientzenoh import UPClientZenoh

rpc_server = UPClientZenoh(get_zenoh_default_config(), authority(), entity(ExampleType.RPC_SERVER))


class RPCRequestListener(UListener):
    def on_receive(self, msg: UMessage) -> UStatus:
        attributes = msg.attributes
        payload = msg.payload
        value = ''.join(chr(c) for c in payload.value)
        source = attributes.source
        sink = attributes.sink
        common_uuri.logging.debug(f"Receive {value} from {source} to {sink}")
        response_payload = format(datetime.utcnow()).encode('utf-8')
        payload = UPayload(value=response_payload, format=UPayloadFormat.UPAYLOAD_FORMAT_TEXT)
        attributes = UAttributesBuilder.response(msg.attributes).build()
        rpc_server.send(UMessage(attributes=attributes, payload=payload))


if __name__ == '__main__':
    uuri = UUri(entity=entity(ExampleType.RPC_SERVER), resource=rpc_resource())

    common_uuri.logging.debug("Register the listener...")
    rpc_server.register_listener(uuri, RPCRequestListener())

    while True:
        time.sleep(1)
