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

from uprotocol.client.usubscription.v3.inmemoryusubcriptionclient import InMemoryUSubscriptionClient
from uprotocol.transport.ulistener import UListener
from uprotocol.v1.umessage_pb2 import UMessage
from uprotocol.v1.uri_pb2 import UUri
from uprotocol.v1.ustatus_pb2 import UStatus

from up_transport_zenoh.examples import common_uuri
from up_transport_zenoh.examples.common_uuri import get_zenoh_default_config
from up_transport_zenoh.uptransportzenoh import UPTransportZenoh


class MyListener(UListener):
    async def on_receive(self, msg: UMessage) -> None:
        common_uuri.logging.debug('on receive called')
        common_uuri.logging.debug(msg.payload)
        common_uuri.logging.debug(msg.attributes.__str__())
        return UStatus(message="Received event")


source = UUri(authority_name="subscriber", ue_id=9)
transport = UPTransportZenoh.new(get_zenoh_default_config(), source)
# create topic uuri
uuri = UUri(authority_name="publisher", ue_id=1, ue_version_major=1, resource_id=0x8001)


async def subscribe_to_zenoh_if_subscription_service_is_not_running():
    status = await transport.register_listener(uuri, MyListener())
    common_uuri.logging.debug(f"Register Listener status  {status}")
    while True:
        await asyncio.sleep(1)


async def subscribe_if_subscription_service_is_running():
    client = InMemoryUSubscriptionClient(transport)
    status = await client.subscribe(uuri, MyListener())
    common_uuri.logging.debug(f"Register Listener status  {status}")
    while True:
        await asyncio.sleep(1)


if __name__ == '__main__':
    asyncio.run(subscribe_to_zenoh_if_subscription_service_is_not_running())
