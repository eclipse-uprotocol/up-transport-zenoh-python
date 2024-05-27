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

from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.uri_pb2 import UUri
from uprotocol.proto.ustatus_pb2 import UStatus
from uprotocol.transport.ulistener import UListener

from up_client_zenoh.examples import common_uuri
from up_client_zenoh.examples.common_uuri import get_zenoh_config, authority, entity, ExampleType, pub_resource
from up_client_zenoh.upclientzenoh import UPClientZenoh


class MyListener(UListener):

    def on_receive(self, msg: UMessage) -> UStatus:
        common_uuri.logging.debug('on receive called')
        common_uuri.logging.debug(msg.payload.value)
        common_uuri.logging.debug(msg.attributes.__str__())
        return UStatus(message="Received event")


client = UPClientZenoh(get_zenoh_config(), authority(), entity(ExampleType.SUBSCRIBER))


def subscribeToZenoh():
    # create uuri
    uuri = UUri(entity=entity(ExampleType.PUBLISHER), resource=pub_resource())
    client.register_listener(uuri, MyListener())


if __name__ == '__main__':
    subscribeToZenoh()
    while True:
        time.sleep(1)
