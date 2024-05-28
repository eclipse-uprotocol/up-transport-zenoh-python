"""
SPDX-FileCopyrightText: 2024 Contributors to the Eclipse Foundation

See the NOTICE file(s) distributed with this work for additional
information regarding copyright ownership.

This program and the accompanying materials are made available under the
terms of the Apache License Version 2.0 which is available at

    http://www.apache.org/licenses/LICENSE-2.0

SPDX-License-Identifier: Apache-2.0
"""

import json
import logging
from enum import Enum

import zenoh
from uprotocol.proto.uri_pb2 import UAuthority
from uprotocol.proto.uri_pb2 import UEntity
from uprotocol.proto.uri_pb2 import UResource
from uprotocol.uri.factory.uresource_builder import UResourceBuilder

# Configure the logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class ExampleType(Enum):
    PUBLISHER = "publisher"
    SUBSCRIBER = "subscriber"
    RPC_SERVER = "rpc_server"
    RPC_CLIENT = "rpc_client"


def authority() -> UAuthority:
    return UAuthority(name="auth_name", id=bytes([1, 2, 3, 4]))


def entity(example_type: ExampleType) -> UEntity:
    mapping = {ExampleType.PUBLISHER : ("publisher", 1), ExampleType.SUBSCRIBER: ("subscriber", 2),
               ExampleType.RPC_SERVER: ("rpc_server", 3), ExampleType.RPC_CLIENT: ("rpc_client", 4)}
    name, id = mapping[example_type]
    return UEntity(name=name, id=1, version_major=id)


def pub_resource() -> UResource:
    return UResource(name="door", instance="front_left", message="Door", id=5678)


def rpc_resource() -> UResource:
    return UResourceBuilder.for_rpc_request("getTime", 5678)


def get_zenoh_config():
    # start your zenoh router and provide router ip and port
    zenoh_ip = "192.168.29.79"  # zenoh router ip
    zenoh_port = 9090  # zenoh router port
    conf = zenoh.Config()
    if zenoh_ip is not None:
        endpoint = [f"tcp/{zenoh_ip}:{zenoh_port}"]
        logging.debug(f"EEE: {endpoint}")
        conf.insert_json5(zenoh.config.MODE_KEY, json.dumps("client"))
        conf.insert_json5(zenoh.config.CONNECT_KEY, json.dumps(endpoint))
    return conf


# Initialize Zenoh with default configuration
def get_zenoh_default_config():
    # Create a Zenoh configuration object with default settings
    config = zenoh.Config()

    # # Set the mode to Peer (or Router, Client depending on your use case)
    # config = "peer"

    return config
