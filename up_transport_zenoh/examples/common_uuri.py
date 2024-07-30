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
from uprotocol.v1.uri_pb2 import UUri

# Configure the logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class ExampleType(Enum):
    PUBLISHER = "publisher"
    SUBSCRIBER = "subscriber"
    RPC_SERVER = "rpc_server"
    RPC_CLIENT = "rpc_client"


def create_method_uri():
    return UUri(authority_name="Neelam", ue_id=4, ue_version_major=1, resource_id=3)


def get_zenoh_config():
    # start your zenoh router and provide router ip and port
    zenoh_ip = "10.0.3.3"  # zenoh router ip
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

    return config
