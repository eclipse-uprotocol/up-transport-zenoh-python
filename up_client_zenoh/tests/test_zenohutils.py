"""
SPDX-FileCopyrightText: 2024 Contributors to the Eclipse Foundation

See the NOTICE file(s) distributed with this work for additional
information regarding copyright ownership.

This program and the accompanying materials are made available under the
terms of the Apache License Version 2.0 which is available at

    http://www.apache.org/licenses/LICENSE-2.0

SPDX-License-Identifier: Apache-2.0
"""

import unittest

import pytest
from uprotocol.uri.serializer.uriserializer import UriSerializer

from up_client_zenoh.zenohutils import MessageFlag, ZenohUtils


class TestZenohUtils(unittest.IsolatedAsyncioTestCase):
    @pytest.mark.asyncio
    async def test_to_zenoh_key_string(self):
        authority = "192.168.1.100"
        test_cases = [
            ("/10AB/3/80CD", None, "up/192.168.1.100/10AB/3/80CD/{}/{}/{}/{}"),
            ("//192.168.1.100/10AB/3/80CD", None, "up/192.168.1.100/10AB/3/80CD/{}/{}/{}/{}"),
            (
                "//192.168.1.100/10AB/3/80CD",
                "//192.168.1.101/20EF/4/0",
                "up/192.168.1.100/10AB/3/80CD/192.168.1.101/20EF/4/0",
            ),
            ("//*/FFFF/FF/FFFF", "//192.168.1.101/20EF/4/0", "up/*/*/*/*/192.168.1.101/20EF/4/0"),
            ("//my-host1/10AB/3/0", "//my-host2/20EF/4/B", "up/my-host1/10AB/3/0/my-host2/20EF/4/B"),
            ("//*/FFFF/FF/FFFF", "//my-host2/20EF/4/B", "up/*/*/*/*/my-host2/20EF/4/B"),
        ]
        for src_uri, sink_uri, expected_zenoh_key in test_cases:
            src = UriSerializer().deserialize(src_uri)
            if sink_uri:
                sink = UriSerializer().deserialize(sink_uri)
                result_key1 = ZenohUtils.to_zenoh_key_string(authority, src, sink)
                print("result1 ", result_key1)
                assert result_key1 == expected_zenoh_key
            else:
                result_key2 = ZenohUtils.to_zenoh_key_string(authority, src, None)
                print("result2 ", result_key2)
                assert result_key2 == expected_zenoh_key

    @pytest.mark.asyncio
    async def test_get_listener_message_type(self):
        test_cases = [
            ("//192.168.1.100/10AB/3/80CD", None, MessageFlag.PUBLISH),
            ("//192.168.1.100/10AB/3/80CD", "//192.168.1.101/20EF/4/0", MessageFlag.NOTIFICATION),
            ("//192.168.1.100/10AB/3/0", "//192.168.1.101/20EF/4/B", MessageFlag.REQUEST),
            ("//192.168.1.101/20EF/4/B", "//192.168.1.100/10AB/3/0", MessageFlag.RESPONSE),
            ("//*/FFFF/FF/FFFF", "//192.168.1.100/10AB/3/0", MessageFlag.NOTIFICATION | MessageFlag.RESPONSE),
            ("//*/FFFF/FF/FFFF", "//192.168.1.101/20EF/4/B", MessageFlag.REQUEST),
            ("//192.168.1.100/10AB/3/0", "//*/FFFF/FF/FFFF", MessageFlag.REQUEST),
            ("//192.168.1.101/20EF/4/B", "//*/FFFF/FF/FFFF", MessageFlag.RESPONSE),
            ("//192.168.1.100/10AB/3/80CD", "//*/FFFF/FF/FFFF", MessageFlag.NOTIFICATION),
        ]

        for src_uri, sink_uri, expected_result in test_cases:
            src = UriSerializer().deserialize(src_uri)
            if sink_uri:
                dst = UriSerializer().deserialize(sink_uri)
                assert ZenohUtils.get_listener_message_type(src, dst) == expected_result
            else:
                assert ZenohUtils.get_listener_message_type(src, None) == expected_result


if __name__ == "__main__":
    unittest.main()
