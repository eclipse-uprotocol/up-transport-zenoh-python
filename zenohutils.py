# -------------------------------------------------------------------------

# Copyright (c) 2024 General Motors GTO LLC
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# SPDX-FileType: SOURCE
# SPDX-FileCopyrightText: 2024 General Motors GTO LLC
# SPDX-License-Identifier: Apache-2.0

# -------------------------------------------------------------------------

from typing import Union

from uprotocol.proto.uattributes_pb2 import UPriority, UAttributes
from uprotocol.proto.upayload_pb2 import UPayloadFormat
from uprotocol.proto.uri_pb2 import UUri
from uprotocol.proto.ustatus_pb2 import UStatus, UCode
from zenoh import Priority, Encoding
from zenoh.value import Attachment

UATTRIBUTE_VERSION: int = 1


class ZenohListener:
    pass


class ZenohUtils:

    @staticmethod
    def get_uauth_from_uuri(uri: UUri) -> Union[str, UStatus]:
        if uri.authority:
            try:
                authority_bytes = uri.authority.SerializeToString()
                # Iterate over each byte and formate it as a two digit hexa decimal
                return "".join(f"{c:02x}" for c in authority_bytes)
            except Exception as e:
                msg = f"Unable to transform UAuthority into micro form: {e}"
                print(msg)
                return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)
        else:
            msg = "UAuthority is empty"
            print(msg)
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

    @staticmethod
    def to_zenoh_key_string(uri: UUri) -> Union[str, UStatus]:
        if uri.authority and not uri.entity and not uri.resource:
            try:
                authority = ZenohUtils.get_uauth_from_uuri(uri)
                if isinstance(authority, UStatus):
                    return authority
                return f"upr/{authority}/**"
            except Exception as e:
                msg = f"Failed to generate Zenoh key: {e}"
                print(msg)
                return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)
        else:
            try:
                uri_bytes = uri.SerializeToString()
                if len(uri_bytes) > 8:
                    authority_hex = ''.join(format(c, '02x') for c in uri_bytes[8:])
                    micro_zenoh_key = f"upr/{authority_hex}/"
                else:
                    micro_zenoh_key = "upl/"
                rest_hex = ''.join(format(c, '02x') for c in uri_bytes[:8])
                micro_zenoh_key += rest_hex
                return micro_zenoh_key
            except Exception as e:
                msg = f"Failed to generate Zenoh key: {e}"
                print(msg)
                return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

    @staticmethod
    def map_zenoh_priority(upriority: UPriority) -> Priority:
        mapping = {UPriority.UPRIORITY_CS0        : Priority.BACKGROUND(), UPriority.UPRIORITY_CS1: Priority.DATA_LOW(),
                   UPriority.UPRIORITY_CS2        : Priority.DATA(), UPriority.UPRIORITY_CS3: Priority.DATA_HIGH(),
                   UPriority.UPRIORITY_CS4        : Priority.INTERACTIVE_LOW(),
                   UPriority.UPRIORITY_CS5        : Priority.INTERACTIVE_HIGH(),
                   UPriority.UPRIORITY_CS6        : Priority.REAL_TIME(),
                   UPriority.UPRIORITY_UNSPECIFIED: Priority.DATA_LOW()}
        return mapping[upriority]

    @staticmethod
    def to_upayload_format(encoding: Encoding) -> UPayloadFormat:
        try:
            # value = int(encoding.suffix())  # Todo need to check with Luca
            # return UPayloadFormat(value)
            return UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF_WRAPPED_IN_ANY
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def uattributes_to_attachment(uattributes: UAttributes):
        # UATTRIBUTE_VERSION.to_bytes(1, byteorder='little') #Todo first param , check with luca, dict cant have two same keys
        attributes_dict = {"": uattributes.SerializeToString()}
        return attributes_dict

    @staticmethod
    def attachment_to_uattributes(attachment: Attachment) -> UAttributes:
        try:
            # version = None
            # version_found = False
            uattributes = None
            version = UATTRIBUTE_VERSION  # Todo remove once attachment builder class is available
            version_found = True
            items = attachment.items()
            for key, value in items.items():
                if key == b"":
                    # if not version_found:
                    #     version = value
                    #     version_found = True
                    # else:
                    # Process UAttributes data
                    uattributes = UAttributes()
                    uattributes.ParseFromString(value)
                    break

            if version is None:
                msg = f"UAttributes version is empty (should be {UATTRIBUTE_VERSION})"
                print(msg)
                raise UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

            if not version_found:
                msg = f"UAttributes version is missing in the attachment"
                print(msg)
                raise UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

            if version != UATTRIBUTE_VERSION:
                msg = f"UAttributes version is {version} (should be {UATTRIBUTE_VERSION})"
                print(msg)
                raise UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

            if uattributes is None:
                msg = "Unable to get the UAttributes"
                print(msg)
                raise UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

            return uattributes
        except Exception as e:
            msg = f"Failed to convert Attachment to UAttributes: {e}"
            print(msg)
            raise UStatus(code=UCode.INVALID_ARGUMENT, message=msg)
