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

import logging
from concurrent.futures import Future
from threading import Lock
from typing import Dict, Tuple, Any

import zenoh
from uprotocol.proto.uattributes_pb2 import CallOptions
from uprotocol.proto.uattributes_pb2 import UAttributes, UMessageType
from uprotocol.proto.uattributes_pb2 import UPriority
from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.upayload_pb2 import UPayloadFormat, UPayload
from uprotocol.proto.uri_pb2 import UUri, UAuthority, UEntity
from uprotocol.proto.ustatus_pb2 import UStatus, UCode
from uprotocol.rpc.rpcclient import RpcClient
from uprotocol.transport.builder.uattributesbuilder import UAttributesBuilder
from uprotocol.transport.ulistener import UListener
from uprotocol.transport.utransport import UTransport
from uprotocol.transport.validate.uattributesvalidator import Validators
from uprotocol.uri.factory.uresource_builder import UResourceBuilder
from uprotocol.uri.validator.urivalidator import UriValidator
from zenoh import open as zenoh_open, Subscriber, Session, Encoding, Config, Query, Sample, Value, Queryable

from up_client_zenoh.zenohutils import ZenohUtils

logger = logging.getLogger(__name__)


class UPClientZenoh(UTransport, RpcClient):

    def __init__(self, session: Session, subscriber_map: Dict[Tuple[str, UListener], Subscriber],
                 queryable_map: Dict[Tuple[str, UListener], Queryable], query_map: Dict[str, Query],
                 rpc_callback_map: Dict[str, UListener], uauthority: UAuthority, uentity: UEntity) -> None:
        self.session = session
        self.subscriber_map = subscriber_map
        self.queryable_map = queryable_map
        self.query_map = query_map
        self.rpc_callback_map = rpc_callback_map
        self.source_uuri = UUri(authority=uauthority, entity=uentity)
        self.rpc_callback_lock = Lock()
        self.queryable_lock = Lock()
        self.subscriber_lock = Lock()

    @classmethod
    def new(cls, config: Config, uauthority: UAuthority, uentity: UEntity) -> 'UPClientZenoh':
        session = zenoh_open(config)
        return cls(session, {}, {}, {}, {}, uauthority, uentity)

    def get_response_uuri(self) -> UUri:
        new_source = self.source_uuri
        new_source.resource.CopyFrom(UResourceBuilder.for_rpc_response())
        return new_source

    def send_publish_notification(self, zenoh_key: str, payload: UPayload, attributes: UAttributes) -> UStatus:
        # Get the data from UPayload

        if not payload.value:
            msg = "The data in UPayload should be Data::Value"
            print(f"ERROR: {msg}")
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

        buf = payload.value

        # Transform UAttributes to user attachment in Zenoh
        attachment = ZenohUtils.uattributes_to_attachment(attributes)
        if not attachment:
            msg = "Unable to transform UAttributes to attachment"
            print(f"ERROR: {msg}")
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

        # Map the priority to Zenoh
        priority = ZenohUtils.map_zenoh_priority(attributes.priority)
        if not priority:
            msg = "Unable to map to Zenoh priority"
            print(f"ERROR: {msg}")
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

        try:
            # Simulate sending data
            print(f"Sending data to Zenoh with key: {zenoh_key}")
            print(f"Data: {buf}")
            print(f"Priority: {priority}")
            print(f"Attachment: {attachment}")

            put_builder = self.session.put(keyexpr=zenoh_key, value=buf, attachment=attachment, priority=priority)

            msg = "Successfully sent data to Zenoh"
            print(f"SUCCESS:{msg}")
            return UStatus(code=UCode.OK, message=msg)
        except Exception as e:
            msg = f"Unable to send with Zenoh: {e}"
            print(f"ERROR: {msg}")
            return UStatus(code=UCode.INTERNAL, message=msg)

    def send_request(self, zenoh_key: str, payload: UPayload, attributes: UAttributes) -> UStatus:
        data = payload.value
        if not data:
            msg = "The data in UPayload should be Data::Value"
            print(f"ERROR: {msg}")
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)
        # Transform UAttributes to user attachment in Zenoh
        attachment = ZenohUtils.uattributes_to_attachment(attributes)
        if attachment is None:
            msg = "Unable to transform UAttributes to attachment"
            print(msg)
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

        # Retrieve the callback

        if attributes.source is None:
            msg = "Lack of source address"
            print(msg)
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

        resp_callback = self.rpc_callback_map.get(attributes.source.SerializeToString())
        if resp_callback is None:
            msg = "Unable to get callback"
            print(msg)
            return UStatus(code=UCode.INTERNAL, message=msg)

        def zenoh_callback(reply: Query.reply) -> None:
            if isinstance(reply.sample, Sample):
                sample = reply.sample
                # Get the encoding of UPayload
                encoding = ZenohUtils.to_upayload_format(sample.encoding)
                if encoding is None:
                    msg = "Unable to get the encoding"
                    print(msg)
                    return UStatus(code=UCode.INTERNAL, message=msg)
                # Get UAttribute from the attachment
                attachment = sample.attachment
                if attachment is None:
                    msg = "Unable to get the attachment"
                    print(msg)
                    return UStatus(code=UCode.INTERNAL, message=msg)

                u_attribute = ZenohUtils.attachment_to_uattributes(attachment)
                if u_attribute is None:
                    msg = "Transform attachment to UAttributes failed"
                    print(msg)
                    return UStatus(code=UCode.INTERNAL, message=msg)
                # Create UMessage
                msg = UMessage(attributes=u_attribute, payload=UPayload(length=0, format=encoding, value=sample.payload))
                resp_callback.on_receive(msg)
            else:
                msg = f"Error while parsing Zenoh reply: {reply.error}"
                print(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

        # Send query
        value = Value(payload.value, encoding=Encoding.APP_CUSTOM())
        self.session.get(zenoh_key, lambda reply: zenoh_callback(reply), target=zenoh.QueryTarget.BEST_MATCHING(),
                         value=value)
        msg = "Successfully sent rpc request to Zenoh"
        print(f"SUCCESS:{msg}")
        return UStatus(code=UCode.OK, message=msg)

    def send_response(self, payload: UPayload, attributes: UAttributes) -> UStatus:
        # Transform attributes to user attachment in Zenoh
        attachment = ZenohUtils.uattributes_to_attachment(attributes)
        # Find out the corresponding query from dictionary
        reqid = attributes.reqid
        query = self.query_map.pop(reqid.SerializeToString(), None)
        if not query:
            msg = "Query doesn't exist"
            print(msg)
            return UStatus(code=UCode.INTERNAL, message=msg)  # Send back the query
        value = Value(payload.value, Encoding.APP_CUSTOM())
        reply = Sample(query.key_expr, value, attachment=attachment)

        try:
            query.reply(reply)
            msg = "Successfully sent rpc response to Zenoh"
            print(f"SUCCESS:{msg}")
            return UStatus(code=UCode.OK, message=msg)

        except Exception as e:
            msg = "Unable to reply with Zenoh: {}".format(str(e))
            print(msg)
            return UStatus(code=UCode.INTERNAL, message=msg)

    def register_publish_notification_listener(self, topic: UUri, listener: UListener) -> UStatus:
        # Get Zenoh key
        zenoh_key = ZenohUtils.to_zenoh_key_string(topic)

        # Setup callback
        def callback(sample: Sample) -> None:
            # Get the UAttribute from Zenoh user attachment
            attachment = sample.attachment
            if attachment is None:
                msg = "Unable to get attachment"
                print(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

            u_attribute = ZenohUtils.attachment_to_uattributes(attachment)
            if u_attribute is None:
                msg = "Unable to decode attributes"
                print(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)
            # Create UPayload
            format = ZenohUtils.to_upayload_format(sample.encoding)
            if format:
                u_payload = UPayload(length=0, format=format, value=sample.payload)
                # Create UMessage
                msg = UMessage(attributes=u_attribute, payload=u_payload)
                listener.on_receive(msg)
            else:
                msg = "Unable to get payload encoding"
                print(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

        # Create Zenoh subscriber
        try:

            subscriber = self.session.declare_subscriber(zenoh_key, callback)
            if subscriber:
                with self.subscriber_lock:
                    self.subscriber_map[(topic.SerializeToString(), listener)] = subscriber

            else:
                msg = "Unable to register callback with Zenoh"
                print(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)
        except Exception as e:
            msg = "Unable to register callback with Zenoh"
            print(msg)
            return UStatus(code=UCode.INTERNAL, message=msg)
        msg = "Successfully register callback with Zenoh"
        print(msg)
        return UStatus(code=UCode.OK, message=msg)

    def register_request_listener(self, topic: UUri, listener: UListener) -> UStatus:
        zenoh_key = ZenohUtils.to_zenoh_key_string(topic)

        def callback(query: Query) -> None:
            nonlocal self, listener, topic
            print(query.selector)
            print(query.decode_parameters())
            attachment = query.attachment
            if not attachment:
                msg = "Unable to get attachment"
                print(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

            u_attribute = ZenohUtils.attachment_to_uattributes(attachment)
            if isinstance(u_attribute, UStatus):
                msg = f"Unable to transform user attachment to UAttributes: {u_attribute}"
                print(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

            value = query.value
            if value:
                # todo encoding logic---- not present in eclipse-zenoh pypi where as it is available in zenoh- rust
                #  release
                encoding = ZenohUtils.to_upayload_format(value.encoding)
                if not encoding:
                    msg = "Unable to get payload encoding"
                    print(msg)
                    return UStatus(code=UCode.INTERNAL, message=msg)

                u_payload = UPayload(format=UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF_WRAPPED_IN_ANY, value=value.payload)
            else:
                u_payload = UPayload(format=UPayloadFormat.UPAYLOAD_FORMAT_UNSPECIFIED)

            msg = UMessage(attributes=u_attribute, payload=u_payload)

            self.query_map[u_attribute.id.SerializeToString()] = query
            listener.on_receive(msg)

        def declare_queryable(callback: Any) -> Any:
            nonlocal self, zenoh_key, topic, listener
            try:
                queryable = self.session.declare_queryable(zenoh_key, callback)
                with self.queryable_lock:
                    self.queryable_map[(topic.SerializeToString(), listener)] = queryable
            except Exception as e:
                msg = "Unable to register callback with Zenoh"
                print(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

        declare_queryable(callback)

        return UStatus(code=UCode.OK, message="Successfully register callback with Zenoh")

    def register_response_listener(self, topic: UUri, listener: UListener) -> UStatus:
        with self.rpc_callback_lock:
            self.rpc_callback_map[topic.SerializeToString()] = listener
            return UStatus(code=UCode.OK, message="Successfully register response callback with Zenoh")

    def send(self, message: UMessage) -> UStatus:
        payload = message.payload
        attributes = message.attributes
        # Check the type of UAttributes (Publish / Notification / Request / Response)
        msg_type = attributes.type
        if msg_type == UMessageType.UMESSAGE_TYPE_PUBLISH:
            Validators.PUBLISH.validator().validate(attributes)
            topic = attributes.source
            zenoh_key = ZenohUtils.to_zenoh_key_string(topic)
            return self.send_publish_notification(zenoh_key, payload, attributes)
        elif msg_type == UMessageType.UMESSAGE_TYPE_NOTIFICATION:
            Validators.NOTIFICATION.validator().validate(attributes)
            topic = attributes.sink
            zenoh_key = ZenohUtils.to_zenoh_key_string(topic)
            return self.send_publish_notification(zenoh_key, payload, attributes)

        elif msg_type == UMessageType.UMESSAGE_TYPE_REQUEST:
            Validators.REQUEST.validator().validate(attributes)
            topic = attributes.sink
            zenoh_key = ZenohUtils.to_zenoh_key_string(topic)
            return self.send_request(zenoh_key, payload, attributes)

        elif msg_type == UMessageType.UMESSAGE_TYPE_RESPONSE:
            Validators.RESPONSE.validator().validate(attributes)
            return self.send_response( payload, attributes)

        else:
            return UStatus(code=UCode.INVALID_ARGUMENT, message="Wrong Message type in UAttributes")

    def register_listener(self, topic: UUri, listener: UListener) -> None:
        if topic.authority and not topic.entity and not topic.resource:
            # This is special UUri which means we need to register for all of Publish, Request, and Response
            # RPC response
            # Register for all of Publish, Notification, Request, and Response
            response_status = self.register_response_listener(topic, listener)
            request_status = self.register_request_listener(topic, listener)
            publish_status = self.register_publish_notification_listener(topic, listener)
            if all(status.code == UCode.OK for status in [response_status, request_status, publish_status]):
                return UStatus(code=UCode.OK, message="Successfully register listener with Zenoh")
            else:
                return UStatus(code=UCode.INTERNAL, message="Unsuccessful registration")

        else:
            # Validate topic
            UriValidator.validate(topic)
            status = None
            if UriValidator.is_rpc_response(topic):
                status = self.register_response_listener(topic, listener)
            elif UriValidator.is_rpc_method(topic):
                status = self.register_request_listener(topic, listener)
            else:
                status = self.register_publish_notification_listener(topic, listener)
            if status.code == UCode.OK:
                return UStatus(code=UCode.OK, message="Successfully register listener with Zenoh")
            else:
                return UStatus(code=UCode.INTERNAL, message="Unsuccessful registration")

    def unregister_listener(self, topic: UUri, listener: UListener) -> None:
        remove_pub_listener = False
        remove_req_listener = False
        remove_resp_listener = False
        if topic.authority and not topic.entity and not topic.resource:
            remove_pub_listener = True
            remove_req_listener = True
            remove_resp_listener = True
        else:
            # Validate topic
            UriValidator.validate(topic)
            if UriValidator.is_rpc_response(topic):
                remove_resp_listener = True
            elif UriValidator.is_rpc_method(topic):
                remove_req_listener = True
            else:
                remove_pub_listener = True
        if remove_resp_listener:
            self._remove_response_listener(topic)
        if remove_req_listener:
            self._remove_request_listener(topic, listener)
        if remove_pub_listener:
            self._remove_publish_listener(topic, listener)

    def _remove_response_listener(self, topic: UUri) -> None:
        with self.rpc_callback_lock:
            if self.rpc_callback_map.pop(topic.SerializeToString(), None) is None:
                raise ValueError("RPC response callback doesn't exist")

    def _remove_publish_listener(self, topic: UUri, listener: UListener) -> None:
        with self.subscriber_lock:
            if self.subscriber_map.pop((topic.SerializeToString(), listener), None) is None:
                raise ValueError("Publish listener doesn't exist")

    def _remove_request_listener(self, topic: UUri, listener: UListener) -> None:
        with self.queryable_lock:
            if self.queryable_map.pop((topic.SerializeToString(), listener), None) is None:
                raise ValueError("RPC request listener doesn't exist")

    def invoke_method(self, topic: UUri, payload: UPayload, options: CallOptions, ) -> Future:
        try:
            # Validate UUri
            if not UriValidator.validate(topic):
                msg = "Invalid UUri for invoke_method"
                print(f"{msg}")
                raise UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

            # Get Zenoh key
            zenoh_key_result = ZenohUtils.to_zenoh_key_string(topic)
            if isinstance(zenoh_key_result, UStatus):
                msg = "Unable to transform UUri to Zenoh key"
                print(f"{msg}")
                raise UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

            zenoh_key = zenoh_key_result

            # Create UAttributes and put into Zenoh user attachment
            uattributes = UAttributesBuilder.request(topic, self.get_response_uuri(), UPriority.UPRIORITY_CS4,
                                                     options.ttl).build()

            attachment = ZenohUtils.uattributes_to_attachment(uattributes)

            # Get the data from UPayload
            if not payload.value:
                msg = "The data in UPayload should be Data::Value"
                print(f"{msg}")
                raise UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

            buf = payload.value
            value = Value(buf, encoding=Encoding.APP_CUSTOM())

            # Send the query
            get_builder = self.session.get(zenoh_key, zenoh.Queue(), target=zenoh.QueryTarget.BEST_MATCHING(), value=value,
                                           attachment=attachment)

            for reply in get_builder.receiver:

                if reply.is_ok:
                    encoding = ZenohUtils.to_upayload_format(reply.ok.encoding)
                    if not encoding:
                        msg = "Error while parsing Zenoh encoding"
                        print(f"{msg}")
                        raise UStatus(code=UCode.INTERNAL, message=msg)

                    umessage = UMessage(attributes=uattributes, payload=UPayload(format=encoding, value=reply.ok.payload))
                    future = Future()
                    future.set_result(umessage)
                    return future
                else:
                    msg = f"Error while parsing Zenoh reply: {reply.err}"
                    print(f"{msg}")
                    raise UStatus(code=UCode.INTERNAL, message=msg)

        except Exception as e:
            msg = f"Unexpected error: {e}"
            print(f"{msg}")
            raise UStatus(code=UCode.INTERNAL, message=msg)
