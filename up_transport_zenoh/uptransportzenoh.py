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
import logging
import threading
from threading import Lock
from typing import Dict, Tuple

import zenoh
from uprotocol.communication.ustatuserror import UStatusError
from uprotocol.transport.ulistener import UListener
from uprotocol.transport.utransport import UTransport
from uprotocol.transport.validator.uattributesvalidator import Validators
from uprotocol.uri.factory.uri_factory import UriFactory
from uprotocol.v1.uattributes_pb2 import UAttributes, UMessageType
from uprotocol.v1.ucode_pb2 import UCode
from uprotocol.v1.umessage_pb2 import UMessage
from uprotocol.v1.uri_pb2 import UUri
from uprotocol.v1.ustatus_pb2 import UStatus
from zenoh import Config, Query, Queryable, Sample, Session, Subscriber, Value
from zenoh.keyexpr import KeyExpr

from up_transport_zenoh.zenohutils import MessageFlag, ZenohUtils

# Configure the logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class UPTransportZenoh(UTransport):
    def get_source(self) -> UUri:
        return self.source

    def close(self) -> None:
        pass

    def __init__(self, session: Session, source: UUri):
        self.session = session
        self.subscriber_map: Dict[Tuple[str, UListener], Subscriber] = {}
        self.queryable_map: Dict[Tuple[str, UListener], Queryable] = {}
        self.query_map: Dict[str, Query] = {}
        self.rpc_callback_map: Dict[str, UListener] = {}
        self.source = source
        self.authority_name = source.authority_name
        self.rpc_callback_lock = Lock()
        self.queryable_lock = Lock()
        self.subscriber_lock = Lock()

    @classmethod
    def new(cls, config: Config, source: UUri):
        try:
            session = zenoh.open(config)
        except Exception:
            msg = "Unable to open Zenoh session"
            logging.error(msg)
            raise UStatus.fail_with_code(UCode.INTERNAL, msg)

        return cls(
            session=session,
            source=source,
        )

    def send_publish_notification(self, zenoh_key: str, payload: bytes, attributes: UAttributes) -> UStatus:
        # Transform UAttributes to user attachment in Zenoh
        attachment = ZenohUtils.uattributes_to_attachment(attributes)
        if not attachment:
            msg = "Unable to transform UAttributes to attachment"
            logging.debug(f"ERROR: {msg}")
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

        # Map the priority to Zenoh
        priority = ZenohUtils.map_zenoh_priority(attributes.priority)
        if not priority:
            msg = "Unable to map to Zenoh priority"
            logging.debug(f"ERROR: {msg}")
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

        try:
            # Simulate sending data
            logging.debug(f"Sending data to Zenoh with key: {zenoh_key}")
            logging.debug(f"Data: {payload}")
            logging.debug(f"Priority: {priority}")
            logging.debug(f"Attachment: {attachment}")

            self.session.put(keyexpr=zenoh_key, value=payload, attachment=attachment, priority=priority)
            msg = "Successfully sent data to Zenoh"
            logging.debug(f"SUCCESS:{msg}")
            return UStatus(code=UCode.OK, message=msg)
        except Exception as e:
            msg = f"Unable to send with Zenoh: {e}"
            logging.debug(f"ERROR: {msg}")
            return UStatus(code=UCode.INTERNAL, message=msg)

    def send_request(self, zenoh_key: str, payload: bytes, attributes: UAttributes) -> UStatus:
        # Transform UAttributes to user attachment in Zenoh
        attachment = ZenohUtils.uattributes_to_attachment(attributes)
        if attachment is None:
            msg = "Unable to transform UAttributes to attachment"
            logging.debug(msg)
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)
        resp_callback = None
        for saved_zenoh_key, listener in self.rpc_callback_map.items():
            keyexpr_zenohkey = KeyExpr.new(zenoh_key)
            keyexpr_savedkey = KeyExpr.new(saved_zenoh_key)

            if keyexpr_zenohkey.intersects(keyexpr_savedkey):
                resp_callback = self.rpc_callback_map.get(saved_zenoh_key)
                break
        if resp_callback is None:
            msg = "Unable to get callback"
            logging.debug(msg)
            return UStatus(code=UCode.INTERNAL, message=msg)

        def handle_response(reply: Query.reply) -> None:
            try:
                sample = reply.ok
                # Get UAttribute from the attachment
                attachment = sample.attachment
                if attachment is None:
                    msg = "Unable to get the attachment"
                    logging.debug(msg)
                    return UStatus(code=UCode.INTERNAL, message=msg)

                u_attribute = ZenohUtils.attachment_to_uattributes(attachment)
                if u_attribute is None:
                    msg = "Transform attachment to UAttributes failed"
                    logging.debug(msg)
                    return UStatus(code=UCode.INTERNAL, message=msg)
                # Create UMessage
                msg = UMessage(attributes=u_attribute, payload=sample.payload)
                asyncio.run(resp_callback.on_receive(msg))
            except Exception:
                msg = f"Error while parsing Zenoh reply: {reply.error}"
                logging.debug(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

        # Send query
        ttl = attributes.ttl / 1000 if attributes.ttl is not None else 1000

        value = Value(payload)
        # Send the query
        get_builder = self.session.get(
            zenoh_key,
            zenoh.Queue(),
            target=zenoh.QueryTarget.BEST_MATCHING(),
            attachment=attachment,
            value=value,
            timeout=ttl,
        )

        def get_response():
            try:
                for reply in get_builder.receiver:
                    if reply.is_ok:
                        handle_response(reply)
                        break
            except Exception:
                pass

        thread = threading.Thread(target=get_response)
        # Start the thread
        thread.start()

        msg = "Successfully sent rpc request to Zenoh"
        logging.debug(f"SUCCESS:{msg}")
        return UStatus(code=UCode.OK, message=msg)

    def send_response(self, payload: bytes, attributes: UAttributes) -> UStatus:
        # Transform attributes to user attachment in Zenoh
        attachment = ZenohUtils.uattributes_to_attachment(attributes)
        if attachment is None:
            msg = "Unable to transform UAttributes to attachment"
            logging.debug(msg)
            return UStatus(code=UCode.INVALID_ARGUMENT, message=msg)

        # Find out the corresponding query from dictionary
        reqid = attributes.reqid

        query = self.query_map.pop(reqid.SerializeToString(), None)
        if not query:
            msg = "Query doesn't exist"
            logging.debug(msg)
            return UStatus(code=UCode.INTERNAL, message=msg)  # Send back the query
        value = Value(payload)
        reply = Sample(query.key_expr, value, attachment=attachment)

        try:
            query.reply(reply)
            msg = "Successfully sent rpc response to Zenoh"
            logging.debug(f"SUCCESS:{msg}")
            return UStatus(code=UCode.OK, message=msg)

        except Exception as e:
            msg = "Unable to reply with Zenoh: {}".format(str(e))
            logging.debug(msg)
            return UStatus(code=UCode.INTERNAL, message=msg)

    def register_publish_notification_listener(self, zenoh_key: str, listener: UListener) -> UStatus:
        def callback(sample: Sample) -> None:
            # Get the UAttribute from Zenoh user attachment
            attachment = sample.attachment
            if attachment is None:
                msg = "Unable to get attachment"
                logging.debug(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)
            try:
                u_attribute = ZenohUtils.attachment_to_uattributes(attachment)
            except UStatusError as error:
                logging.debug(error.get_message())
                return UStatus(code=error.get_code(), message=error.get_message())
            if u_attribute is None:
                msg = "Unable to decode attributes"
                logging.debug(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)
            message = UMessage(attributes=u_attribute, payload=sample.payload)
            asyncio.run(listener.on_receive(message))

        # Create Zenoh subscriber
        try:
            subscriber = self.session.declare_subscriber(zenoh_key, callback)
            if subscriber:
                with self.subscriber_lock:
                    self.subscriber_map[(zenoh_key, listener)] = subscriber
        except Exception:
            msg = "Unable to register callback with Zenoh"
            logging.debug(msg)
            raise UStatus.fail_with_code(UCode.INTERNAL, msg)

        msg = "Successfully register callback with Zenoh"
        logging.debug(msg)
        return UStatus(code=UCode.OK, message=msg)

    def register_request_listener(self, zenoh_key: str, listener: UListener) -> UStatus:
        def callback(query: Query) -> None:
            nonlocal self, listener, zenoh_key
            attachment = query.attachment
            if not attachment:
                msg = "Unable to get attachment"
                logging.debug(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

            try:
                u_attribute = ZenohUtils.attachment_to_uattributes(attachment)
            except UStatusError as error:
                logging.debug(error.get_message())
                return UStatus(code=error.get_code(), message=error.get_message())
            if u_attribute is None:
                msg = "Unable to decode attributes"
                logging.debug(msg)
                return UStatus(code=UCode.INTERNAL, message=msg)

            message = UMessage(attributes=u_attribute, payload=query.value.payload if query.value else None)
            self.query_map[u_attribute.id.SerializeToString()] = query
            asyncio.run(listener.on_receive(message))

        try:
            with self.queryable_lock:
                queryable = self.session.declare_queryable(zenoh_key, callback)
                self.queryable_map[(zenoh_key, listener)] = queryable

        except Exception:
            msg = "Unable to register callback with Zenoh"
            logging.debug(msg)
            return UStatus(code=UCode.INTERNAL, message=msg)

        return UStatus(code=UCode.OK, message="Successfully register callback with Zenoh")

    def register_response_listener(self, zenoh_key: str, listener: UListener) -> UStatus:
        with self.rpc_callback_lock:
            self.rpc_callback_map[zenoh_key] = listener
            return UStatus(code=UCode.OK, message="Successfully register response callback with Zenoh")

    async def send(self, message: UMessage) -> UStatus:
        attributes = message.attributes
        source = attributes.source
        sink = attributes.sink
        zenoh_key = ZenohUtils.to_zenoh_key_string(self.authority_name, source, sink)

        if not source:
            return UStatus(code=UCode.INVALID_ARGUMENT, message="attributes.source shouldn't be empty")
        payload = message.payload or b''
        # Check the type of UAttributes (Publish / Notification / Request / Response)
        msg_type = attributes.type
        if msg_type == UMessageType.UMESSAGE_TYPE_PUBLISH:
            Validators.PUBLISH.validator().validate(attributes)
            return self.send_publish_notification(zenoh_key, payload, attributes)
        elif msg_type == UMessageType.UMESSAGE_TYPE_NOTIFICATION:
            Validators.NOTIFICATION.validator().validate(attributes)
            return self.send_publish_notification(zenoh_key, payload, attributes)

        elif msg_type == UMessageType.UMESSAGE_TYPE_REQUEST:
            Validators.REQUEST.validator().validate(attributes)
            return self.send_request(zenoh_key, payload, attributes)

        elif msg_type == UMessageType.UMESSAGE_TYPE_RESPONSE:
            Validators.RESPONSE.validator().validate(attributes)
            return self.send_response(payload, attributes)

        else:
            return UStatus(code=UCode.INVALID_ARGUMENT, message="Wrong Message type in UAttributes")

    async def register_listener(
        self, source_filter: UUri, listener: UListener, sink_filter: UUri = UriFactory.ANY
    ) -> UStatus:
        flag = ZenohUtils.get_listener_message_type(source_filter, sink_filter)

        # RPC request
        if flag & MessageFlag.REQUEST:
            # Get Zenoh key
            zenoh_key = ZenohUtils.to_zenoh_key_string(self.authority_name, source_filter, sink_filter)
            return self.register_request_listener(zenoh_key, listener)  # RPC response
        if flag & MessageFlag.RESPONSE:
            if sink_filter is not None:
                # Get Zenoh key
                zenoh_key = ZenohUtils.to_zenoh_key_string(self.authority_name, sink_filter, source_filter)
                return self.register_response_listener(zenoh_key, listener)
            else:
                return UStatus(code=UCode.INVALID_ARGUMENT, message="Sink should not be None in Response")
        # Publish & Notification
        if flag & (MessageFlag.PUBLISH | MessageFlag.NOTIFICATION):
            # Get Zenoh key
            zenoh_key = ZenohUtils.to_zenoh_key_string(self.authority_name, source_filter, sink_filter)
            return self.register_publish_notification_listener(zenoh_key, listener)

    async def unregister_listener(
        self, source_filter: UUri, listener: UListener, sink_filter: UUri = UriFactory.ANY
    ) -> UStatus:
        flag = ZenohUtils.get_listener_message_type(source_filter, sink_filter)
        # Publish & Notification
        if flag & (MessageFlag.PUBLISH | MessageFlag.NOTIFICATION):
            # Get Zenoh key
            zenoh_key = ZenohUtils.to_zenoh_key_string(source_filter, sink_filter)
            return self._remove_publish_listener(zenoh_key, listener)
        # RPC request
        if flag & MessageFlag.REQUEST:
            # Get Zenoh key
            zenoh_key = ZenohUtils.to_zenoh_key_string(source_filter, sink_filter)
            return self._remove_request_listener(zenoh_key, listener)  # RPC response
        if flag & MessageFlag.RESPONSE:
            if sink_filter is not None:
                # Get Zenoh key
                zenoh_key = ZenohUtils.to_zenoh_key_string(sink_filter, source_filter)
                return self._remove_response_listener(zenoh_key)
            else:
                return UStatus(code=UCode.INVALID_ARGUMENT, message="Sink should not be None in Response")

    def _remove_response_listener(self, zenoh_key: str) -> UStatus:
        with self.rpc_callback_lock:
            if self.rpc_callback_map.pop(zenoh_key, None) is None:
                msg = "RPC response callback doesn't exist"
                logging.error(msg)
                return UStatus(code=UCode.NOT_FOUND, message=msg)
        return UStatus(code=UCode.OK)

    def _remove_publish_listener(self, zenoh_key: str, listener: UListener) -> UStatus:
        with self.subscriber_lock:
            if self.subscriber_map.pop((zenoh_key, listener), None) is None:
                msg = "Listener not registered for filters: {source_filter}, {sink_filter}"
                logging.error(msg)
                return UStatus(code=UCode.NOT_FOUND, message=msg)

        return UStatus(code=UCode.OK, message="Listener removed successfully")

    def _remove_request_listener(self, zenoh_key: str, listener: UListener) -> UStatus:
        with self.queryable_lock:
            if self.queryable_map.pop((zenoh_key, listener), None) is None:
                msg = "RPC request listener doesn't exist"
                logging.error(msg)
                return UStatus(code=UCode.NOT_FOUND, message=msg)
        return UStatus(code=UCode.OK, message="Listener removed successfully")
