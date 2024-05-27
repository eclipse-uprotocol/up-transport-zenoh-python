import time

from uprotocol.proto.uattributes_pb2 import UPriority
from uprotocol.proto.umessage_pb2 import UMessage
from uprotocol.proto.upayload_pb2 import UPayloadFormat, UPayload
from uprotocol.proto.uri_pb2 import UUri
from uprotocol.transport.builder.uattributesbuilder import UAttributesBuilder

from up_client_zenoh.examples import common_uuri
from up_client_zenoh.examples.common_uuri import get_zenoh_config, authority, entity, ExampleType, pub_resource
from up_client_zenoh.upclientzenoh import UPClientZenoh

publisher = UPClientZenoh(get_zenoh_config(), authority(), entity(ExampleType.PUBLISHER))


def publishtoZenoh():
    # create uuri
    uuri = UUri(entity=entity(ExampleType.PUBLISHER), resource=pub_resource())
    cnt = 0
    while True:
        data = f"{cnt}"
        attributes = UAttributesBuilder.publish(uuri, UPriority.UPRIORITY_CS4).build()
        payload = UPayload(value=data.encode('utf-8'), format=UPayloadFormat.UPAYLOAD_FORMAT_TEXT)
        umessage = UMessage(attributes=attributes, payload=payload)
        common_uuri.logging.debug(f"Sending {data} to {uuri}...")
        publisher.send(umessage)
        time.sleep(3)
        cnt += 1


if __name__ == '__main__':
    publishtoZenoh()
