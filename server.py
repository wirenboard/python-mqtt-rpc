#!/usr/bin/python
import argparse

try:
    import mosquitto
except ImportError:
    import paho.mqtt.client as mosquitto

import logging

from mqttrpc import MQTTRPCResponseManager, dispatcher

logging.getLogger().setLevel(logging.DEBUG)


@dispatcher.add_method
def foobar(**kwargs):
    return kwargs["foo"] + kwargs["bar"]


class TMQTTRPCServer:
    def __init__(self, client, driver_id):  # pylint: disable=redefined-outer-name
        self.client = client
        self.driver_id = driver_id

    def on_mqtt_message(self, mosq, obj, msg):  # pylint: disable=unused-argument
        print(msg.topic)
        print(msg.payload)

        parts = msg.topic.split("/")
        service_id = parts[4]
        method_id = parts[5]
        client_id = parts[6]

        response = MQTTRPCResponseManager.handle(msg.payload, service_id, method_id, dispatcher)

        self.client.publish(
            f"/rpc/v1/{self.driver_id}/{service_id}/{method_id}/{client_id}/reply", response.json
        )

    def setup(self):
        for service, method in dispatcher.items():
            self.client.publish(f"/rpc/v1/{self.driver_id}/{service}/{method}", "1", retain=True)

            self.client.subscribe(f"/rpc/v1/{self.driver_id}/{service}/{method}/+")


# Dispatcher is dictionary {<method_name>: callable}
dispatcher[("test", "echo")] = lambda s: s
dispatcher[("test", "add")] = lambda a, b: a + b


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample RPC server", add_help=False)

    parser.add_argument("-h", "--host", dest="host", type=str, help="MQTT host", default="localhost")

    parser.add_argument("-u", "--username", dest="username", type=str, help="MQTT username", default="")

    parser.add_argument("-P", "--password", dest="password", type=str, help="MQTT password", default="")

    parser.add_argument("-p", "--port", dest="port", type=int, help="MQTT port", default="1883")

    args = parser.parse_args()
    client = mosquitto.Mosquitto()

    if args.username:
        client.username_pw_set(args.username, args.password)

    rpc_server = TMQTTRPCServer(client, "Driver")

    client.connect(args.host, args.port)
    client.on_message = rpc_server.on_mqtt_message
    rpc_server.setup()

    while 1:
        rc = client.loop()
        if rc != 0:
            break
