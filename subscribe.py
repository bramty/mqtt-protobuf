import random
import time
from time import strftime
import keyboard

from paho.mqtt import client as mqtt_client

from proto_files.send_pb2 import t_payload
from proto_files.receive_pb2 import r_payload

# Initialize protobuf class
# Transmit protobuf
t_msg = t_payload()
t_msg.id = ""
t_msg.timestamp = ""
t_msg.msg = ""
# Receive protobuf
r_msg = r_payload()
r_msg.id = ""
r_msg.timestamp = ""
r_msg.ret = ""

broker = 'broker.emqx.io'
port = 1883
topic = "ascube/pub"
# Generate a Client ID with the subscribe prefix.
client_id = f'grpd-{random.randint(0, 100)}'
# username = 'emqx'
# password = 'public'

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        t_msg.ParseFromString(msg.payload)
        print(f"Received `{t_msg.SerializeToString()}` from `{msg.topic}` topic")
        r_msg.id = t_msg.id
        r_msg.timestamp = t_msg.timestamp
        r_msg.ret = "message received"

    client.subscribe(topic)
    client.on_message = on_message

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_start()

    while True:
        if keyboard.is_pressed("q"):            
            break
        else:
            time.sleep(1)
            print("Receive protobuf class:")
            print(f"id:`{r_msg.id}`\n")
            print(f"timestamp:`{r_msg.timestamp}`\n")
            print(f"return:`{r_msg.ret}`\n")
            
    client.loop_stop()

if __name__ == '__main__':
    run()