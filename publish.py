import random
import time
from time import strftime

from paho.mqtt import client as mqtt_client

from proto_files.send_pb2 import t_payload

# Initialize protobuf class
t_msg = t_payload()
t_msg.id = "001"
t_msg.timestamp = ""
t_msg.msg = ""

# MQTT broker parameters
broker = 'broker.emqx.io'
port = 1883
topic = "test/pub"
timeout = 60
# Generate a Client ID with the publish prefix.
client_id = f'grpd-{random.randint(0, 100)}'

# Limiting the number of publish
MAX_COUNT = 30

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port, 60)
    return client

def publish(client, msg):
    time.sleep(1)
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")

def run():
    client = connect_mqtt()
    client.loop_start()
    count = 0

    while True:
        time.sleep(0.5)
        if count >= MAX_COUNT:            
            break
        else:
            t_msg.timestamp = strftime("%d%m%y%I%M%S%p")
            t_msg.msg = "test"
            publish(client, t_msg.SerializeToString())
            count = count + 1
            
    client.loop_stop()

if __name__ == '__main__':
    run()