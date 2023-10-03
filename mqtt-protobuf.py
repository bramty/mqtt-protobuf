import random
import time
from time import strftime
import warnings

from paho.mqtt import client as mqtt_client

from proto_files.send_pb2 import t_payload
from proto_files.receive_pb2 import r_payload

import threading
import queue
from enum import Enum, auto

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

# Broker parameter
broker = 'broker.emqx.io'
port = 1883
keepalive = 60
topic_pub = "test/pub"
topic_sub = "test/sub"

# Generate a Client ID with the subscribe prefix.
client_id_pub = f'grpd-{random.randint(0, 100)}'
client_id_sub = f'grpd-{random.randint(0, 100)}'
client_id_ack = f'grpd-{random.randint(0, 100)}'

# ------ QUEUE CLASSES ------ #
class QueueItemType(Enum):
    ACK = auto()
    UserQuit = auto()

# Seeting up queue
_q_ack = queue.Queue()
_q_subscribe = queue.Queue()

# Limiting the number of subscribe
MAX_COUNT = 30

def connect_mqtt(client_id):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port, keepalive)
    return client

def publish(client, msg, topic):
    time.sleep(1)
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")

def subscribe(client: mqtt_client, topic, t_msg, r_msg):
    def on_message(client, userdata, msg):
        t_msg.ParseFromString(msg.payload)
        print(f"Received `{t_msg.SerializeToString()}` from `{msg.topic}` topic")
        r_msg.id = t_msg.id
        r_msg.timestamp = t_msg.timestamp
        r_msg.ret = "message received"
        # Send receive ACK to Thread ACK
        _q_ack.put_nowait([QueueItemType.ACK, r_msg.SerializeToString()])

    client.subscribe(topic)
    client.on_message = on_message

def execPublish():
    print("Thread Publish Start!")
    client = connect_mqtt(client_id_pub)
    client.loop_start()
    count = 0

    while True:
        time.sleep(0.5)
        if count >= MAX_COUNT:            
            _q_ack.put_nowait([QueueItemType.UserQuit])
            _q_subscribe.put_nowait([QueueItemType.UserQuit])
            break
        else:
            t_msg.timestamp = strftime("%d%m%y%I%M%S%p")
            t_msg.msg = "test"
            publish(client, t_msg.SerializeToString(), topic_pub)
            count = count + 1
            
    client.loop_stop()  

def execSubscribe():
    print("Thread Subscribe Start!")
    client = connect_mqtt(client_id_sub)
    subscribe(client, topic=topic_pub, t_msg=t_msg, r_msg=r_msg)
    client.loop_start()

    while True:
        # Dequeue an item
        qitem = _q_subscribe.get()
        
        if qitem[0] == QueueItemType.UserQuit:
            break 
            
    client.loop_stop()

def execACK():
    print("Thread ACK Start!")
    client = connect_mqtt(client_id_ack)
    client.loop_start()

    while True:
        time.sleep(0.5)
        
        # Dequeue an item
        qitem = _q_ack.get()

        if qitem[0] == QueueItemType.ACK:
            publish(client, qitem[1], topic_sub)
        elif qitem[0] == QueueItemType.UserQuit:
            break

    client.loop_stop()  


# ------ MAIN CODE ------ #
warnings.filterwarnings("ignore")

thread_publish = threading.Thread(target=execPublish)
thread_publish.start()

thread_subscribe = threading.Thread(target=execSubscribe)
thread_subscribe.start()

thread_ACK = threading.Thread(target=execACK)
thread_ACK.start()
