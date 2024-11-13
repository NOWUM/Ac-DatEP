import os
import logging
import time
import json

import db_service

import paho.mqtt.client as mqtt


def on_connect(
        client,
        userdata,
        flags,
        reason_code,
        properties):

    msg = f"Succesfully connected with code {reason_code}"
    logging.info(msg)

    subscribe(client)


def on_message(
        client,
        userdata,
        msg):


    logging.info("Received message, processing")

    if "acdvu" in msg.topic:
        logging.info(f"Not processing message with topic {msg.topic}")
        return None

    # convert payload to json / dict
    try:
        payload = json.loads(msg.payload)
    except Exception as e:
        msg = f"Could not convert msg payload to json / dict"
        logging.warning(msg)
        return None

    db_service.process_payload(payload)


def create_configure_client() -> mqtt.Client:

    # read in user and password
    user = os.getenv("TTN_EINZELHANDEL_APP_ID")
    password = os.getenv("TTN_EINZELHANDEL_API_KEY")

    # create client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    # set username and password
    client.username_pw_set( 
        username=user,
        password=password)

    # set on connect function
    client.on_connect = on_connect

    # set on message function
    client.on_message = on_message

    return client


def connect(client: mqtt.Client) -> None:

    # read in host and port parameters
    host = os.getenv("TTN_EINZELHANDEL_HOST", None)
    port = int(os.getenv("TTN_EINZELHANDEL_PORT", None))

    # try 5 times to connect
    for i in range(6):

        try:
            client.connect(
                host=host,
                port=port,
                keepalive=60)

        except Exception as e:

            # log warning after first 4 attempts
            if i < 5:
                msg = f"Could not connect to {host} on port {port}: {e}"
                logging.warning(msg)
                time.sleep(10)

            # log and raise error after 5th attempt
            else:
                msg = f"Could not connect to {host} on "
                msg += f"port {port} after 5 tries: {e}"
                logging.error(msg)
                raise e


def subscribe(client: mqtt.Client) -> None:

    # read in topic to subscribe to
    topic = os.getenv("TTN_EINZELHANDEL_TOPIC", None)

    if not topic:
        logging.warning(f"No topic to subscripe to provided. Using '#' (all).")
        topic = "#"

    # subscribe
    try:
        client.subscribe(topic=topic)
        logging.info(f"Succesfully subscribed to topic {topic}")

    except Exception as e:
        msg = f"Could not subscripe"
        logging.error(msg)
        raise e


def listen_and_process(client: mqtt.Client) -> None:

    try:
        logging.info("Trying to connect to broker and listen for messages...")
        client.loop_forever()

    except Exception as e:
        msg = f"Loop did not start / closed unexpectedly: {e}"
        logging.error(msg)
        raise e
