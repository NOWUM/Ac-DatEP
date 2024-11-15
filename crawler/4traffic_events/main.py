import logging
import os

import mqtt_service

logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")

if __name__ == "__main__":

    if logging_level_str == "INFO":
        logging_level = logging.INFO
    elif logging_level_str == "WARNING":
        logging_level = logging.WARNING
    elif logging_level_str == "ERROR":
        logging_level = logging.ERROR

    # logging
    logging.basicConfig(
        level=logging_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S')

    # get mqtt client
    mqtt_client = mqtt_service.create_configure_client()

    # connect with client
    mqtt_service.connect(mqtt_client)

    # listen to mqtt broker and process broadcasted message
    mqtt_service.listen_and_process(mqtt_client)
