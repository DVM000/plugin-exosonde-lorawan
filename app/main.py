import logging
import argparse
import os
from client import *

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="enable debug logs")
    parser.add_argument(
        "--dry",
        action="store_true",
        default=False,
        help="enable dry-run mode where no messages will be broadcast to Beehive",
    )
    parser.add_argument(
        "--mqtt-server-ip",
        default=os.getenv("MQTT_SERVER_HOST", "wes-rabbitmq"),
        help="MQTT server IP address",
    )
    parser.add_argument(
        "--mqtt-server-port",
        default=os.getenv("MQTT_SERVER_PORT", "1883"),
        help="MQTT server port",
        type=int,
    )
    parser.add_argument(
        "--mqtt-subscribe-topic",
        default=os.getenv("MQTT_SUBSCRIBE_TOPIC", "application/#"),
        help="MQTT subscribe topic",
    )
    parser.add_argument(
        "--collect",
        nargs="*",  # 0 or more values expected => creates a list
        type=str,
        default=[],  # default if nothing is provided
        help="A list of chirpstack measurements to retrieve. If empty all will be retrieved (ex: --collect m1 m2 m3)"
    )
    parser.add_argument(
        "--ignore",
        nargs="*",  # 0 or more values expected => creates a list
        type=str,
        default=[],  # default if nothing is provided
        help="A list of chirpstack measurements to ignore (opposite of --collect). If empty all will be retrieved (ex: --ignore m1 m2 m3)"
    )
    parser.add_argument(
        "--signal-strength-indicators",
        action="store_true",
        default=False,
        help="enable signal strength indicators"
    )
    parser.add_argument(
        "--plr",
        default=os.getenv("PLR", 3600),
        help="plr's(packet loss rate) time interval in seconds, for example 3600 will mean plr will be measured every hour",
        type=int
    )
    parser.add_argument(
        "--dry-raw-payload",
        default=os.getenv("DRY_RAW_PAYLOAD", False),
        help="Pass flag to NOT publish raw payload to beehive",
        type=bool
    )
    
    #get args
    args = parser.parse_args()

    #configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    #configure client
    mqtt_client = My_Client(args)
    mqtt_client.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
