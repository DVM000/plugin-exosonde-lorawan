import logging
import os
import base64
import paho.mqtt.client as mqtt
from waggle.plugin import Plugin
from parse import *
from calc import PacketLossCalculator
from decoder import *

class My_Client:
    def __init__(self, args):
        self.args = args
        self.client = self.configure_client()
        self.plr_calc = PacketLossCalculator(self.args.plr)
        self.decoder = Decoder()

    def configure_client(self):
        client_id = self.generate_client_id()
        client = mqtt.Client(client_id)
        client.on_subscribe = self.on_subscribe
        client.on_connect = self.on_connect
        #reconnect_delay_set: 
        # delay is the number of seconds to wait between successive reconnect attempts(default=1).
        # delay_max is the maximum number of seconds to wait between reconnection attempts(default=1)
        client.reconnect_delay_set(min_delay=5, max_delay=60)
        if self.args.dry:
            client.on_message = lambda client, userdata, message: self.dry_message(client, userdata, message)
        else:
            client.on_message = lambda client, userdata, message: self.publish_message(client, userdata, message)
        client.on_log = self.on_log
        return client

    @staticmethod
    def generate_client_id():
        hostname = os.uname().nodename
        process_id = os.getpid()
        return f"{hostname}-{process_id}"

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("[MQTT CLIENT] Connected to MQTT broker")
            client.subscribe(self.args.mqtt_subscribe_topic)
        else:
            logging.error(f"[MQTT CLIENT] Connection to MQTT broker failed with code {rc}") 
        return

    @staticmethod
    def on_subscribe(client, obj, mid, granted_qos):
        logging.info("[MQTT CLIENT] Subscribed: " + str(mid) + " " + str(granted_qos))
        return

    @staticmethod
    def on_log(client, obj, level, string):
        logging.debug(f"[MQTT CLIENT] on_log: {string}") #prints if args.debug = true
        return

    def publish_message(self, client, userdata, message):
        self.log_message(message) #log message

        try: #get metadata and payload received
            metadata = parse_message_payload(message.payload.decode("utf-8"))
            payload = metadata.get("data")
            fport = metadata.get("fport")
            if fport == 0:
                raise ValueError(
                    """
                    fport 0 detected, payload size may be too big for current data rate
                    - At Data Rate 0 (SF10): Maximum payload size is 11 bytes.
                    - At Data Rate 1 (SF9): Maximum payload size is 53 bytes.
                    - At Data Rate 2 (SF8): Maximum payload size is 125 bytes.
                    - At Data Rate 3 (SF7): Maximum payload size is 222 bytes.
                    """)
            elif not payload:
                raise ValueError("Message did not contain data.")
        except (ValueError, KeyError, json.JSONDecodeError) as e:
            logging.error(f"[MQTT CLIENT] Failed to parse message: {e}")
            return

        #get chirpstack time and convert to time in nanoseconds
        timestamp = convert_time(metadata["time"])

        #Get Lorawan signal performance vals
        if self.args.signal_strength_indicators:
            Performance_vals = Get_Signal_Performance_values(metadata)
            Performance_metadata = Get_Signal_Performance_metadata(metadata) 

        #remove measurement metadata
        try:
            Measurement_metadata = Get_Measurement_metadata(metadata)
        except:
            return
        
        #decode payload
        base64_decoded_payload = base64.b64decode(payload)
        decoded_payload = self.decoder.decode(base64_decoded_payload)
        measurements = decoded_payload.get("measurements", [])

        # Check measurements format
        try:
            self.check_measurements(measurements)
        except ValueError as e:
            logging.error(f"[MQTT CLIENT] {e}")
            return

        # if measurements is empty, log
        if not measurements:
            logging.debug(f"[MQTT CLIENT] No measurements returned from Decoder")

        # Don't publish raw payload if requested      
        if not self.args.dry_raw_payload:
            measurements.append({"name": "raw_payload", "value": payload, "unit": "base64"})
        
        for measurement in measurements:
            # Skip the measurement if it's in the ignore list
            if measurement["name"] in self.args.ignore:
                continue
            if self.args.collect: #true if not empty
                if measurement["name"] in self.args.collect: #if not empty only publish measurements in list
                    self.publish_measurement(measurement,timestamp,Measurement_metadata)
            else: #else collect is empty so publish all measurements
                self.publish_measurement(measurement,timestamp,Measurement_metadata)

        if self.args.signal_strength_indicators:
            #snr,pl,plr do not depend on gateway
            self.publish_signal(measurement={"name": "signal.spreadingfactor","value": Performance_vals["spreadingfactor"]},timestamp=timestamp, metadata=Performance_metadata)
            pl,plr = self.plr_calc.process_packet(Performance_metadata['devEui'],Performance_vals['fCnt'])
            self.publish_signal(measurement={"name": "signal.pl","value": pl},timestamp=timestamp, metadata=Performance_metadata)
            if plr is not None:
                self.publish_signal(measurement={"name": "signal.plr","value": plr},timestamp=timestamp, metadata=Performance_metadata)
            for val in Performance_vals['rxInfo']:
                Performance_metadata['gatewayId'] = val["gatewayId"] #add gateway id to metadata since rssi and snr differ per gateway
                self.publish_signal(measurement={"name": "signal.rssi","value": val["rssi"]},timestamp=timestamp, metadata=Performance_metadata)
                self.publish_signal(measurement={"name": "signal.snr","value": val["snr"]},timestamp=timestamp, metadata=Performance_metadata)

        return

    def publish_signal(self, measurement,timestamp,metadata):
        self.publish(measurement,timestamp,metadata)
        return
    
    def publish_measurement(self, measurement,timestamp,metadata):
        metadata["unit"] = measurement["unit"] # add unit to metadata
        measurement = clean_message_measurement(measurement) #clean measurement names
        self.publish(measurement,timestamp,metadata)
        return

    @staticmethod
    def publish(measurement,timestamp,metadata):
        if measurement["value"] is not None: #avoid NULLs
            with Plugin() as plugin: #publish lorawan data
                try:
                    plugin.publish(measurement["name"], measurement["value"], timestamp=timestamp, meta=metadata)
                    # If the function succeeds, log a success message
                    logging.info(f'[MQTT CLIENT] {measurement["name"]} published')
                except Exception as e:
                    # If an exception is raised, log an error message
                    logging.error(f'[MQTT CLIENT] measurement {measurement["name"]} did not publish encountered an error: {str(e)}')
        return

    def dry_message(self, client, userdata, message):

        self.log_message(message)

        self.log_measurements(message)

        return

    @staticmethod
    def log_message(message):
            
        data = (
            "LORAWAN Message received: " + message.payload.decode("utf-8") + " with topic " + str(message.topic)
        )
        logging.info(f"[MQTT CLIENT] {data}") #log message received

        return

    def log_measurements(self,message):

        try: #get metadata and payload received
            metadata = parse_message_payload(message.payload.decode("utf-8"))
            payload = metadata.get("data")
            fport = metadata.get("fport")
            if fport == 0:
                raise ValueError(
                    """
                    fport 0 detected, payload size may be too big for current data rate
                    - At Data Rate 0 (SF10): Maximum payload size is 11 bytes.
                    - At Data Rate 1 (SF9): Maximum payload size is 53 bytes.
                    - At Data Rate 2 (SF8): Maximum payload size is 125 bytes.
                    - At Data Rate 3 (SF7): Maximum payload size is 222 bytes.
                    """)
            elif not payload:
                raise ValueError("Message did not contain data.")
        except (ValueError, KeyError, json.JSONDecodeError) as e:
            logging.error(f"[MQTT CLIENT] Failed to parse message: {e}")
            return

        if self.args.signal_strength_indicators:
            Performance_vals = Get_Signal_Performance_values(metadata)
            Performance_metadata = Get_Signal_Performance_metadata(metadata)
        
        #decode payload
        payload = base64.b64decode(payload)
        decoded_payload = self.decoder.decode(payload)
        measurements = decoded_payload.get("measurements", [])

        # Check measurements format
        try:
            self.check_measurements(measurements)
        except ValueError as e:
            logging.error(f"[MQTT CLIENT] {e}")
            return

        # if measurements is empty, log
        if not measurements:
            logging.debug(f"[MQTT CLIENT] No measurements returned from Decoder")   
        measurements.append({"name": "raw_payload", "value": payload, "unit": "base64"})

        for measurement in measurements:
            # Skip the measurement if it's in the ignore list
            if measurement["name"] in self.args.ignore:
                continue
            if self.args.collect: #true if not empty
                if measurement["name"] in self.args.collect: #if not empty only log measurements in list
                    logging.info("[MQTT CLIENT] " + str(measurement["name"]) + ": " + str(measurement["value"]) + " unit: " + str(measurement["unit"]))
            else: #else collect is empty so log all measurements
                    logging.info("[MQTT CLIENT] " + str(measurement["name"]) + ": " + str(measurement["value"]) + " unit: " + str(measurement["unit"]))

        if self.args.signal_strength_indicators:
            for val in Performance_vals['rxInfo']:
                logging.info("[MQTT CLIENT] gatewayId: " + str(val["gatewayId"]))
                logging.info("[MQTT CLIENT]  rssi: " + str(val["rssi"]))
                logging.info("[MQTT CLIENT]  snr: " + str(val["snr"]))
            logging.info("[MQTT CLIENT] spreading factor: " + str(Performance_vals["spreadingfactor"]))
            pl,plr = self.plr_calc.process_packet(Performance_metadata['devEui'], Performance_vals['fCnt'])
            logging.info(f"[MQTT CLIENT] packet loss: {pl}")
            if plr is not None:
                logging.info(f"[MQTT CLIENT] packet loss rate: {plr:.2f}%")

        return
    
    def check_measurements(self,measurements):
        # log if measurements is not a list
        if not isinstance(measurements, list):
            raise ValueError("Measurements returned from Decoder is not a list")
        
        # Check if each item inside measurements is a dict with 'name', 'value', and 'unit'
        for idx, item in enumerate(measurements):
            if not isinstance(item, dict):
                raise ValueError(f"Invalid measurement at index {idx}: Not a dictionary. Got: {type(item)}")
            
            missing_keys = [key for key in ["name", "value", "unit"] if key not in item]
            if missing_keys:
                raise ValueError(f"Invalid measurement format at index {idx}, missing keys: {missing_keys}. Got: {item}")

    def run(self):
        logging.info(f"[MQTT CLIENT] connecting [{self.args.mqtt_server_ip}:{self.args.mqtt_server_port}]...")
        self.client.connect(host=self.args.mqtt_server_ip, port=self.args.mqtt_server_port, bind_address="0.0.0.0")
        logging.info("[MQTT CLIENT] waiting for callback...")
        self.client.loop_forever()
