import logging
import os
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
            logging.info("Connected to MQTT broker")
            client.subscribe(self.args.mqtt_subscribe_topic)
        else:
            logging.error(f"Connection to MQTT broker failed with code {rc}") 
        return

    @staticmethod
    def on_subscribe(client, obj, mid, granted_qos):
        logging.info("Subscribed: " + str(mid) + " " + str(granted_qos))
        return

    @staticmethod
    def on_log(client, obj, level, string):
        logging.debug(string) #prints if args.debug = true
        return

    def publish_message(self, client, userdata, message):
        self.log_message(message) #log message

        try: #get metadata and payload received
            metadata = parse_message_payload(message.payload.decode("utf-8"))
            payload = metadata["data"]
        except:
            logging.error("Message did not contain data.")
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
        measurements = self.decoder.decode(payload)

        # publish raw payload if requested
        if self.args.publish_raw_payload:
            measurements.append({"name": "raw_payload", "value": payload})
        
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
                    logging.info(f'{measurement["name"]} published')
                except Exception as e:
                    # If an exception is raised, log an error message
                    logging.error(f'measurement {measurement["name"]} did not publish encountered an error: {str(e)}')
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
        logging.info(data) #log message received

        return

    def log_measurements(self,message):

        try: #get metadata and measurements received
            metadata = parse_message_payload(message.payload.decode("utf-8"))
            measurements = metadata["object"]["measurements"]
        except:
            logging.error("Message did not contain measurements.")
            return

        if self.args.signal_strength_indicators:
            Performance_vals = Get_Signal_Performance_values(metadata)
            Performance_metadata = Get_Signal_Performance_metadata(metadata)
        
        for measurement in measurements:
            # Skip the measurement if it's in the ignore list
            if measurement["name"] in self.args.ignore:
                continue
            if self.args.collect: #true if not empty
                if measurement["name"] in self.args.collect: #if not empty only log measurements in list
                    logging.info(str(measurement["name"]) + ": " + str(measurement["value"]))
            else: #else collect is empty so log all measurements
                    logging.info(str(measurement["name"]) + ": " + str(measurement["value"]))

        if self.args.signal_strength_indicators:
            for val in Performance_vals['rxInfo']:
                logging.info("gatewayId: " + str(val["gatewayId"]))
                logging.info("  rssi: " + str(val["rssi"]))
                logging.info("  snr: " + str(val["snr"]))
            logging.info("spreading factor: " + str(Performance_vals["spreadingfactor"]))
            pl,plr = self.plr_calc.process_packet(Performance_metadata['devEui'], Performance_vals['fCnt'])
            logging.info(f"packet loss: {pl}")
            if plr is not None:
                logging.info(f"packet loss rate: {plr:.2f}%")

        return

    def run(self):
        logging.info(f"connecting [{self.args.mqtt_server_ip}:{self.args.mqtt_server_port}]...")
        self.client.connect(host=self.args.mqtt_server_ip, port=self.args.mqtt_server_port, bind_address="0.0.0.0")
        logging.info("waiting for callback...")
        self.client.loop_forever()
