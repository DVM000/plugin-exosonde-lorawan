# -*- coding: utf-8 -*-

# Decoding data coming from EXO sensor via Arduino + loRaWAN
import sys
import struct
from datetime import datetime
import pandas as pd
import crcmod
from waggle.plugin import Plugin
import base64
import zlib
import os
import logging
import argparse


class Decoder: # This class is used to decode the data received from the sensor

    def decode(self, payload):
        # Decode the payload and return a dictionary with the decoded values in this structure:
        # payload = {
        #   measurements:[
        #   {name:"<measurement name>",value:<measurement value>},
        #   {name:"<measurement name>",value:<measurement value>}, ...
        #   ]
        # }
        # This is a placeholder function. The actual decoding logic should be implemented here.
        
        if isinstance(payload, str):
           try:
               payload = bytes.fromhex(payload)
               print("Converted to bytes: ", payload)
           except Exception as e:
               print("Error converting payload to bytes: ", e)
               return {"error": "invalid hex string"}
            
        self.crc8_func = crcmod.mkCrcFun(0x107, initCrc=0x00, rev=False)
        
        # Load lookup table for parameter names
        lookup_dict = self.load_lookup_table()   
    
        date, time, df = self.process_packet(payload, lookup_dict)
        
        measurements = []
        for _, row in df.iterrows():
            if row['Status'] != "Available": # skip NaN values (status!=0)
                continue
            data = {"name": row['Parameter'].replace(' ','_').split(',')[0], 
                    "value": row['Value']},
            measurements.append( data )
    
        payload = {"measurements": measurements}
        return payload
     
    def load_lookup_table(self, csv_file='../register_configuration.csv' ):
        # Load the correct lookup table from the uploaded CSV
        csv_file = os.path.abspath(os.path.join(os.path.dirname(__file__), csv_file))
        print(csv_file)
        lookup_df = pd.read_csv(csv_file)
        filtered_df = lookup_df[(lookup_df['Read Holding Register'] >= 128) & 
                            (lookup_df['Read Holding Register'] <= 159)]
        lookup_dict = dict(zip(filtered_df['Read Holding Register Value'], filtered_df['Specific Parameter']))
        return lookup_dict #  dictionary of parameter names indexed by register code

    @staticmethod
    def decode_date(value):
        date_str = f"{int(value):06d}"[-6:]   # zero padding until 6 
        try:
            date_obj = datetime.strptime(date_str, "%d%m%y")
            return date_obj.strftime("%d-%m-%Y")
        except ValueError:
            return f"Invalid date: {date_str}"
    @staticmethod
    def decode_time(value):
        try:
            # Convert to string, pad with zeros if needed
            time_str = f"{int(value):06d}"
            time_obj = datetime.strptime(time_str, "%H%M%S")
            return time_obj.strftime("%H:%M:%S")
        except Exception as e:
            return f"Invalid time: {value}"
    
    def verify_crc8(self, payload, received_crc):  
        calculated_crc = self.crc8_func(payload)  
        return received_crc == calculated_crc

    def process_packet(self, payload, lookup_dict):
        # --- CRC ---
        crc = payload[-1]
        if not self.verify_crc8(payload[:-1], crc):
            logging.error("Incorrect CRC.")
            return None

        # --- HEADER + PARAMETERS ---
        index = 0
        reserved = payload[index];  index += 1
        version  = payload[index];  index += 1
        device_id = payload[index]; index += 1

        date_bytes = payload[index:index+4];  index += 4
        time_bytes = payload[index:index+4];  index += 4

        date_float = struct.unpack('<f', date_bytes)[0]   
        time_float = struct.unpack('<f', time_bytes)[0]

        date = self.decode_date(date_float)
        time = self.decode_time(time_float)
    
        # --- PARAMETERS ---
        decoded_data = []
        while index < len(payload) - 1:
            code = payload[index]; index += 1

            if code == 0:
                status = 0
                value_bytes = payload[index:index+2]; index += 2
                value = int.from_bytes(value_bytes, byteorder='little')
                name = 'Sampling period'
            else:
                status = payload[index]; index += 1
                value_bytes = payload[index:index+4]; index += 4
                value = struct.unpack('<f', value_bytes)[0]
                name = lookup_dict.get(code, f"Unknown (Code {code})")

            status = 'Available' if status == 0 else 'Unavailable'
            value = value if status == 'Available' else None

            decoded_data.append({
                'Date': date,
                'Time': time,
                'Code': code,
                'Parameter': name,
                'Status': status,
                'Value': value
            })
        
        print(f"({date} {time}) Packet from devID={device_id} v.{version}. #parameters: {len(decoded_data)}")

        return date, time, pd.DataFrame(decoded_data)

# TESTING
#decoder = Decoder()
#decoder.decode(bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 EF 00 00 00 00 00 E5 00 E6 D3 98 3F 01 00 09 0F AA 41 F1"))

