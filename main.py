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
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y/%m/%d %H:%M:%S')

def get_data():
    # REPLACE BY ACTUAL CODE TO GET DATA
    # 1) single example
    hex_str = "00 00 12 40 06 61 48 C0 BE 1D 48 E6 00 6C 19 3D 41 EF 00 00 00 00 00 2F"
    hex_str = "00 00 12 40 06 61 48 40 93 1F 48 EF 00 00 00 00 00 E5 00 B2 CF 98 3F 01 00 98 F4 B6 41 D6"
  
    # byte list
    payload = bytes.fromhex(hex_str)
  
    # 2) Example multiple packets
    all_packets = [
      bytes.fromhex("00 00 12 40 06 61 48 80 98 1F 48 00 78 00 1C 00 00 00 00 00 E6 00 E9 49 3D 41 DB"),
      bytes.fromhex("00 00 12 40 06 61 48 80 98 1F 48 EF 00 00 00 00 00 E5 00 B2 CF 98 3F 01 00 A9 FB B6 41 64"),
      bytes.fromhex("00 00 12 40 06 61 48 80 98 1F 48 02 00 A6 57 92 42 03 00 F4 02 94 43 04 00 1F 53 FA 37 B3"),
      bytes.fromhex("00 00 12 40 06 61 48 80 98 1F 48 05 00 2C 75 F4 3C 06 00 60 76 02 38 07 00 34 CF FE 3C 93"),
      bytes.fromhex("00 00 12 40 06 61 48 80 98 1F 48 ED 00 CA FC 02 38 EE 00 BA D5 FF 3C 0C 00 00 DD F1 B8 3C"), 
      bytes.fromhex("00 00 12 40 06 61 48 80 98 1F 48 11 02 FF FF FF 7F 12 02 FF FF FF 7F 13 02 FF FF FF 7F AF"),
      bytes.fromhex("00 00 12 40 06 61 48 80 98 1F 48 D3 00 09 D9 CE 42 D6 00 09 D9 CE 42 D4 00 5C 4E 0E 41 0C"),
      bytes.fromhex("00 00 12 40 06 61 48 80 98 1F 48 E4 00 9F 20 84 BF E3 00 9B 19 14 BF EC"),
      
      bytes.fromhex("00 00 12 40 06 61 48 80 9D 1F 48 00 78 00 1C 00 00 00 00 00 E6 00 42 09 3D 41 AA"),
      bytes.fromhex("00 00 12 40 06 61 48 80 9D 1F 48 EF 00 00 00 00 00 E5 00 4C D0 98 3F 01 00 27 04 B7 41 1B"),
    ]
  
    all_packets = [
      bytes.fromhex("00 00 12 40 CA 6A 48 80 E7 A4 47 00 78 00 1C 00 00 00 00 00 E6 00 42 09 3D 41 3F"),
      bytes.fromhex("00 00 12 40 CA 6A 48 80 E7 A4 47 EF 00 00 00 00 00 E5 00 E6 D3 98 3F 01 00 CD 0A AA 41 A5"),
  
      bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 00 78 00 1C 00 00 00 00 00 E6 00 18 F9 3C 41 1C"),
      bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 EF 00 00 00 00 00 E5 00 E6 D3 98 3F 01 00 09 0F AA 41 F1"),
      bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 02 00 C4 86 8C 42 03 00 26 34 93 43 04 00 07 21 0A 38 D3 "),
      bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 05 00 41 E4 06 3D 06 00 5E C3 14 38 07 00 CA 46 11 3D 2A "),
      bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 ED 00 DC C7 15 38 EE 00 2D 45 12 3D 0C 00 00 3A FE B8 7B"), 
      bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 11 02 FF FF FF 7F 12 02 FF FF FF 7F 13 02 FF FF FF 7F 7D"),
      bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 D3 00 BC 0A CA 42 D6 00 BC 0A CA 42 D4 00 60 61 0F 41 DB"),
      bytes.fromhex("00 00 12 40 CA 6A 48 00 F2 A4 47 E4 00 8F 9B 89 BF E3 00 DE C1 17 BF 9F"),
      ]
  
    '''all_packets = [
      bytes.fromhex("00 00 12 40 CA 6A 48 00 05 DC 47 00 78 00 1C 00 00 00 00 00 E6 00 E9 49 3D 41 EF 00 00 00 00 00 E5 00 B2 D2 98 3F 01 00 90 97 AC 41 02 00 9A AA 8D 42 03 00 AC 5C 93 43 77"), 
      bytes.fromhex("00 00 12 40 CA 6A 48 00 05 DC 47 04 00 0A 8B 05 38 05 00 C8 69 02 3D 06 00 C3 E4 0E 38 07 00 54 8B 0B 3D ED 00 5F CB 0F 38 EE 00 9B 6C 0C 3D 0C 00 80 40 FA B8 11 02 FF FF FF 7F 46"), 
      bytes.fromhex("00 00 12 40 CA 6A 48 00 05 DC 47 12 02 FF FF FF 7F 13 02 FF FF FF 7F D3 00 9B 56 CC 42 D6 00 9B 56 CC 42 D4 00 9C 1F 10 41 E4 00 4D 13 84 BF E3 00 B7 10 14 BF 3A"), 
      ]  ''' 
    
    #return payload
    return all_packets                  
  
    
def load_lookup_table(csv_file="./register_configuration.csv"):
    # Load the correct lookup table from the uploaded CSV
    lookup_df = pd.read_csv(csv_file)
    filtered_df = lookup_df[(lookup_df['Read Holding Register'] >= 128) & 
                            (lookup_df['Read Holding Register'] <= 159)]
    lookup_dict = dict(zip(filtered_df['Read Holding Register Value'], filtered_df['Specific Parameter']))
    return lookup_dict #  dictionary of parameter names indexed by register code

# -------------------------------
# Decode data
# -------------------------------
def decode_date(value):
    date_str = f"{int(value):06d}"[-6:]   # zero padding until 6 
    try:
        date_obj = datetime.strptime(date_str, "%d%m%y")
        return date_obj.strftime("%d-%m-%Y")
    except ValueError:
        return f"Invalid date: {date_str}"

def decode_time(value):
    try:
        # Convert to string, pad with zeros if needed
        time_str = f"{int(value):06d}"
        time_obj = datetime.strptime(time_str, "%H%M%S")
        return time_obj.strftime("%H:%M:%S")
    except Exception as e:
        return f"Invalid time: {value}"
    
crc8_func = crcmod.mkCrcFun(0x107, initCrc=0x00, rev=False)
 
def verify_crc8(payload, received_crc):  
    calculated_crc = crc8_func(payload)  
    return received_crc == calculated_crc

def process_packet(payload, lookup_dict):
    # --- CRC ---
    crc = payload[-1]
    if not verify_crc8(payload[:-1], crc):
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

    date = decode_date(date_float)
    time = decode_time(time_float)
    
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


def main(args):
    
    # Load lookup table for parameter names
    lookup_dict = load_lookup_table()   
    
    # Load data received via lorawan
    all_packets = get_data()
    
    # Decode each packet to build a complete dataframe
    all_data = []  
    for packet in all_packets:
        result = process_packet(packet, lookup_dict)
        if result:
            date, time, df = result
            all_data.append(df)
            
    if not len(all_data):
        logging.warning("No received data.")
        return 1
    
    df_all = pd.concat(all_data, ignore_index=True)
    
    # Dictionary of dataframes per DATE-TIME measurement
    grouped_dfs = {
        f"{date}_{time}": group for (date,time),group in df_all.groupby(['Date', 'Time'])
    }
    
    # Publish data per DATE-TIME measurement
    for key,df in grouped_dfs.items(): 
        print(f"\nData collected at {key}:")
        print(df[["Parameter","Value"]])
        ts = datetime.strptime(key,'%d-%m-%Y_%H:%M:%S')
        timestamp_ns = int(ts.timestamp()*1e9)
        
        # Publish encoded and compressed data
        json_string = df.drop(["Date","Time"], axis=1).to_json(orient='records') 
        json_bytes = json_string.encode('utf-8')
        rawzb64_data = base64.b64encode(zlib.compress(json_bytes)).decode('utf-8')
    
        with Plugin() as plugin:
            plugin.publish("rawzb64.data", rawzb64_data, timestamp=timestamp_ns)
    
    # Data decoding
    #decoded = zlib.decompress(base64.b64decode(rawzb64_data))
    #json_restored = decoded.decode('utf-8')
    #df_restored = pd.read_json(json_restored)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #parser.add_argument()
    args = parser.parse_args()
    main(args)
       



