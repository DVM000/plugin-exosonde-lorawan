import json
import logging
import re
from dateutil import parser

def parse_message_payload(payload_data):

    tmp_dict = json.loads(payload_data)

    return tmp_dict

def Get_Measurement_metadata(message_dict):
    tmp_dict = {}

    #Get static metadata
    tmp_dict['devAddr'] = message_dict.get('devAddr', None)

    #get values from nested dictionary
    deviceInfo_dict = message_dict.get('deviceInfo', None)
    try:
        tmp_dict['tenantId'] = deviceInfo_dict['tenantId']
        tmp_dict['tenantName'] = deviceInfo_dict['tenantName']
        tmp_dict['applicationId'] = deviceInfo_dict['applicationId']
        tmp_dict['applicationName'] = deviceInfo_dict['applicationName']
        tmp_dict['deviceProfileId'] = deviceInfo_dict['deviceProfileId']
        tmp_dict['deviceProfileName'] = deviceInfo_dict['deviceProfileName']
        tmp_dict['deviceName'] = deviceInfo_dict['deviceName']
        tmp_dict['devEui'] = deviceInfo_dict['devEui']
    except:
        logging.error("deviceInfo was not found")

    #get tags
    tags_dict = deviceInfo_dict.get('tags', None)
    try:
        for key, value in tags_dict.items():
            key = clean_string(key)
            tmp_dict[key + "_tag"] = value
    except:
        pass

    return tmp_dict

def clean_message_measurement(measurement):

    #clean name
    measurement["name"] = clean_string(measurement["name"])

    return measurement

def clean_string(txt):
    #convert capital letters to lowercase
    txt = txt.lower()

    #pattern excepted 
    pattern = r'[^a-z0-9_]'

    #replace not excepted values with '_' in txt
    txt = re.sub(pattern, '_', txt)

    return txt

def Get_Signal_Performance_values(message_dict):
    tmp_dict = {}

    #Get Lorawan Performance values
    tmp_dict['rxInfo'] = []
    if 'rxInfo' in message_dict:
        for val in message_dict['rxInfo']:
            temp = {
                "gatewayId": val.get('gatewayId', None),
                "rssi": val.get('rssi', None),
                "snr": val.get('snr', None)
            }
            tmp_dict['rxInfo'].append(temp)
    else:
        logging.error("rxInfo was not found")

    txInfo_dict = message_dict.get('txInfo', None)
    if (
        txInfo_dict
        and 'modulation' in txInfo_dict
        and 'lora' in txInfo_dict['modulation']
        and 'spreadingFactor' in txInfo_dict['modulation']['lora']
    ):
        tmp_dict['spreadingfactor'] = txInfo_dict['modulation']['lora']['spreadingFactor']
    else:
        tmp_dict['spreadingfactor'] = None
        logging.error("spreadingFactor was not found")

    tmp_dict['fCnt'] = message_dict.get('fCnt', None)

    return tmp_dict

def Get_Signal_Performance_metadata(message_dict):
    tmp_dict = {}

    #get values from nested dictionary
    deviceInfo_dict = message_dict.get('deviceInfo', None)
    try:
        tmp_dict['deviceName'] = deviceInfo_dict['deviceName']
        tmp_dict['devEui'] = deviceInfo_dict['devEui']
    except:
        logging.error("deviceInfo was not found")

    #get tags
    tags_dict = deviceInfo_dict.get('tags', None)
    try:
        for key, value in tags_dict.items():
            key = clean_string(key)
            tmp_dict[key + "_tag"] = value
    except:
        pass

    return tmp_dict


def convert_time(iso_time):
    # Parse the timestamp string using dateutil.parser
    try:
        datetime_obj = parser.isoparse(iso_time)
    except ValueError as e:
        logging.error(f"Error: {e}")
    else:
        # Convert the datetime object to nanoseconds since the epoch
        total_seconds = datetime_obj.timestamp()
        nanoseconds = int(total_seconds * 1e9)

    return nanoseconds
