# EXO Sonde Sensor Data Decoder from LoRaWAN Packets

This application decodes packets received via LoRaWAN, containing measurement data from [EXO Sonde sensor](https://www.xylem.com/siteassets/brand/ysi/resources/manual/exo-user-manual-web.pdf). Once decoded, a lookup table is used to map register codes to readable parameter names. 
Extracted information is compressed and published to Waggle system. 

## Features

- CRC-8 validation to ensure packet integrity.
- Decodes multiple measurement parameters from each packet and extract parameter names.
- Decoded data is grouped by acquisition date and time, reconstructing each complete measurement that was split during transmission.
- Publishes encoded and compressed data.

## How to Use
To run the program,

```bash
# Decode and publish parameter values from EXO Sonde instrument
python3 main.py
```

Then, each sensor measurement is published on topic `rawzb64.data` as base64-encoded, zlib-compressed JSON. Data may contain up to 30 parameter values.


## Example Decoded Output

```json
[
  {
    "Parameter Name": "Date (DDMMYY)",
    "Value": "18-04-2025"
  },
  {
    "Parameter Name": "Time (HHMMSS)",
    "Value": "09:43:38"
  },
  {
    "Parameter Name": "Temperature, C",
    "Value": 21.23
  },
  ...
]
```




