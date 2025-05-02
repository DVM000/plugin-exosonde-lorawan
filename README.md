# LoRaWAN Listener + EXO Sonde Sensor Data Decoder

This application decodes packets received via LoRaWAN, containing measurement data from [EXO Sonde sensor](https://www.xylem.com/siteassets/brand/ysi/resources/manual/exo-user-manual-web.pdf). Once decoded, a lookup table is used to map register codes to readable parameter names. 
Extracted information is published to Waggle system. 

## Features of Decoder

- CRC-8 validation to ensure packet integrity.
- Decodes multiple measurement parameters from each packet and extract parameter names, units, and values.
- Publishes encoded and compressed data, as well as parameter values along with its actual measurement timestamp in `env.PARAM.UNIT` topics.

## Example Decoded Output

```
[
{'measurements': 
   [   {'name': 'device_id', 'value': 18, 'unit': ''}, 
       {'name': 'version', 'value': 0, 'unit': ''}, 
       {'name': 'Wiper_Peak_Current', 'value': 0.0, 'unit': 'mA'}, 
       {'name': 'Wiper_Position', 'value': 1.1939666271209717, 'unit': 'V'}, 
       {'name': 'Temperature', 'value': 21.257341384887695, 'unit': 'C'}
   ]
}
```

## Example Job Spec

This is an example Job spec for usage with Sage

```
name: W027-lorawan-codec-exosonde3
plugins:
- name: lorawan-codec-exosonde3
  pluginSpec:
    image: http://registry.sagecontinuum.org/deliavm/plugin-exosonde-lorawan:0.1.1
    args:
    - --dev_eui #<--- REQUIRED ARGUMENT
    - 95d6ec7dced4ba39 #<--- add more as needed each in new line 
    - --signal-strength-indicators
    - --plr
    - "3600"
    volume: {}
nodeTags: []
nodes:
  W027: true
scienceRules:
- 'schedule("lorawan-codec-exosonde3"): cronjob("lorawan-codec-exosonde3", "* * *
  * *")'
successCriteria: []

```




