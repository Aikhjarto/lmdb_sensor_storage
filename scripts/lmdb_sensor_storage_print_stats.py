#!/usr/bin/python3
import requests
import json
import pandas as pd

r = requests.request(url='http://heizung.private.lan:8000/stat.json', method='GET')
data_dict = json.loads(r.content)
df = pd.DataFrame.from_dict(data_dict['sensors']).T


pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
print(df.sort_values(by=['until']))
