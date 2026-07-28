import sys
sys.path.insert(0, '.')
from cron import handler
import json
import os

request = {'headers': {}, 'query': {}, 'body': None, 'method': 'GET'}
if os.getenv('CRON_SECRET'):
    request['headers']['Authorization'] = 'Bearer ' + os.getenv('CRON_SECRET')

result = handler(request)
print(json.dumps(result, indent=2))