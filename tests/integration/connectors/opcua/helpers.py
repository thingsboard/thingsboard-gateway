# import asyncio
# import json
# import math
# import os
# from datetime import datetime, timedelta
# from os import path
# from time import sleep
#
#
# async def await_until(somepredicate, timeout, period=0.25, *args, **kwargs):
#     must_end = datetime.now() + timedelta(seconds=timeout)
#     while datetime.now() < must_end:
#         if somepredicate(*args, **kwargs): return True
#         await asyncio.sleep(period)
#     return False
#
#
# def wait_until(somepredicate, timeout, period=0.25, *args, **kwargs):
#     must_end = datetime.now() + timedelta(seconds=timeout)
#     while datetime.now() < must_end:
#         if somepredicate(*args, **kwargs): return True
#         sleep(period)
#     return False
#
#
# def read_config(filename):
#     with open(path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__))))) + "/data/opcua/" + filename, 'r') as f:
#         return json.load(f)
#
#
# def send_to_storage_to_timeseries(calls, device_name, section_name, var_name):
#     results = []
#     for call in calls:
#         if len(call.args) >= 1:
#             data = call.args[1]
#             if 'deviceName' in data and data['deviceName'] == device_name:
#                 for sample in data.get(section_name, {}):
#                     if section_name == 'attributes':
#                         ts = math.ceil(datetime.now().timestamp() * 1000)
#                         values = sample
#                     else:
#                         ts = sample.get('ts') if 'ts' in sample else -1
#                         values = sample.get('values', {})
#
#                     if var_name in values:
#                         results.append((ts, values[var_name]))
#     return sorted(results, key=lambda tup: tup[0])
#
#
# def list_intersect_ordered(l1, l2):
#     for i in range(min(len(l1), len(l2))):
#         if l1[i] != l2[i]:
#             return False
#     return len(l1) > 0 and len(l2) > 0
