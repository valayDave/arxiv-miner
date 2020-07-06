
import os
import json
import datetime

def dir_exists(dir_path):
  try:
    os.stat(dir_path)
    return True
  except:
    return False

def load_json_from_file(file_path):
    with open(file_path,'r') as f:
        json_file = json.load(f)
    return json_file

def save_json_to_file(json_dict,file_path):
    with open(file_path,'w') as f:
        json.dump(json_dict,f)

def get_date_range_from_today(num_days):
    end_date = datetime.datetime.now()
    start_date = (datetime.datetime.now() - datetime.timedelta(days = num_days))
    return (start_date,end_date)
