
import os
import json

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
