from contextlib import contextmanager

import os
import re
import pickle
import tempfile
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

# global settings
# -----------------------------------------------------------------------------
class Config(object):
    # Database Related Configuration
    data_path = os.path.abspath('./data')
    database_port = 18861
    database_host = 'localhost'
    database_config = {
        'allow_public_attrs': True,\
        'sync_request_timeout': 10\
    }
    
    # Mining Related Configuration
    detex_path = os.path.abspath('./detex')
    mining_data_path = os.path.abspath('./mining_data/papers')



# Context managers for atomic writes courtesy of
# http://stackoverflow.com/questions/2333872/atomic-writing-to-file-with-python
@contextmanager
def _tempfile(*args, **kws):
    """ Context for temporary file.

    Will find a free temporary filename upon entering
    and will try to delete the file on leaving

    Parameters
    ----------
    suffix : string
        optional file suffix
    """

    fd, name = tempfile.mkstemp(*args, **kws)
    os.close(fd)
    try:
        yield name
    finally:
        try:
            os.remove(name)
        except OSError as e:
            if e.errno == 2:
                pass
            else:
                raise e


@contextmanager
def open_atomic(filepath, *args, **kwargs):
    """ Open temporary file object that atomically moves to destination upon
    exiting.

    Allows reading and writing to and from the same filename.

    Parameters
    ----------
    filepath : string
        the file path to be opened
    fsync : bool
        whether to force write the file to disk
    kwargs : mixed
        Any valid keyword arguments for :code:`open`
    """
    fsync = kwargs.pop('fsync', False)

    with _tempfile(dir=os.path.dirname(filepath)) as tmppath:
        with open(tmppath, *args, **kwargs) as f:
            yield f
            if fsync:
                f.flush()
                os.fsync(f.fileno())
        os.rename(tmppath, filepath)

def safe_pickle_dump(obj, fname):
    with open_atomic(fname, 'wb') as f:
        pickle.dump(obj, f, -1)


# arxiv utils
# -----------------------------------------------------------------------------

def strip_version(idstr):
    """ identity function if arxiv id has no version, otherwise strips it. """
    parts = idstr.split('v')
    return parts[0]

# "1511.08198v1" is an example of a valid arxiv id that we accept
def isvalidid(pid):
  return re.match('^\d+\.\d+(v\d+)?$', pid)
