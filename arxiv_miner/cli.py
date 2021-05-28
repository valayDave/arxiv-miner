'''
This is the Generalised CLI origin of the Project. 
this will be used for the Extracting the Important CLI information such as Database 
Selection etc. Can be used as a gateway to integrate all the submodules into one cli invocation
'''

import click
from functools import wraps
import configparser
from .config import Config
from .database import SUPPORTED_DBS,get_database_client
import json

DEFAULT_APP_NAME= 'ArXiv-Miner'

def common_run_options(func):
    db_defaults = Config.get_db_defaults()
    @click.option('--host', default=db_defaults['host'], help='ArxivDatabase Host')
    @click.option('--port', default=db_defaults['port'], help='ArxivDatabase Port')
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@click.group(invoke_without_command=True)
@click.option('--use_defaults',is_flag=True,help='Use Default Database Configurations For Chosen Datastore.')
@click.option('--with-config',default=None,help='Path to configuration ini file to use. Uses a configuration file for the instantiation of the database')
@common_run_options
@click.pass_context
def db_cli(ctx,use_defaults,with_config,host,port,app_name=DEFAULT_APP_NAME):
    ctx.obj = {}    
    args , client_class = database_choice(use_defaults,with_config,host,port)
    print_str = '\n %s Process Using %s Datastore'%(app_name,'elasticsearch')
    args_str = ''.join(['\n\t'+ i + ' : ' + str(args[i]) for i in args])
    click.secho(print_str,fg='green',bold=True)
    click.secho(args_str+'\n\n',fg='magenta')
    ctx.obj['db_class'] = client_class
    ctx.obj['db_args'] = args


def database_choice(use_defaults,with_config,host,port):
    client_class = get_database_client('elasticsearch')
    if with_config is not None:
        config = configparser.ConfigParser()
        config.read(with_config)
        args = dict(index_name=config['elasticsearch']['index'],
                    host=config['elasticsearch']['host']
                    )
        if 'port' in config['elasticsearch']:
            args['port'] = config['elasticsearch']['port']
        if 'auth' in config['elasticsearch']:
            args['auth'] = config['elasticsearch']['auth'].split(' ')
    # get_database_client will raise error if some-one feeds BS DB
    elif use_defaults:
        args = Config.get_defaults('elasticsearch')
    else:
        args = dict(index_name=Config.elasticsearch_index,host=host,port=port)
    return args, client_class

if __name__ == '__main__':
    db_cli()