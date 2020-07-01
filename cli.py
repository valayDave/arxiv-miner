'''
This is the Generalised CLI origin of the Project. 
this will be used for the Extracting the Important CLI information such as Database 
Selection etc. Can be used as a gateway to integrate all the submodules into one cli invocation
'''

import click
from functools import wraps
from config import Config
from arxiv_miner import SUPPORTED_DBS,get_database_client
import json

DB_HELP = 'The Chosen Backend Store. Select from : '+','.join(SUPPORTED_DBS)
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
@click.option('--datastore', type=click.Choice(SUPPORTED_DBS),default=Config.default_database, help=DB_HELP)
@click.option('--use_defaults',is_flag=True,help='Use Default Database Configurations For Chosen Datastore. Config currently comes from utils.py')
@common_run_options
@click.pass_context
def db_cli(ctx,datastore,use_defaults,host,port,app_name=DEFAULT_APP_NAME):
    ctx.obj = {}
    # get_database_client will raise error if some-one feeds BS DB
    client_class = get_database_client(datastore)
    if datastore == 'fs':
        if use_defaults:
            args = Config.get_defaults('fs')
        else:
            args = dict(host=host,port=port)
    elif datastore == 'elasticsearch':
        if use_defaults:
            args = Config.get_defaults('elasticsearch')
        else:
            args = dict(index_name=Config.elasticsearch_index,host=host,port=port)
        
    
    print_str = '\n %s Process Using %s Datastore'%(app_name,datastore)
    args_str = ''.join(['\n\t'+ i + ' : ' + str(args[i]) for i in args])
    click.secho(print_str,fg='green',bold=True)
    click.secho(args_str+'\n\n',fg='magenta')
    arxiv_database = client_class(**args)
    ctx.obj['db_class'] = client_class
    ctx.obj['db_args'] = args


if __name__ == '__main__':
    db_cli()