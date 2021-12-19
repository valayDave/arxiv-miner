from metaflow import Parameter, IncludeFile
import configparser
from .config import Config
from . import KeywordsTextSearch


class DatabaseParameters:
    host = Parameter(
        "host", default=Config.get_db_defaults()["host"], help="ArxivDatabase Host"
    )
    port = Parameter(
        "port", default=Config.get_db_defaults()["port"], help="ArxivDatabase Port"
    )
    configfile = IncludeFile(
        "configfile",
        required=False,
        help="Path to the ini config file that helps conifgure the app.",
        default=None,
    )

    def get_connection(self):
        if self.configfile is not None:
            config = configparser.ConfigParser()
            config.read_string(str(self.configfile))
            args = dict(
                index_name=config["elasticsearch"]["index"],
                host=config["elasticsearch"]["host"],
                port=None,
            )
            if "port" in config["elasticsearch"]:
                args["port"] = config["elasticsearch"]["port"]
            if "auth" in config["elasticsearch"]:
                args["auth"] = config["elasticsearch"]["auth"].split(" ")
        else:
            args = dict(
                index_name=Config.elasticsearch_index, host=self.host, port=self.port
            )
        return KeywordsTextSearch(**args)
