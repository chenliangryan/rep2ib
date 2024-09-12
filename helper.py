import os, json, logging
from enum import Enum, auto


class Source:
    def __init__(self, config_file: str, logger: logging.Logger = None):
        self.__logger = logger
        self.__config_file = config_file

        # Loading config file
        logger.info(f"Loading config from {config_file}...")
        if not os.path.exists(config_file):
            logger.error(f"Config file {config_file} does not exist.")
            raise ValueError("Config file does not exist.")

        try:
            with open(config_file, 'r') as jf:
                self.__config = json.load(jf)
                logger.debug(f"Loaded config.")
        except Exception as e:
            logger.error(f"Failed to load config file {config_file}: {e}")
            raise ValueError("Failed to read config file.")

        self.__type = self.__config['type']

    @property
    def type(self):
        return self.__type

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    @property
    def config(self) -> dict:
        return self.__config

    @config.setter
    def config(self, config: dict):
        self.__config = config

    def update_config_file(self):
        self.logger.info(f"Updating configuration file...")
        json_config = json.dumps(self.config, indent=4)
        with open(self.__config_file, "w") as jf:
            jf.write(json_config)
        self.logger.debug(f"Finished updating configuration file {self.__config_file}.")


class Table:
    class AccessMode(Enum):
        READONLY = auto()
        APPEND = auto()
        UPSERT = auto()
        OVERWRITE = auto()
        REPLACE = auto()

    def __init__(self, namespace: str, name: str,
                 access_mode: AccessMode = AccessMode.READONLY,
                 logger: logging.Logger = None):
        self.__namespace: str = namespace
        self.__name: str = name
        self.__access_mode: Table.AccessMode = access_mode
        self.__columns: list[str] = []
        self.__logger = logger

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    @property
    def namespace(self) -> str:
        return self.__namespace

    @property
    def name(self) -> str:
        return self.__name

    @property
    def access_mode(self) -> AccessMode:
        return self.__access_mode

    @property
    def columns(self) -> list[str]:
        return self.__columns

    @columns.setter
    def columns(self, columns: list[str]):
        self.__columns = columns

    @classmethod
    def access_mode_from_string(cls, mode: str) -> AccessMode:
        try:
            return Table.AccessMode[mode.upper()]
        except KeyError:
            raise ValueError(f"Invalid access mode string {mode}")