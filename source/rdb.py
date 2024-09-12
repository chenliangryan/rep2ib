import logging, json, traceback
from jsonschema import validate, exceptions
from helper import Source, Table


DEFAULT_BATCH_SIZE = 10000

class RDBSource(Source):
    def __init__(self, config_file: str, logger: logging.Logger = None):
        super().__init__(config_file, logger)
        self.__host: str = self.config["database"]["host"]
        self.__port: int = self.config["database"]["port"]
        self.__database: str = self.config["database"]["database"]
        self.__user: str = self.config["database"]["user"]
        self.__password: str = self.config["database"]["password"]
        self.__tables: list[dict] = self.config["tables"]

    @property
    def name(self) -> str:
        return f"{self.__host}:{self.__port}/{self.__database}"

    @property
    def host(self) -> str:
        return self.__host

    @host.setter
    def host(self, host: str):
        self.__host = host

    @property
    def port(self) -> int:
        return self.__port

    @port.setter
    def port(self, port: int):
        self.__port = port

    @property
    def database(self) -> str:
        return self.__database

    @property
    def user(self) -> str:
        return self.__user

    @property
    def password(self) -> str:
        return self.__password

    @property
    def tables(self) -> list[dict]:
        return self.__tables


class RDBTable(Table):
    def __init__(self, spec: dict, logger: logging.Logger = None):
        namespace = spec["namespace"].strip()
        name = spec["name"].strip()
        super().__init__(namespace=namespace, name=name, logger=logger)

        # Validate the table spec
        logger.debug(f"Validating table with spec: {spec}")
        try:
            with open("resource/rdb_table_schema.json", 'r') as jf:
                schema = json.load(jf)
                validate(instance=spec, schema=schema)
        except exceptions.ValidationError as e:
            logger.error(f"Table spec doesn't comply with the schema. {e.message}")
            raise ValueError("Failed to validate the spec.")
        except Exception as e:
            logger.error(f"Other error occurred while validating the spec.")
            logger.error(traceback.format_exc())
            raise ValueError("Failed to validate the spec.")
        logger.debug(f"Table spec is valid.")

        logger.debug(f"Instantiating Table with spec...")
        self.__spec = spec

        if "columns" in self.__spec:
            self.__schema = [(i.strip(), "") for i in self.__spec["columns"].split(',')]
        else:
            self.__schema = [("*", "")]

        if "filter_exp" in self.__spec:
            self.__filter_exp = self.__spec["filter_exp"].strip()
        else:
            self.__filter_exp = ""

        self.__cursor_exp = ""

    @property
    def spec(self) -> dict:
        return self.__spec

    @property
    def schema(self) -> list[(str, str)]:
        return self.__schema

    @schema.setter
    def schema(self, schema: list[(str, str)]):
        self.__schema = schema

    @property
    def filter_exp(self) -> str:
        return self.__filter_exp

    @filter_exp.setter
    def filter_exp(self, filter_exp: str):
        self.__filter_exp = filter_exp

    @property
    def batch_size(self) -> int:
        if "batch_size" in self.__spec:
            batch_size = self.__spec["batch_size"]
        else:
            batch_size = DEFAULT_BATCH_SIZE
        return batch_size

    @property
    def target(self) -> Table:
        namespace = self.__spec["target"]["namespace"]
        if "name" in self.__spec["target"]:
            name = self.__spec["target"]["name"]
        else:
            name = self.name
        if "access_mode" in self.__spec["target"]:
            access_mode = self.access_mode_from_string(self.__spec["target"]["access_mode"])
        else:
            access_mode = self.AccessMode.OVERWRITE
        return Table(namespace, name, access_mode)

    @property
    def is_incremental(self) -> bool:
        if "cursor" in self.__spec:
            is_incremental = True
        else:
            is_incremental = False
        return is_incremental

    @property
    def cursor_exp(self) -> str:
        return self.__cursor_exp

    @cursor_exp.setter
    def cursor_exp(self, cursor_exp: str):
        self.__cursor_exp = cursor_exp

    @property
    def query(self) -> str:
        self.logger.debug(f"Generating query from spec...")
        # Generate projection expression
        projection = ', '.join([col[0] for col in self.schema])

        # Base query
        query = f"SELECT {projection} FROM {self.namespace}.{self.name}"

        # Generate where clause
        where_clause = False

        # Add filter expression
        if self.filter_exp:
            query += f" WHERE ({self.filter_exp})"
            where_clause = True

        # Add cursor filtering expression
        if self.is_incremental:
            if not self.cursor_exp:
                self.logger.error(f"Table {self.namespace}.{self.name} is configured as incremental loading"
                                  f" but no cursor expression is found.")
                raise ValueError("Incremental loading table with no cursor expression.")

            if not where_clause:
                query += f" WHERE "
            else:
                query += f" AND "
            query += f"({self.cursor_exp})"

        self.logger.debug(f"Generated query: {query}")
        return query
