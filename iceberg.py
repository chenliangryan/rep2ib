import json, logging, traceback
import pyarrow as pa
import pyiceberg.catalog as ib
from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError
from helper import Table


# Todo
#  1. Update Iceberg table if the PA table schema is different
#  2. Handle upsert
#  3. Run table maintenance after loading

class IBSession:
    def __init__(self, catalog_name: str = "default", logger: logging.Logger = None):
        self.__catalog = ib.load_catalog(name=catalog_name)
        self.logger = logger

    def create_table(self, table: str, schema: pa.Schema):
        self.logger.info(f"Creating table `{table}`")
        try:
            self.__catalog.create_table(identifier=table, schema=schema)
        except TableAlreadyExistsError as e:
            self.logger.error(f"Table `{table}` already exists.")

    def recreate_table(self, table: str, schema: pa.Schema):
        self.logger.info(f"Recreating table `{table}`")
        try:
            self.__catalog.drop_table(identifier=table)
        except NoSuchTableError as e:
            self.logger.warning(f"Table `{table}` doesn't exists.")
        finally:
            self.create_table(table=table, schema=schema)

    def alter_table(self, table: str, schema: pa.Schema):
        pass

    def append_table(self, table: str, data: pa.Table):
        self.logger.debug(f"Appending table {table}")
        dump_error = False
        try:
            ib_table = self.__catalog.load_table(identifier=table)
        except NoSuchTableError as e:
            self.logger.warning(f"Table `{table}` does not exist. Creating it.")
            self.create_table(table, data.schema)
            ib_table = self.__catalog.load_table(identifier=table)

        try:
            ib_table.append(df=data)
        except Exception as e:
            self.logger.error(f"Failed to append to `{table}`. Error: {e}")
            self.logger.error(traceback.format_exc)
            if not dump_error:
                self.dump_table(table=table, data=data)
                dump_error = True

    def overwrite_table(self, table: str, data: pa.Table):
        self.logger.debug(f"Overwriting table {table}")
        dump_error = False
        try:
            ib_table = self.__catalog.load_table(identifier=table)
        except NoSuchTableError as e:
            self.logger.warning(f"Table `{table}` does not exist. Creating it.")
            self.create_table(table, data.schema)
            ib_table = self.__catalog.load_table(identifier=table)

        try:
            ib_table.overwrite(df=data)
        except Exception as e:
            self.logger.error(f"Failed to overwrite to `{table}`. Error: {e}")
            self.logger.error(traceback.format_exc)
            if not dump_error:
                self.dump_table(table=table, data=data)
                dump_error = True

    def upsert_table(self, table: str, data: pa.Table):
        self.logger.debug(f"Upserting table `{table}`")
        pass

    def dump_table(self, table: str, data: pa.Table):
        self.logger.info(f"Dumping table {table}")
        try:
            with open(f"debug/{table}_error_sample.json", 'w') as ef:
                for row in data.to_pylist():
                    ef.write(json.dumps(row))
        except Exception as e:
            self.logger.error(f"Failed to dump the data.")
            self.logger.error(traceback.format_exc())