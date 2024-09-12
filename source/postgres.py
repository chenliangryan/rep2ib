import logging, psycopg2, traceback, json
import pyarrow as pa
from sshtunnel import SSHTunnelForwarder
from source.rdb import RDBSource, RDBTable


# Todo
#  1. Handle nullable in decimal128
#  1. Update list of tables when a whole schema is defined
#  2. Optimize data types conversion, update_table_spec. The conversion logic shall be configurable as well.

class PGSource(RDBSource):
    def __init__(self, config_file: str, logger: logging.Logger = None):
        logger.debug(f"Instantiating PGSource using {config_file}.")
        super().__init__(config_file, logger)
        if not self.config["type"] == "Postgres":
            logger.error("The type of config file is not PostgreSQL.")
            raise ValueError("Config file does not exist.")

    def __enter__(self):
        self.logger.debug(f"Getting a connection to Postgres database...")
        if self.config["database"]["use_ssh"]:
            self.logger.debug("Starting SSH tunnel...")
            ssh_host = self.config["database"]["ssh_config"]["ssh_host"]
            ssh_port = self.config["database"]["ssh_config"]["ssh_port"]
            ssh_user = self.config["database"]["ssh_config"]["ssh_user"]
            ssh_pkey = self.config["database"]["ssh_config"]["ssh_pkey"]

            self.__ssh_tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_pkey=ssh_pkey,
                remote_bind_address=("localhost", self.port)
            )
            self.__ssh_tunnel.start()
            self.host = self.__ssh_tunnel.local_bind_host
            self.port = self.__ssh_tunnel.local_bind_port
            self.logger.debug(f"Started SSH tunnel. Local bind port {self.port}.")

        self.__pg_conn = psycopg2.connect(database=self.database,
                                          user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port)
        self.logger.debug(f"Postgres connection is established.")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.debug(f"Closing Postgres connection...")
        if self.__pg_conn is not None:
            self.__pg_conn.close()
        self.logger.debug(f"Finished closing Postgres connection.")

        if self.config["database"]["use_ssh"] and self.__ssh_tunnel is not None:
            self.logger.debug(f"Closing SSH tunnel...")
            self.__ssh_tunnel.close()
            self.logger.debug(f"Finished closing SSH tunnel.")

        # Save the cursor target value to config file
        self.logger.debug(f"Updating config file...")
        self.update_config_file()
        self.logger.debug(f"Finished updating config file.")

    @property
    def conn(self):
        return self.__pg_conn


class PGTable(RDBTable):
    def __init__(self, conn, spec: dict, logger: logging.Logger = None):
        logger.debug(f"Instantiating PGTable with spec: {spec}")
        super().__init__(spec=spec, logger=logger)
        self.__conn = conn

        # Load Postgres special characters and reserved words
        self.__reserved_keywords = []
        self.logger.debug(f"Loading Postgres reserved words...")
        self.__special_characters = set(' !"#$%&\'()*+,./:;<=>?@[\\]^`{|}~')
        query = "SELECT word FROM pg_catalog.pg_get_keywords() WHERE catcode = 'R'"
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                self.__reserved_keywords = {row[0] for row in cur.fetchall()}
        except Exception as e:
            self.logger.error(f"Failed to load special words in Postgres.")
            self.logger.error(traceback.format_exc())
        self.logger.debug(f"Reserved words: {self.__reserved_keywords}")
        self.logger.debug(f"Special characters: {self.__special_characters}")

        # Load data type mappings
        self.logger.debug(f"Loading data type mappings...")
        try:
            with open("resource/pg_pa_dt_mapping.json", 'r') as jf:
                self.__dt_mappings = json.load(jf)
        except Exception as e:
            logger.error(f"Failed to load data type mappings from psycpog2 to pyarrow. {e}")
            raise ValueError("Failed to load data type mapping.")
        self.logger.debug(f"Data type mappings: {self.__dt_mappings}")

        # Update column list for SELECT *
        self.__pa_schema = []
        self.update_table_spec()

    @property
    def pyarrow_schema(self):
        return self.__pa_schema

    # Update the table spec with Postgres features
    # Generate the PyArrow Schema of the table
    def update_table_spec(self):
        # Fill data type of the table schema property
        # Extract column data types and expand column names if * is used
        self.logger.debug(f"Getting Postgres and PyArrow schema of {self.namespace}.{self.name}...")
        self.logger.debug(f"Original schema: {self.schema}...")

        # Prepare query to update table spec
        self.logger.debug(f"Preparing the query to update table spec...")
        if any("*" == col[0] for col in self.schema):
            query = f"""
            SELECT column_name, udt_name, numeric_precision, numeric_scale, datetime_precision
            FROM information_schema.columns
            WHERE table_schema = '{self.namespace}' AND table_name = '{self.name}'
            ORDER BY ordinal_position
            """
        else:
            column_names = ", ".join(f"'{column[0]}'" for column in self.schema)
            query = f"""
            SELECT column_name, udt_name, numeric_precision, numeric_scale, datetime_precision
            FROM information_schema.columns
            WHERE table_schema = '{self.namespace}' AND table_name = '{self.name}'
            AND column_name IN ({column_names})
            ORDER BY ordinal_position
            """
        self.logger.debug(f"Query for updating table spec: {query}.")

        # Update the Postgres and PyArrow schema
        with self.__conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

            pg_schema: list[(str, str)] = []
            pa_fields: list[(str, pa.Field)] = []
            for row in rows:
                column_name = self.escape_column_name(row[0])
                pa_column_name = row[0]
                pg_data_type = row[1]
                self.logger.debug(f"Original: "
                                  f"Column name: {pa_column_name}. " 
                                  f"Escaped column name: {column_name}. " 
                                  f"PA data type: {pg_data_type}.")

                if pg_data_type in self.__dt_mappings:
                    pa_data_type = getattr(pa, self.__dt_mappings[pg_data_type])()
                elif pg_data_type in ("bit", "varbit", "bytea", "uuid", "json", "jsonb", "xml"):
                    column_name = f"{column_name}::text as {column_name}"
                    pa_data_type = getattr(pa, "string")()
                elif pg_data_type in ("money",):
                    column_name = f"{column_name}::numeric(21,4) as {column_name}"
                    pa_data_type = getattr(pa, "decimal128")(21, 4)
                elif pg_data_type in ("numeric",):
                    # Get precision and scale for numeric data type
                    n_precision = int(row[2])
                    n_scale = int(row[3])
                    pa_data_type = getattr(pa, "decimal128")(n_precision, n_scale)
                elif pg_data_type in ("date",):
                    # Todo: convert the date into PyArrow.timestamp
                    # row[4] is the precision of datetime. Postgres supports millisecond.
                    column_name = f"tochar({column_name}, 'YYYY-MM-DD')::text as {column_name}"
                    pa_data_type = getattr(pa, "string")()
                elif pg_data_type in ("time", "timez", "timestamp", "timestampz"):
                    # Todo: convert into PyArrow time64, tiemstamp data type
                    #  row[4] is the precision of datetime.
                    #  Postgres supports microsecond(ms) and millisecond(US).
                    column_name = f"to_char({column_name}, 'YYYY-MM-DD HH24:MI:SS.US') as {column_name}"
                    pa_data_type = getattr(pa, "string")()
                elif pg_data_type in ("interval",):
                    # Todo: convert interval into PyArrow duration
                    column_name = f"{column_name}::text as {column_name}"
                    pa_data_type = getattr(pa, "string")()
                else:
                    self.logger.warning(f"Unsupported data type: {column_name}:{pg_data_type}.")
                    column_name = f"{column_name}::text as {column_name}"
                    pa_data_type = getattr(pa, 'string')()

                self.logger.debug(f"After patching: "
                                  f"Column name: {pa_column_name}. "
                                  f"Escaped column name: {column_name}. "
                                  f"PA data type: {pg_data_type}.")
                pg_schema.append((column_name, pg_data_type))
                pa_fields.append(pa.field(name=pa_column_name, type=pa_data_type))

        self.__pa_schema = pa.schema(pa_fields)
        self.logger.debug(f"PyArrow schema: {self.__pa_schema}")
        self.schema = pg_schema
        self.logger.debug(f"Postgres schema: {self.schema}")
        self.logger.debug(f"Finished getting schema.")

        # Update cursor related properties
        if self.is_incremental:
            self.logger.debug(f"Updating cursor expression of {self.namespace}.{self.name}...")

            field = self.spec["cursor"]["field"]
            op = self.spec["cursor"]["operator"]
            value = self.spec["cursor"]["value"]
            if isinstance(value, str):
                value = f"'{value}'"

            if field == "xid":
                # Rewrite cursor expression if it is Postgres and xid is used as cursor field
                exp = f"xmin::TEXT::BIGINT {op} {value}"
            else:
                exp = f"\"{field}\" {op} {value}"

            self.cursor_exp = exp
            self.logger.debug(f"Cursor expression: {self.cursor_exp}")

    def get_table_info(self) -> dict:
        self.logger.debug(f"Getting table info of {self.namespace}.{self.name}...")
        info = {}
        try:
            # Using the cursor configuration and filter expression to get
            # size: number of records
            # target_value: the latest cursor value
            self.logger.debug(f"Generating the query to get table info of {self.namespace}.{self.name}...")
            if self.is_incremental:
                if self.spec["cursor"]["field"] == "xid":
                    query = f"""
                            SELECT COUNT(*) as table_size, max(xmin::TEXT::BIGINT) AS target_value
                            FROM {self.namespace}.{self.name}
                            WHERE {self.cursor_exp}"""
                else:
                    cursor_field = self.spec["cursor"]["field"]
                    query = f"""
                            SELECT COUNT(*) as table_size, max(\"{cursor_field}\") AS target_value
                            FROM {self.namespace}.{self.name}
                            WHERE {self.cursor_exp}"""

                if self.filter_exp:
                    query += f" AND ({self.filter_exp})"
            else:
                query = f"""
                        SELECT COUNT(*) as table_size, -1 AS target_value
                        FROM {self.namespace}.{self.name}
                        """
                if self.filter_exp:
                    query += f" WHERE {self.filter_exp}"
            self.logger.debug(f"Query to get info of {self.namespace}.{self.name}: {query}")

            with self.__conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                if row is not None:
                    size = row[0]
                    target_value = row[1]

            info = {"size": size, "target_value": target_value}
            self.logger.debug(f"Table info: {info}.")
        except Exception as e:
            self.logger.error(f"Failed to get info from {self.namespace}.{self.name}: {e}")
            self.logger.error(traceback.print_exc())
        return info

    def get_batches(self):
        """Return an iterator of PyArrow tables."""
        self.logger.info(f"Reading {self.namespace}.{self.name} into PyArrow table batches...")
        table_info = self.get_table_info()

        try:
            if table_info["size"] == 0:
                self.logger.debug(f"No record was found in {self.namespace}.{self.name}.")
                return

            # Use psycopg2 cursor
            with self.__conn.cursor(name='replicate2ib_cursor') as cur:
                cur.execute(self.query)
                while True:
                    rows = cur.fetchmany(size=self.batch_size)
                    if not rows:
                        break
                    col_names = [desc[0] for desc in cur.description]
                    data = [dict(zip(col_names, row)) for row in rows]
                    batch = pa.RecordBatch.from_pylist(data, schema=self.__pa_schema)
                    yield pa.Table.from_batches([batch])

            self.logger.debug(f"Finished streaming table {self.namespace}.{self.name}.")

            if self.is_incremental:
                self.logger.debug(f"Updated the new cursor value of table.")
                self.spec["cursor"]["value"] = table_info["target_value"]
        except Exception as e:
            self.logger.error(f"Failed to get PyArrow Table batches from {self.namespace}.{self.name}: {e}")
            self.logger.error(traceback.format_exc())

    def escape_column_name(self, column_name: str) -> str:
        if (column_name.lower() in self.__reserved_keywords
                or any(c in self.__special_characters for c in column_name)):
            self.logger.debug(f"Escaping column name: {column_name}")
            return f"\"{column_name}\""
        return column_name
