import sys, logging, traceback
from source.postgres import PGSource, PGTable
from iceberg import IBSession

# Config logging
logging.basicConfig()

logger = logging.getLogger("rep2ib")
logger.setLevel(logging.DEBUG)
l_formatter = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")

f_handler = logging.FileHandler(f"rep2ib.log")
f_handler.setFormatter(l_formatter)
f_handler.setLevel(logging.DEBUG)
logger.addHandler(f_handler)

c_handler = logging.StreamHandler()
c_handler.setFormatter(l_formatter)
c_handler.setLevel(logging.ERROR)
logger.addHandler(c_handler)


def main():
    # Get arguments
    args = sys.argv[1:]

    if len(args) == 0:
        config_file = "config/default.json"
        logger.warning(f"No config file specified. Using default config file {config_file}.")
    else:
        config_file = args[0]
        logger.debug(f"Reading config from {config_file}.")

    ib_session = IBSession(catalog_name="default", logger=logger)

    # Todo: append is not working
    with PGSource(config_file=config_file, logger=logger) as source:
        logger.debug(f"Replicating Postgres source {source.name}...")
        for table in source.tables:
            table_name = table["namespace"] + "." + table["name"]
            logger.debug(f"Replicating table {table_name}...")
            table = PGTable(conn=source.conn, spec=table, logger=logger)
            target_table = table.target
            target_table_name = f"{target_table.namespace}.{target_table.name}"

            if target_table.access_mode == PGTable.AccessMode.REPLACE:
                logger.debug(f"Replacing table {target_table_name}...")
                ib_session.recreate_table(table=target_table_name, schema=table.pyarrow_schema)

            overwrite = False
            if target_table.access_mode == PGTable.AccessMode.OVERWRITE:
                overwrite = True

            upsert = False
            if target_table.access_mode == PGTable.AccessMode.UPSERT:
                upsert = True

            for batch in table.get_batches():
                try:
                    # Overwrite the table in first batch
                    if overwrite:
                        ib_session.overwrite_table(table=target_table_name, data=batch)
                        overwrite = False
                        continue

                    if upsert:
                        ib_session.upsert_table(table=target_table_name, data=batch)
                    else:
                        ib_session.append_table(table=target_table_name, data=batch)
                except Exception as e:
                    logger.error(f"Failed to write batch. Error: {e}")
                    logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
