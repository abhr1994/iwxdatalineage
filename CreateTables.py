import traceback
from mongo_utils import mongodb
from bson import ObjectId
import argparse, logging
import configparser
from TreeSchemaConnection import TreeSchemaConnection


class CreateIwxTables:
    def __init__(self):
        pass

    @staticmethod
    def create_iwx_tables(ts_obj, data_store_conn, table_id):
        try:
            sqlTypes = {12: str, 93: str, 4: int, -2: str, -5: int, 8: float, 6: float, 3: float, 16: str, 91: str}
            table_info = mongodb.tables.find_one({"_id": ObjectId(table_id)},no_cursor_timeout=True)
            target_schema_name = table_info.get("target_schema_name")
            table_name = table_info.get("table")
            columns = table_info.get("columns")
            rowCount = int(table_info.get("rowCount",0))
            target_base_path = table_info.get("target_base_path",None)
            state = table_info.get("state",None)
            description = "target_base_path : {} \n rowCount : {} \n state : {}".format(target_base_path,rowCount,state)
            treeschema_entity = "iwx." + target_schema_name + "." + table_name
            ts_obj.create_schema(data_store_conn, treeschema_entity, description)
            schema = ts_obj.get_schema(data_store_conn, treeschema_entity)
            fields = {}
            for column in columns:
                column_name = column["name"]
                if column_name.lower().startswith("ziw"):
                    continue
                column_type = sqlTypes[int(column["sqlType"])]
                fields[column_name] = column_type
            ts_obj.create_fields(schema, fields)
        except Exception as e:
            print(str(e))
            logging.error('Table Creation failed for {}'.format(table_id))
            logging.error(str(e))
            traceback.print_exc()


def main():
    config = configparser.ConfigParser()
    config.read('tables.config')
    email = config.get("DEFAULT", "email")
    api = config.get("DEFAULT", "api")
    default_datastore = config.get("DEFAULT", "datastore")
    logfile = config.get("DEFAULT", "logfile")
    logging.basicConfig(filename=logfile, filemode='w', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser('CreateTables')
    parser.add_argument('--table_ids', required=True, help='Pass comma separated table IDs')
    parser.add_argument('--datastore', required=False, default=None, help='Pass the datastore name here')
    args = vars(parser.parse_args())
    tsconn = TreeSchemaConnection(email, api)
    if args["datastore"]:
        datastore = args["datastore"]
    else:
        datastore = default_datastore
    data_store_conn = tsconn.get_datastore_connection(datastore)
    logging.info("Connected to {} DataStore".format(datastore))
    for table_id in args["table_ids"].split(","):
        try:
            CreateIwxTables.create_iwx_tables(tsconn, data_store_conn, table_id)
        except Exception:
            pass


if __name__ == '__main__':
    main()
