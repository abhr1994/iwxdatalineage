from os.path import dirname as up

cwd = up(up(__file__))
import sys

sys.path.insert(0, cwd)
from TreeSchemaConnection import TreeSchemaConnection
import csv
import configparser
import argparse
import logging


def validate_params(tsobj, schema_name, datastore_name, schema_type):
    datastore = tsobj.get_datastore_connection(datastore_name)
    if datastore and schema_type in ['table']:
        logging.info("Connected to Datastore: {datastore_name}".format(datastore_name=datastore_name))
        print("Validated the parameters")
        return True
    else:
        print("Please validate that the datastore is present in TreeSchema")
        return False


def main():
    config = configparser.ConfigParser()
    config.read('../tables.config')
    email = config.get("DEFAULT", "email")
    api = config.get("DEFAULT", "api")
    logfile = config.get("DEFAULT", "logfile")
    logging.basicConfig(filename=logfile, filemode='w', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser('CreateTables')
    parser.add_argument('--create_schema_csv_path', required=True, help='Pass the create schema CSV file path')
    args = vars(parser.parse_args())
    tsconn = TreeSchemaConnection(email, api)
    with open(args["create_schema_csv_path"], mode='r') as file:
        schemaFile = csv.reader(file)
        header = next(schemaFile)
        for row in schemaFile:
            if row:
                validation_status = validate_params(tsconn, row[0], row[1], row[2])
                if validation_status:
                    datastore_conn = tsconn.get_datastore_connection(row[1])
                    schema_exists = tsconn.check_schema(datastore_conn, row[0])
                    if not schema_exists:
                        schema_creation_status = tsconn.create_schema(datastore_conn, row[0], row[2])
                        if schema_creation_status:
                            print("Schema {schema_name} created successfully".format(schema_name=row[0]))
                            logging.info("Schema {schema_name} created successfully".format(schema_name=row[0]))
                        else:
                            logging.error("Failed to create Schema {schema_name}".format(schema_name=row[0]))
                    else:
                        print("Schema {schema_name} already exists..Skipping the Schema Creation".format(
                            schema_name=row[0]))
                        logging.info("Schema {schema_name} already exists..Skipping the Schema Creation".format(
                            schema_name=row[0]))
                else:
                    logging.error("invalid parameters passed")
            else:
                continue


if __name__ == '__main__':
    main()
