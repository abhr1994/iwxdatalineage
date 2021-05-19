from os.path import dirname as up
cwd=up(up(__file__))
import sys
sys.path.insert(0,cwd)
from TreeSchemaConnection import TreeSchemaConnection
import csv
import configparser
import argparse
import logging
import sys
import os.path
def main():
    config = configparser.ConfigParser()
    config.read('../tables.config')
    email = config.get("DEFAULT", "email")
    api = config.get("DEFAULT", "api")
    default_datastore = config.get("DEFAULT", "datastore")
    logfile = config.get("DEFAULT", "logfile")
    logging.basicConfig(filename=logfile, filemode='w', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser('CreateTables')
    parser.add_argument('--add_fields_csv_path', required=True, help='Pass the create schema CSV file path')
    args = vars(parser.parse_args())
    tsconn = TreeSchemaConnection(email, api)
    with open(args["add_fields_csv_path"],mode='r') as file:
        fieldsFile = csv.reader(file)
        header = next(fieldsFile)
        DataTypes={"integer":int,"float":float,"string":str,"boolean":bool,"bytes":bytes,"array":list,"object":dict,"null":None}

        for row in fieldsFile:
            if(row):
                schema_name = row[0]
                datastore_name=row[1]
                datastore=tsconn.get_datastore_connection(datastore_name)
                if(datastore):
                    logging.info("Got connected to Datastore {datastore_name} Successfully".format(datastore_name=datastore_name))
                else:
                    logging.error("Failed to connect to Datastore {datastore_name}".format(datastore_name=datastore_name))
                schema = tsconn.get_schema(datastore,schema_name)
                if (schema):
                    logging.info("Schema Found {schema_name}".format(schema_name=schema_name))
                else:
                    logging.error("Failed to find Schema {schema_name}".format(schema_name=schema_name))
                columns=row[2].split("|")
                field_dict={}
                for column in columns:
                    field_name,field_type=column.split(":")
                    field_dict[field_name]=DataTypes.get(field_type,"Unknown")
                tsconn.create_fields(schema,field_dict)
            else:
                continue


if __name__ == '__main__':
    main()


