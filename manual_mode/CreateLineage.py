import argparse, logging
import configparser
from os.path import dirname as up

cwd = up(up(__file__))
import sys

sys.path.insert(0, cwd)
from TreeSchemaConnection import TreeSchemaConnection
import traceback
import csv


class CreateLineage:
    def __init__(self):
        pass

    @staticmethod
    def define_lineage_link(tsconn, lineage_name, src_ds_conn, src_schema_name, tgt_ds_conn, tgt_schema_name,
                            col_mappings):
        try:
            src_schema = tsconn.get_schema(src_ds_conn, src_schema_name)
            tgt_schema = tsconn.get_schema(tgt_ds_conn, tgt_schema_name)
            src_schema.get_fields()
            tgt_schema.get_fields()
            transform_links = []
            for cols in col_mappings.split("|"):
                from_col, to_col = cols.split(":")
                link = (src_schema.field(from_col), tgt_schema.field(to_col))
                transform_links.append(link)

            transform_inputs = {'name': lineage_name, 'type': 'batch_process_scheduled'}
            tsconn.create_transformations(transform_inputs, transform_links)
            logging.info('Lineage Job ' + src_schema_name + ' To ' + tgt_schema_name + " created!!!")
        except Exception as e:
            print('Failed to create lineage {}'.format(lineage_name))
            print(str(e))
            logging.error('Failed to create lineage {}'.format(lineage_name))
            logging.error(str(e))
            traceback.print_exc()


def main():
    config = configparser.ConfigParser()
    config.read('../tables.config')
    email = config.get("DEFAULT", "email")
    api = config.get("DEFAULT", "api")
    logfile = config.get("DEFAULT", "logfile")
    logging.basicConfig(filename=logfile, filemode='w', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser('Create Lineage Process')
    parser.add_argument('--lineage_file_path', required=True, help='Provide the location of lineage file path')
    args = vars(parser.parse_args())
    tsconn = TreeSchemaConnection(email, api)
    with open(args["lineage_file_path"], 'r') as data:
        for line in csv.DictReader(data):
            lineage_to_create = dict(line)
            src_ds_name, tgt_ds_name = lineage_to_create["datastore_mapping"].split(":")
            src_ds_conn = tsconn.get_datastore_connection(src_ds_name)
            tgt_ds_conn = tsconn.get_datastore_connection(tgt_ds_name)
            src_schema, tgt_schema = lineage_to_create["schema_mapping"].split(":")
            lineage_name = lineage_to_create["lineage_name"]
            col_mappings = lineage_to_create["col_mapping"]
            try:
                CreateLineage.define_lineage_link(tsconn, lineage_name, src_ds_conn, src_schema, tgt_ds_conn,
                                                  tgt_schema, col_mappings)
            except:
                pass


if __name__ == '__main__':
    main()
