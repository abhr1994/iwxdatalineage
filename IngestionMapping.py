import argparse, logging
import configparser
from TreeSchemaConnection import TreeSchemaConnection
import traceback

class CreateIngestionProcess:
    def __init__(self):
        pass

    @staticmethod
    def define_ingestion_link(tsconn, src_ds_conn, src_schema_name, tgt_ds_conn, tgt_schema_name, ingestion_name):
        try:
            src_schema = tsconn.get_schema(src_ds_conn,src_schema_name)
            tgt_schema = tsconn.get_schema(tgt_ds_conn,tgt_schema_name)
            src_schema.get_fields()
            tgt_schema.get_fields()
            transform_links = []
            # For each field in the source, check if the field name exists in the target
            for src_field in src_schema.fields.values():
                tgt_field = tgt_schema.field(src_field.name)
                if tgt_field:
                    link_tuple = (src_field, tgt_field)
                    transform_links.append(link_tuple)

            # Set the state to the current value, this will create new links and deprecate old links
            transform_inputs = {'name': ingestion_name, 'type': 'batch_process_scheduled'}
            tsconn.create_transformations(transform_inputs,transform_links)
            logging.info('Ingestion Job '+src_schema_name+' To '+tgt_schema_name+" created!!!")
        except Exception as e:
            print('Failed to create transformation {}'.format(ingestion_name))
            print(str(e))
            logging.error('Failed to create transformation {}'.format(ingestion_name))
            logging.error(str(e))
            traceback.print_exc()


def main():
    config = configparser.ConfigParser()
    config.read('tables.config')
    email = config.get("DEFAULT", "email")
    api = config.get("DEFAULT", "api")
    logfile = config.get("DEFAULT", "logfile")
    logging.basicConfig(filename=logfile, filemode='w', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser('Create Ingestion Process')
    parser.add_argument('--src_ds', required=True, help='Pass the source datastore name here')
    parser.add_argument('--tgt_ds', required=True, help='Pass the target datastore name here')
    parser.add_argument('--ingestion_job_name', required=False, default='src_tgt_ds', help='src_tgt_ds or src_tgt_schema')
    parser.add_argument('--src_target_schema', required=False, help='Pass comma seperated multiple src:target schema '
                                                                    'combinations')
    args = vars(parser.parse_args())

    tsconn = TreeSchemaConnection(email, api)
    src_ds_name = args["src_ds"]
    tgt_ds_name = args["tgt_ds"]
    src_ds_conn = tsconn.get_datastore_connection(src_ds_name)
    tgt_ds_conn = tsconn.get_datastore_connection(tgt_ds_name)
    for src_target_schema in args["src_target_schema"].split(","):
        src_schema,tgt_schema = src_target_schema.split(":")
        if args["ingestion_job_name"] and args["ingestion_job_name"].lower() == "src_tgt_schema":
            ingestion_name = "Ingestion of {} to {}".format(src_schema,tgt_schema)
        else:
            ingestion_name = src_ds_name+" : "+tgt_ds_name
        CreateIngestionProcess.define_ingestion_link(tsconn, src_ds_conn, src_schema, tgt_ds_conn, tgt_schema, ingestion_name)


if __name__ == '__main__':
    main()
