import traceback
from treeschema import TreeSchema
import logging


class TreeSchemaConnection:
    def __init__(self, email, api_key):
        self.ts = TreeSchema(email, api_key)

    def get_datastore_connection(self,datastore_name):
        try:
            return self.ts.data_store(datastore_name)
        except Exception as e:
            print('Failed to connect to TreeSchema...Validate the API credentials')
            logging.error('Failed to connect to TreeSchema...Validate the API credentials')
            logging.error(str(e))
            traceback.print_exc()
            raise RuntimeError('Failed to connect to TreeSchema...Validate the API credentials')

    def create_schema(self, datastore_conn, schema_name):
        try:
            schema_obj = {
                'name': schema_name,
                'type': 'table',
                'description': 'This is an Infoworks table'
            }
            schema = datastore_conn.schema(schema_obj)
            logging.info("Table {} created successfully".format(schema_name))
            logging.info(schema)
        except Exception as e:
            print('Failed to create table in DataStore')
            logging.error('Failed to create table in DataStore')
            logging.error(str(e))
            traceback.print_exc()
            raise RuntimeError('Failed to create table in DataStore')

    def get_schema(self, datastore_conn, schema_name):
        try:
            return datastore_conn.schema(schema_name)
        except Exception as e:
            print('Failed to get the Schema {}'.format(schema_name))
            print(str(e))
            logging.error('Failed to get the Schema {}'.format(schema_name))
            logging.error(str(e))
            traceback.print_exc()
            raise RuntimeError('Failed to get the Schema {}'.format(schema_name))

    def create_fields(self, schema, fields):
        try:
            for key in fields.keys():
                field_obj = {
                    'name': key,
                    'type': fields[key]
                }
                schema.field(field_obj)
                logging.info("Field {} created successfully".format(key))
                logging.info(field_obj)
        except Exception as e:
            print('Failed to create Data Fields {}'.format(fields))
            print(str(e))
            logging.error('Failed to create Data Fields {}'.format(fields))
            logging.error(str(e))
            traceback.print_exc()
            raise RuntimeError('Failed to create Data Fields {}'.format(fields))

    def create_transformations(self,transform_inputs,transform_links):
        try:
            t = self.ts.transformation(transform_inputs)
            t.create_links(transform_links)
        except Exception as e:
            print('Failed to create transformation {}'.format(transform_inputs['name']))
            print(str(e))
            logging.error('Failed to create transformation {}'.format(transform_inputs['name']))
            logging.error(str(e))
            traceback.print_exc()
            raise RuntimeError('Failed to create transformation {}'.format(transform_inputs['name']))

