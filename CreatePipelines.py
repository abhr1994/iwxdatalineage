import argparse, logging
import configparser
from TreeSchemaConnection import TreeSchemaConnection
import traceback
import networkx as nx
from mongo_utils import mongodb
from bson import ObjectId
from collections import defaultdict


def get_pipeline_name(pipeline_id):
    result = mongodb.pipelines.find_one({"_id": ObjectId(pipeline_id)}, no_cursor_timeout=True)
    return result.get("name", None)


class CreatePipelineProcess:
    def __init__(self):
        pass

    @staticmethod
    def get_pipeline_lineage(pipeline_info):
        lineage = []
        for node in pipeline_info["model"]["pipeline_graph"]:
            lineage.append((node["from"], node["to"]))
        # lineage = [('SOURCE_TABLE_wd4r', 'JOIN_6jwr'), ('SOURCE_TABLE_b564', 'JOIN_6jwr'), ('JOIN_6jwr', 'TARGET_z6q1')]
        graph = nx.DiGraph()
        graph.add_edges_from(lineage)
        return list(nx.topological_sort(graph))

    @staticmethod
    def get_pipeline_column_lineage(pipeline_info):
        column_mappings = {}
        target_schemas = {}
        src_schemas = {}
        src_cols = defaultdict(lambda: [])
        for node_key in pipeline_info["model"]["nodes"].keys():
            if pipeline_info["model"]["nodes"][node_key]["type"] == "TARGET":
                target_schemas[node_key] = "iwx." + pipeline_info["model"]["nodes"][node_key]['properties'][
                    'target_schema_name'] + "." + pipeline_info["model"]["nodes"][node_key]['properties'][
                                               'target_table_name']
            elif pipeline_info["model"]["nodes"][node_key]["type"] == "SOURCE_TABLE":
                table_id = pipeline_info["model"]["nodes"][node_key]['properties']['table_id']
                table_info = mongodb.tables.find_one({"_id": ObjectId(table_id)}, no_cursor_timeout=True)
                target_schema_name = table_info.get("target_schema_name")
                table_name = table_info.get("table")
                schema = "iwx." + target_schema_name + "." + table_name
                src_schemas[node_key] = schema
            elif pipeline_info["model"]["nodes"][node_key]["type"] == "SNOWFLAKE_TARGET":
                target_schema_name = pipeline_info["model"]["nodes"][node_key]['properties']['schema_name']
                table_name = pipeline_info["model"]["nodes"][node_key]['properties']['table_name']
                schema = target_schema_name.upper() + "." + table_name.upper()
                target_schemas[node_key] = schema
            else:
                pass

            input_entities = pipeline_info["model"]["nodes"][node_key]['input_entities']
            output_entities = pipeline_info["model"]["nodes"][node_key]['output_entities']
            outputmapping = {}
            inputmapping = {}
            for column in output_entities:
                name = column["name"]
                reference_id = column["mapping"].get("reference_id", None)
                if reference_id:
                    outputmapping[reference_id] = name
            for column in input_entities:
                id = column["id"]
                name = column["name"]
                inputmapping[id] = name
                if pipeline_info["model"]["nodes"][node_key]["type"] == "SOURCE_TABLE":
                    src_cols[node_key].append(name)

            temp_mapping = {}
            for key in outputmapping.keys():
                temp_mapping[outputmapping[key]] = inputmapping[key]
            flipped_mapping = dict([(value, key) for key, value in temp_mapping.items()])
            column_mappings[node_key] = flipped_mapping

        return column_mappings, target_schemas, src_schemas, src_cols

    @staticmethod
    def create_dag_columns(columns_list, pipeline_lineage):
        merged_columns = {}
        for node in pipeline_lineage:
            if len(merged_columns) == 0:
                merged_columns = columns_list[node]
            else:
                merged_columns |= columns_list[node]

        col_graph = nx.DiGraph()
        col_graph.add_edges_from(list(merged_columns.items()))
        simple_cycles = list(nx.simple_cycles(col_graph))
        if len(simple_cycles) > 0:
            for item in simple_cycles:
                col_graph.remove_edge(item[-1], item[0])
        return col_graph

    @staticmethod
    def find_root(G, child):
        parent = list(G.predecessors(child))
        if len(parent) == 0:
            return child
        else:
            if parent[0] == child:
                return child
            else:
                return CreatePipelineProcess.find_root(G, parent[0])

    @staticmethod
    def create_ts_schemas(ts, config, src_schemas, target_schemas):
        iwx_ds_name = config.get("DEFAULT", "iwx_datastore")
        snowflake_ds_name = config.get("DEFAULT", "snowflake_datastore")
        src_ts_schemas = {}
        ds_iwx_conn = ts.get_datastore_connection(iwx_ds_name)
        try:
            ds_sf_conn = ts.get_datastore_connection(snowflake_ds_name)
        except:
            pass
        for schema_key in src_schemas.keys():
            schema_name = src_schemas[schema_key]
            src_ts_schemas[schema_key] = ts.get_schema(ds_iwx_conn, schema_name)
            src_ts_schemas[schema_key].get_fields()
        target_ts_schemas = {}
        for schema_key in target_schemas:
            schema_name = target_schemas[schema_key]
            # If type is snowflake then change the datastore
            if "SNOWFLAKE" in schema_key:
                target_ts_schemas[schema_key] = ts.get_schema(ds_sf_conn, schema_name)
            else:
                target_ts_schemas[schema_key] = ts.get_schema(ds_iwx_conn, schema_name)
            target_ts_schemas[schema_key].get_fields()

        return src_ts_schemas, target_ts_schemas

    @staticmethod
    def create_transformations(ts, config, pipeline_name, output_input_mapping, src_ts_schemas, target_ts_schemas, src_cols, pipeline_lineage, column_mappings):
        transform_inputs = {'name': pipeline_name, 'type': 'batch_process_scheduled'}
        transform_links = []
        for target_key in output_input_mapping.keys():
            for output_column in output_input_mapping[target_key].keys():
                for src in src_cols.keys():
                    if output_input_mapping[target_key][output_column] in src_cols[src]:
                        src_schema = src_ts_schemas[src]
                        break
                else:
                    # If the output column is not present at all in any of the sources (Derive column). ##TO-DO. Not tested
                    iwx_ds_name = config.get("DEFAULT", "iwx_datastore")
                    ds_iwx_conn = ts.get_datastore_connection(iwx_ds_name)
                    for node in pipeline_lineage:
                        if output_input_mapping[target_key][output_column] in column_mappings[node].values():
                            schema_name = "other."+pipeline_name+"."+node
                            break
                    derive_schema = ts.create_schema(ds_iwx_conn, schema_name, "This is node with new columns")
                    fields = {output_input_mapping[target_key][output_column]:str}
                    ts.create_fields(derive_schema,fields)
                    orig_field = ds_iwx_conn.schema(schema_name).field(output_input_mapping[target_key][output_column])
                    transform_links.append((orig_field,target_ts_schemas[target_key].field(output_column)))
                    continue
                transform_links.append((src_schema.field(output_input_mapping[target_key][output_column]),
                                        target_ts_schemas[target_key].field(output_column)))
        # print(transform_links)
        ts.create_transformations(transform_inputs, transform_links)


def main():
    config = configparser.ConfigParser()
    config.read('pipeline.config')
    email = config.get("DEFAULT", "email")
    api = config.get("DEFAULT", "api")
    logfile = config.get("DEFAULT", "logfile")
    logging.basicConfig(filename=logfile, filemode='w', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser('Create Ingestion Process')
    parser.add_argument('--pipeline_ids', required=True,help='Comma seperated pipeline ids here')
    args = vars(parser.parse_args())
    tsconn = TreeSchemaConnection(email, api)
    for pipeline_id in args["pipeline_ids"].split(","):
        try:
            pipeline_info = mongodb.pipeline_versions.find_one({"_id": ObjectId(pipeline_id)}, no_cursor_timeout=True)
            pipeline_name = get_pipeline_name(str(pipeline_info["pipeline_id"]))
            pipeline_lineage = CreatePipelineProcess.get_pipeline_lineage(pipeline_info)
            column_mappings, target_schemas, src_schemas, src_cols = CreatePipelineProcess.get_pipeline_column_lineage(
                pipeline_info)
            col_lineage_graph = CreatePipelineProcess.create_dag_columns(column_mappings, pipeline_lineage)
            output_input_colmapping = {}
            for tgt_key in target_schemas.keys():
                temp_col_mapping = {}
                for leaf_col in list(column_mappings[tgt_key].values()):
                    parent = CreatePipelineProcess.find_root(col_lineage_graph, leaf_col)
                    # print(leaf_col,":",parent)
                    temp_col_mapping[leaf_col] = parent
                output_input_colmapping[tgt_key] = temp_col_mapping

            src_ts_schemas, target_ts_schemas = CreatePipelineProcess.create_ts_schemas(tsconn, config, src_schemas,
                                                                                        target_schemas)
            CreatePipelineProcess.create_transformations(tsconn, config, pipeline_name, output_input_colmapping, src_ts_schemas,
                                                         target_ts_schemas, src_cols, pipeline_lineage, column_mappings)
            logging.info("Transformation {} created succesfully".format(pipeline_name))
        except Exception as e:
            print('Failed to create transformation {}'.format(pipeline_name))
            print(str(e))
            logging.error('Failed to create transformation {}'.format(pipeline_name))
            logging.error(str(e))
            traceback.print_exc()


if __name__ == '__main__':
    main()
