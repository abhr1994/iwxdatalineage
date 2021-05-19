import os
os.chdir("../")
from mongo_utils import mongodb
from bson import ObjectId
import networkx as nx
from treeschema import TreeSchema
ts = TreeSchema('abhr1994@gmail.com', 'tree_schema_66600c282b1b48298e4d8fdcb852166f')
pipeline_info = mongodb.pipeline_versions.find_one({"_id": ObjectId("f49482994e996d82c8f426fe")},no_cursor_timeout=True)
from collections import defaultdict

target_schemas = []
src_schemas = []
lineage = []
for node in pipeline_info["model"]["pipeline_graph"]:
    lineage.append((node["from"],node["to"]))

lineage = [('SOURCE_TABLE_wd4r', 'JOIN_6jwr'), ('SOURCE_TABLE_b564', 'JOIN_6jwr'), ('JOIN_6jwr', 'TARGET_z6q1')]
graph = nx.DiGraph()
graph.add_edges_from(lineage)


output = {}
src_cols = defaultdict(lambda: [])
for node_key in pipeline_info["model"]["nodes"].keys():
    if pipeline_info["model"]["nodes"][node_key]["type"] == "TARGET":
        target_schemas.append((node_key,"iwx."+pipeline_info["model"]["nodes"][node_key]['properties']['target_schema_name']+"."+pipeline_info["model"]["nodes"][node_key]['properties']['target_table_name']))
    elif pipeline_info["model"]["nodes"][node_key]["type"] == "SOURCE_TABLE":
        table_id = pipeline_info["model"]["nodes"][node_key]['properties']['table_id']
        table_info = mongodb.tables.find_one({"_id": ObjectId(table_id)},no_cursor_timeout=True)
        target_schema_name = table_info.get("target_schema_name")
        table_name = table_info.get("table")
        schema = "iwx."+target_schema_name+"."+table_name
        src_schemas.append(schema)
    elif pipeline_info["model"]["nodes"][node_key]["type"] == "SNOWFLAKE_TARGET":
        target_schema_name = pipeline_info["model"]["nodes"][node_key]['properties']['schema_name']
        table_name = pipeline_info["model"]["nodes"][node_key]['properties']['table_name']
        schema = target_schema_name+"."+table_name
        target_schemas.append((node_key,schema))
    else:
        pass
    input_entities = pipeline_info["model"]["nodes"][node_key]['input_entities']
    output_entities = pipeline_info["model"]["nodes"][node_key]['output_entities']
    outputmapping = {}
    inputmapping = {}
    for column in output_entities:
        id = column["id"]
        name = column["name"]
        reference_id = column["mapping"].get("reference_id",None)
        if reference_id:
            outputmapping[reference_id] = name

    for column in input_entities:
        id = column["id"]
        name = column["name"]
        reference_id = column["mapping"].get("reference_id")
        inputmapping[id] = name
        if pipeline_info["model"]["nodes"][node_key]["type"] == "SOURCE_TABLE":
            src_cols[schema].append(name)

    final_mapping={}
    for key in outputmapping.keys():
        final_mapping[outputmapping[key]] = inputmapping[key]

    flipped_mapping = dict([(value, key) for key, value in final_mapping.items()])

    output[node_key] = flipped_mapping


print(output)
merged_dict = {}
for node in list(nx.topological_sort(graph)):
    if len(merged_dict) == 0:
        merged_dict = output[node]
    else:
        merged_dict|=output[node]

col_graph = nx.DiGraph()
col_graph.add_edges_from(list(merged_dict.items()))


def find_root(G,child):
    parent = list(G.predecessors(child))
    if len(parent) == 0:
        return child
    else:
        if parent[0] == child:
            return child
        else:
            return find_root(G, parent[0])

output_input_mapping = {}
for tgt in target_schemas:
    for leaf_col in list(output[tgt[0]].values()):
        parent = find_root(col_graph,leaf_col)
        print(leaf_col,":",parent)
        output_input_mapping[leaf_col] = parent


src_ts = []

src_ts_schema = {}
for schema_name in src_schemas:
    globals()[f"{'_'.join(schema_name.split('.'))}"] = ts.data_store('Infoworks').schema(schema_name)
    src_ts_schema[schema_name] = ts.data_store('Infoworks').schema(schema_name)
    src_ts.append(globals()[f"{'_'.join(schema_name.split('.'))}"])

target_ts = []
for schema_name in target_schemas:
    #If type is snowflake then change the datastore
    globals()[f"{'_'.join(schema_name[1].split('.'))}"] = ts.data_store('Infoworks').schema(schema_name[1])
    target_ts.append(globals()[f"{'_'.join(schema_name[1].split('.'))}"])

print(target_ts)
#transform_inputs = {'name': 'Testing Pipeline', 'type': 'batch_process_scheduled'}
#t = ts.transformation(transform_inputs)

#Do get schemas of all the schema to speed up the process so that validation can happen faster

transform_links = []
for key in output_input_mapping.keys():
    for src in src_cols.keys():
        if output_input_mapping[key] in src_cols[src]:
            src_schema = globals()[f"{'_'.join(src.split('.'))}"]
            break
    transform_links.append(src_schema.field(output_input_mapping[key]),target_ts[0].field(key))
    print(transform_links)

print(transform_links)
#t.create_links(transform_links)
