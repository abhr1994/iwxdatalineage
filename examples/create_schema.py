from treeschema import TreeSchema
ts = TreeSchema('abhr1994@gmail.com', 'tree_schema_66600c282b1b48298e4d8fdcb852166f')

#data_store = ts.data_store('Infoworks')
data_store = ts.data_store('Snowflake')

my_schema_obj = {
    'name': 'iwx.HR_Database.dimDept',
    'type': 'table',
    'description': 'This is an Infoworks table'
}
new_schema = data_store.schema(my_schema_obj)


