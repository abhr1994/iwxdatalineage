from treeschema import TreeSchema
ts = TreeSchema('abhr1994@gmail.com', 'tree_schema_66600c282b1b48298e4d8fdcb852166f')

# Define the schemas that have at least 1 field in the transformation
dvc_session_schema = ts.data_store('Infoworks Source').schema('Sales_DW_Demo')
user_click_schema = ts.data_store('Infoworks Source').schema('Customer_Data_Source')


# Create or retrieve the transformation
transform_inputs = {'name': 'Infoworks Transform!', 'type': 'batch_process_scheduled'}
t = ts.transformation(transform_inputs)

transform_links = [
    (dvc_session_schema.field('customer_id'), user_click_schema.field('id')),
    (dvc_session_schema.field('customer_name'), user_click_schema.field('name'))
]

t.create_links(transform_links)