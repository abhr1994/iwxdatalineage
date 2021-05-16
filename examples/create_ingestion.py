from treeschema import TreeSchema
ts = TreeSchema('abhr1994@gmail.com', 'tree_schema_66600c282b1b48298e4d8fdcb852166f')

sql_dimEmployee = ts.data_store('SQL Server').schema('supportdb.dbo.dimEmployee')
sql_dimDept = ts.data_store('SQL Server').schema('supportdb.dbo.dimDept')

iwx_dimEmployee = ts.data_store('Infoworks').schema('iwx.HR_Database.dimEmployee')
iwx_dimDept = ts.data_store('Infoworks').schema('iwx.HR_Database.dimDept')

transform_inputs = {'name': 'HR Ingestion', 'type': 'batch_process_scheduled'}
t = ts.transformation(transform_inputs)


ingest_links = [
    (sql_dimEmployee.field('EMP_ID'), iwx_dimEmployee.field('EMP_ID')),
    (sql_dimEmployee.field('EMP_NAME'), iwx_dimEmployee.field('EMP_NAME')),
    (sql_dimEmployee.field('COMPANY'), iwx_dimEmployee.field('COMPANY')),
    (sql_dimEmployee.field('EMAIL'), iwx_dimEmployee.field('EMAIL')),
    (sql_dimEmployee.field('PAN'), iwx_dimEmployee.field('PAN')),
    (sql_dimEmployee.field('DEPT_CODE'), iwx_dimEmployee.field('DEPT_CODE')),
    (sql_dimDept.field('DPT_CODE'), iwx_dimDept.field('DPT_CODE')),
    (sql_dimDept.field('DPT_NAME'), iwx_dimDept.field('DPT_NAME')),
]

t.create_links(ingest_links)


'''
from treeschema import TreeSchema
  ts = TreeSchema('YOUR_EMAIL', 'YOUR_SECRET_KEY')
  
  # Get the source and target schemas
  src_schema = ts.data_store('My Source DS').schema('src.schema')
  tgt_schema = ts.data_store('Target DS').schema('tgt.schema')

  # Pre fetch all of the fields to reduce API overhead
  src_schema.get_fields()
  tgt_schema.get_fields()
  
  transform_links = []
  
  # For each field in the source, check if the field name exists in the target
  for src_field in src_schema.fields.values():
      tgt_field = tgt_schema.field(src_field.name)
      if tgt_field:
        link_tuple = (src_field, tgt_field)
        transform_links.append(link_tuple)
  
  # Set the state to the current value, this will create new links and
  # deprecate old links
  
  t = ts.transformation('My Second Transform')
  t.set_links_state(transform_links)

'''