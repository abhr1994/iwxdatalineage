from treeschema import TreeSchema
ts = TreeSchema('abhr1994@gmail.com', 'tree_schema_66600c282b1b48298e4d8fdcb852166f')

dimDept = ts.data_store('Infoworks').schema('iwx.AMN_pepsi_chargeback_report_schema.databricks_runs')

#fields = {"EMP_ID":"integer","EMP_NAME":"string","COMPANY":"string","EMAIL":"string","PAN":"string","DEPT_CODE":"integer"}
fields = {"created_at":"timestamp"}

for key in fields.keys():
    field_obj = {
        'name': key,
        'type': 'scalar',
        'data_type': 'integer'
    }
    dimDept.field(field_obj)
    print(field_obj)



