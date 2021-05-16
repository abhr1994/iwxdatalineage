from treeschema import TreeSchema
ts = TreeSchema('abhr1994@gmail.com', 'tree_schema_66600c282b1b48298e4d8fdcb852166f')

dimEmployee = ts.data_store('Infoworks').schema('iwx.HR_Database.dimEmployee')
dimDept = ts.data_store('Infoworks').schema('iwx.HR_Database.dimDept')
empl_details = ts.data_store('Infoworks').schema('iwx.HR.EMPLOYEE_DETAILS')
sf_target = ts.data_store('Snowflake').schema('PUBLIC.EMPLOYEE_DETAILS')

transform_inputs = {'name': 'HR Pipeline', 'type': 'batch_process_scheduled'}
t = ts.transformation(transform_inputs)


transform_links = [
    (dimEmployee.field('EMP_ID'), empl_details.field('EMP_ID')),
    (dimEmployee.field('EMP_NAME'), empl_details.field('EMP_NAME')),
    (dimEmployee.field('COMPANY'), empl_details.field('COMPANY')),
    (dimEmployee.field('EMAIL'), empl_details.field('EMAIL')),
    (dimEmployee.field('PAN'), empl_details.field('PAN')),
    (dimEmployee.field('DEPT_CODE'), empl_details.field('DEPT_CODE')),
    (dimDept.field('DPT_NAME'), empl_details.field('DPT_NAME')),
    (empl_details.field('EMP_ID'), sf_target.field('EMP_ID')),
    (empl_details.field('EMP_NAME'), sf_target.field('EMP_NAME')),
    (empl_details.field('COMPANY'), sf_target.field('COMPANY')),
    (empl_details.field('EMAIL'), sf_target.field('EMAIL')),
    (empl_details.field('PAN'), sf_target.field('PAN')),
    (empl_details.field('DEPT_CODE'), sf_target.field('DEPT_CODE')),
    (empl_details.field('DPT_NAME'), sf_target.field('DPT_NAME')),
]


t.create_links(transform_links)

