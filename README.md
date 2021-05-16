##What is Treeschema?

```markdown

Treeschema Data Catalog helps you find the data you need anywhere within your data ecosystem from the database all the way down to the specific values for each field


Treeschema Data Lineage helps you  explore your data lineage and understand where your data comes from and where it is going

```

## How to use the scripts in this repository?

- mongo.config is the configuration file used to connect to the Infoworks metadata
- tables.config is the configuration file used to connect to the TreeSchema Server

```markdown
# Create Tables in TreeSchema

python CreateTables.py --table_ids 609f85e2ecc4dd3c323fc41f

# Create Ingestion Job

python IngestionMapping.py --src_ds "SQL Server" --tgt_ds Infoworks --src_target_schema "supportdb.dbo.dimEmployee:iwx.HR_Database.dimEmployee,supportdb.dbo.dimDept:iwx.HR_Database.dimDept"

# Create Pipeline Job


# Create Export Job

```
