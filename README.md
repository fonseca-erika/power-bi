# powerbi_end2end_demo
Repository that creates the DBR tables and loading process for a simple star schema including the related power bi report templates and lake view dashboards

This demo uses the tpch dataset provided in the databricks samples dataset and contains the following

1. code to create the raw tables in the bronze layer
2. code to create the dimensional model in the silver layer
3. power bi report template that visualize the data
4. lakeview dashboards that visualize the data

# List of tables

| Table      | Description |
| ----------- | ----------- |
| dim_customer | contains the attributes from the tpch.customners table|
| dim_part | contains the attributes from the tpch.part table|
| dim_supplier | contains the attributes from the tpch.suppliers table|
| dim_date | a date dimension|
| fact_order_line_item | fact table built using tpch.line_item and tpch.orders |


steps to create this demo
1. Execute the notebook data_model/Layer_Silver_ddl : This notebook creates the tables(including Pk-Fk constraints with Rely option), before you begin replace the catalog and schema to the appropriate values.
2. Execute the notebook data_model/Layer_Silver_load : This notebook loads the tables, before you begin replace the catalog and schema to the appropriate values.
3. Import the lakeview dashboard dashboards/Sample_Dashboard.lvdash.json
4. Import the pbit (https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-templates#using-report-templates)

