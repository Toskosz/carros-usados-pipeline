# carros-usados-pipeline
https://tokmas.space/public/dashboard/49a73de4-8384-4cbc-88e4-afcda2b5dfc5

## What
It's a ETL data pipeline that extracts information about used cars ad's from multiple sources, compiles it into a datawarehouse and makes data visualization through MetaBase.

## Diagram
![Design](https://github.com/Toskosz/carros-usados-pipeline/blob/pandas/media/used-cars-pipeline-diagram_page-0001.jpg)

## Current data sources

- https://www.webmotors.com.br/
- https://www.autoline.com.br/ 

## What it looks like
![Final Dashboard](https://github.com/Toskosz/carros-usados-pipeline/blob/mem_test/media/metabase_dashboard.png)

## Improvements to be made
- Store the raw files extracted in S3
- Error notification through SNS
- Paralallelization of data extraction
- Better schedueling (Airflow)
- Decouple extraction, load and transformation
- Add more datasources such as https://www.meucarronovo.com.br
- Add more tests
