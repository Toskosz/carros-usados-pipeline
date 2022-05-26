# carros-usados-pipeline

## What
It's a ETL data pipeline that extracts information about used cars ad's from multiple sources, compiles it into a datawarehouse and makes data visualization through MetaBase. 

## Current data sources

- https://www.webmotors.com.br/
- https://www.autoline.com.br/ 

## Improvements
- Paralallelization of data extraction
- Better schedueling (Airflow)
- Decouple load from transformation
- Creation of hash algo for business Key (Avoiding duplicate fact row)
- Add more datasources such as https://www.meucarronovo.com.br
