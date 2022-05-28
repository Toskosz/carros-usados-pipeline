# carros-usados-pipeline

## What
It's a ETL data pipeline that extracts information about used cars ad's from multiple sources, compiles it into a datawarehouse and makes data visualization through MetaBase.

## Diagram
![Design](https://github.com/Toskosz/carros-usados-pipeline/blob/pandas/media/used-cars-pipeline-diagram_page-0001.jpg)

## Current data sources

- https://www.webmotors.com.br/
- https://www.autoline.com.br/ 

## Improvements to be made
- Paralallelization of data extraction
- Better schedueling (Airflow)
- Decouple load from transformation
- Creation of hash algo for business Key (Avoiding duplicate fact row)
- Add more datasources such as https://www.meucarronovo.com.br
