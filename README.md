# carros-usados-pipeline

## What
It's a ETL data pipeline that extracts information about used cars ad's from multiple sources, compiles it into a datawarehouse and makes data visualization through MetaBase.

## Diagram
![Design](https://github.com/Toskosz/carros-usados-pipeline/blob/main/media/used-cars-pipeline-diagram_page-0001.jpg)

## Current data sources

- https://www.webmotors.com.br/
- https://www.autoline.com.br/ 

## What it looks like
![Final Dashboard](https://github.com/Toskosz/carros-usados-pipeline/blob/main/media/metabase_dashboard.png)

## Setup

### Pre-requisites

1. [Docker](https://docs.docker.com/engine/install/) and [Docker Compose](https://docs.docker.com/compose/install/) v1.27.0 or later.
2. [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

### Local

We have a [`Makefile`](Makefile) with common commands. These are executed in the running container.

```bash
cd bitcoinmonitor
make up # starts all the containers
make ci # runs formatting, lint check, type check and python test
```

If the CI step passes you can go to http://localhost:3000 to checkout your Metabase instance.

To login refer to the credentials in the env file.

## Improvements to be made
- Store the raw data extracted in S3
- Error notification and log storage
- Better schedueling (Airflow)
- Decouple extraction sources
- Decouple extraction, visualizaton (metabase) and storage
- Add more datasources such as https://www.meucarronovo.com.br
- Add more tests
