up:
	docker compose --env-file env up --build -d

down: 
	docker compose down

# -ti == -i -t
shell:
	docker exec -ti pipelinerunner bash

wh-shell:
	docker exec -ti warehouse bash

pytest:
	docker exec pipelinerunner python -m unittest discover /code/test

stop-etl: 
	docker exec pipelinerunner service cron stop