.PHONY: build run stop clean test deploy

# Docker commands
build:
	docker compose -f docker-compose.yml build

run:
	docker compose -f docker-compose.yml up -d

stop:
	docker compose -f docker-compose.yml down

clean:
	docker compose -f docker-compose.yml down -v
	docker system prune -f

# Kubernetes commands
#k8s-deploy:
#	kubectl apply -f kubernetes/namespace.yaml
#	kubectl apply -f kubernetes/configmap.yaml
#	kubectl apply -f kubernetes/secrets.yaml
#	kubectl apply -f kubernetes/postgres-deployment.yaml
#	kubectl apply -f kubernetes/metastore-deployment.yaml
#	kubectl apply -f kubernetes/metastore-service.yaml

#k8s-delete:
#	kubectl delete -f kubernetes/

# Testing
#test:
#	python tests/integration-test.py

# Monitoring
#monitor:
#	docker-compose -f monitoring/docker-compose-monitoring.yml up -d

# Logs
logs:
	docker compose -f docker-compose.yml logs -f hive-metastore

# Schema initialization
init-schema:
	docker compose -f docker-compose.yml exec hive-metastore /opt/hive-metastore/bin/schematool -dbType postgres -initSchema

# Backup
#backup:
#	./scripts/backup-metadata.sh