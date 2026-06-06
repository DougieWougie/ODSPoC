docker rm -f zookeeper core-banking-db ods-db kafka debezium raw-sink $(docker ps -a -q --filter name=transformer) 2>/dev/null || true
cd /home/dougiewougie/Projects/architecture/ods/infrastructure
docker compose up -d
sleep 15

docker compose exec -T core-banking-db psql -U admin -d core_banking < ../init-scripts/init-source.sql
docker compose exec -T ods-db psql -U admin -d ods < ../init-scripts/init-ods.sql

jq '.config' connectors/register-postgres-source.json > config.json
curl -X POST -H "Content-Type: application/json" -d @config.json http://localhost:8083/connectors
sleep 10

docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic src.payments.transactions --partitions 10 2>/dev/null || true
docker compose up --scale transformer=10 -d
sleep 10

cd ..
./venv/bin/python src/tools/measure_latency.py > measure_latency.log 2>&1 &
sleep 5
./venv/bin/python src/tools/generator.py > generator.log 2>&1
sleep 40
cat measure_latency.log
