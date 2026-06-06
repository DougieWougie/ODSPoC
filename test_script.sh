cd infrastructure
docker compose up --scale transformer=10 -d
sleep 10
cd ..
./venv/bin/python src/tools/measure_latency.py > measure_latency_verification.log 2>&1 &
sleep 5
./venv/bin/python src/tools/generator.py > generator_verification.log 2>&1
sleep 40
