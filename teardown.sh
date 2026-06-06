#!/bin/bash

echo "Tearing down the PoC infrastructure..."

# Stop and remove all containers, networks, and volumes defined in the docker-compose.yaml
docker compose -f infrastructure/docker-compose.yaml down -v

echo "Teardown complete! You can also safely delete the 'venv' directory if you want a completely fresh start."
