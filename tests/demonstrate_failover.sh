#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "      NGINX Regional Failover Demonstration Script       "
echo "=========================================================="

BASE_URL="http://localhost:8080"

echo "[1/6] Ensuring all Docker services are running and healthy..."
docker compose up -d

echo "Waiting for services to become healthy..."
MAX_RETRIES=30
RETRY_COUNT=0
until [ $RETRY_COUNT -ge $MAX_RETRIES ]
do
    US_HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/us/health" || true)
    EU_HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/eu/health" || true)
    if [ "$US_HEALTH" = "200" ] && [ "$EU_HEALTH" = "200" ]; then
        echo "All regional backends are healthy and reachable via NGINX proxy!"
        break
    fi
    echo "Waiting for backends to report healthy... ($RETRY_COUNT/$MAX_RETRIES)"
    RETRY_COUNT=$((RETRY_COUNT+1))
    sleep 3
done

if [ "$US_HEALTH" != "200" ] || [ "$EU_HEALTH" != "200" ]; then
    echo "ERROR: Services failed to report healthy within timeout."
    docker compose ps
    exit 1
fi

echo ""
echo "[2/6] Sending GET request to /us/health (Normal Operation)..."
NORMAL_RESP=$(curl -s -i "$BASE_URL/us/health")
echo "$NORMAL_RESP"

echo ""
echo "[3/6] Inspecting backend_us logs to confirm primary handled request..."
docker logs backend_us --tail 4

echo ""
echo "[4/6] Simulating US region failure by stopping backend_us container..."
docker stop backend_us

echo "Container backend_us stopped successfully."
sleep 2

echo ""
echo "[5/6] Sending GET request to /us/health while backend_us is DOWN..."
FAILOVER_RESP=$(curl -s -i "$BASE_URL/us/health")
echo "$FAILOVER_RESP"

FAILOVER_CODE=$(echo "$FAILOVER_RESP" | grep -i "HTTP/" | head -n 1 | awk '{print $2}')
if [ "$FAILOVER_CODE" != "200" ]; then
    echo "ERROR: Failover request returned status $FAILOVER_CODE instead of 200 OK!"
    docker start backend_us
    exit 1
fi

echo ""
echo "[6/6] Inspecting backend_eu logs to verify EU handled the /us/ traffic..."
docker logs backend_eu --tail 6

echo ""
echo "Restarting backend_us container to restore cluster topology..."
docker start backend_us
sleep 3

echo ""
echo "=========================================================="
echo " SUCCESS: NGINX successfully detected US backend failure  "
echo " and rerouted /us/ traffic to backend_eu without downtime! "
echo "=========================================================="
