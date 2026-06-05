#!/bin/bash

set -e

BASE_URL="http://localhost:8132"

echo "=== Temporal Lite Integration Test ==="
echo ""

echo "1. Checking health endpoint..."
curl -s $BASE_URL/health | python3 -m json.tool
echo ""

echo "2. Starting a new OrderWorkflow..."
RESPONSE=$(curl -s -X POST $BASE_URL/api/workflow/start \
  -H "Content-Type: application/json" \
  -d '{
    "workflowType": "OrderWorkflow",
    "input": {
      "orderId": "ORD-12345",
      "userId": "user-987",
      "amount": 99.99,
      "item": "Wireless Headphones"
    },
    "version": "v1"
  }')

echo "$RESPONSE" | python3 -m json.tool

WORKFLOW_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['workflowId'])")
RUN_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['runId'])")

echo ""
echo "Workflow ID: $WORKFLOW_ID"
echo "Run ID: $RUN_ID"
echo ""

echo "3. Waiting 2 seconds for workflow to process..."
sleep 2

echo ""
echo "4. Checking workflow status..."
curl -s "$BASE_URL/api/workflow/status?workflowId=$WORKFLOW_ID&runId=$RUN_ID" | python3 -m json.tool
echo ""

echo "5. Sending signal to workflow..."
curl -s -X POST "$BASE_URL/api/workflow/signal?workflowId=$WORKFLOW_ID&runId=$RUN_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "signalName": "updateOrder",
    "input": {
      "note": "Customer requested gift wrapping",
      "updatedBy": "system"
    },
    "version": "v1"
  }' | python3 -m json.tool
echo ""

echo "6. Waiting 5 seconds for workflow to complete..."
sleep 5

echo ""
echo "7. Querying workflow state via query handler..."
curl -s -X POST "$BASE_URL/api/workflow/query?workflowId=$WORKFLOW_ID&runId=$RUN_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "queryName": "getState",
    "input": {}
  }' | python3 -m json.tool
echo ""

echo "8. Getting full event history..."
curl -s "$BASE_URL/api/workflow/history?workflowId=$WORKFLOW_ID&runId=$RUN_ID" | python3 -m json.tool
echo ""

echo "9. Getting final workflow status..."
curl -s "$BASE_URL/api/workflow/status?workflowId=$WORKFLOW_ID&runId=$RUN_ID" | python3 -m json.tool
echo ""

echo "10. Listing all workflows..."
curl -s "$BASE_URL/api/workflows" | python3 -m json.tool
echo ""

echo "=== Test Complete ==="
echo ""
echo "To debug with replay, run:"
echo "  go run ./cmd/cli replay --workflow-id $WORKFLOW_ID --run-id $RUN_ID --verbose"
echo ""
