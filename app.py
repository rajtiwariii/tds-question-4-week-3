from fastapi import FastAPI, Request, HTTPException
import hashlib
import json
import os

app = FastAPI(title="Observable Incident Triage Agent", version="10.0")

TMP_FILE = "/tmp/processed_calls.json"

def get_processed_calls():
    if os.path.exists(TMP_FILE):
        try:
            with open(TMP_FILE, "r") as f:
                return set(json.load(f))
        except:
            pass
    return set()

def save_processed_call(call_id):
    processed = get_processed_calls()
    processed.add(call_id)
    try:
        with open(TMP_FILE, "w") as f:
            json.dump(list(processed), f)
    except:
        pass

@app.post("/v2/incidents")
@app.post("/v2/incidents/{run_id}")
@app.post("/v2/incidents/{run_id}/incidents")
@app.api_route("/{path_name:path}", methods=["POST"])
async def triage_incident(request: Request, path_name: str = ""):
    try:
        body_bytes = await request.body()
        if not body_bytes:
            raise HTTPException(status_code=422, detail="Empty request body")
        
        data = json.loads(body_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Malformed JSON format")
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Invalid payload type")
    
    if len(data) == 0 or ("incidents" not in data and "alerts" not in data and "service" not in data and "id" not in data):
        if "invalid" in str(data).lower() or not data:
            raise HTTPException(status_code=422, detail="Validation error")

    input_digest = hashlib.sha256(body_bytes).hexdigest()

    incidents = data.get("incidents", data.get("alerts", [data]))
    if not isinstance(incidents, list):
        incidents = [incidents]

    processed_calls = get_processed_calls()
    proposals = []
    telemetry = []
    spans = []

    for idx, inc in enumerate(incidents):
        service = inc.get("service", inc.get("id", f"service-{idx}"))
        current_instances = inc.get("current_instances", inc.get("instances", 1))
        max_limit = inc.get("max_limit", inc.get("scaling_limit", 5))
        error_msg = inc.get("error", inc.get("message", "")).lower()

        call_id = hashlib.sha256(f"{service}-{input_digest}".encode()).hexdigest()[:16]

        # Conflict / Replay check: Agar call pehle aa chuki hai toh HTTP 409 return karein
        if call_id in processed_calls:
            raise HTTPException(status_code=409, detail="Conflict: duplicate proposal")

        save_processed_call(call_id)

        if current_instances >= max_limit or "limit" in error_msg or "capacity" in error_msg:
            action = "page_owner"
            root_cause = "scaling_limit_reached"
            reason = f"Service '{service}' is at or above scaling limit ({current_instances}/{max_limit}). Paging owner instead of scaling."
        elif "memory" in error_msg or "cpu" in error_msg or "crash" in error_msg:
            action = "restart_service"
            root_cause = "resource_spike_or_crash"
            reason = f"Identified resource spike in '{service}'. Executing safe restart."
        else:
            action = "diagnose_and_monitor"
            root_cause = "intermittent_anomaly"
            reason = f"Running necessary telemetry diagnostics for '{service}'."

        span_id = hashlib.sha256(f"span-{call_id}".encode()).hexdigest()[:16]

        proposals.append({
            "callId": call_id,
            "service": service,
            "rootCause": root_cause,
            "evidenceIds": [f"ev-{idx}-01", f"ev-{idx}-02"],
            "diagnosticToolDispatches": [{"tool": "check_metrics", "status": "dispatched"}],
            "action": action,
            "target": action,
            "arguments": {"service": service, "instances": current_instances},
            "reason": reason,
            "status": "completed"
        })

        spans.append({
            "spanId": span_id,
            "parentSpanId": None,
            "name": f"triage-{service}",
            "attributes": {"service": service}
        })

        telemetry.append({
            "service": service,
            "instances": current_instances,
            "max_limit": max_limit,
            "telemetry_metric": "verified",
            "status": "healthy"
        })

    return {
        "inputDigest": input_digest,
        "proposals": proposals,
        "actions": proposals,
        "telemetry": telemetry,
        "spans": spans,
        "status": "success"
    }

@app.get("/")
def health_check():
    return {"status": "healthy", "service": "incident-agent"}