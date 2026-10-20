"""A tiny fake ERP for local runs:  uvicorn fake_erp:app --port 18080

POST /orders books an order; POST /toggle switches the ERP down/up (503 while down)."""

import itertools

from fastapi import FastAPI, Response

app = FastAPI(title="Fake ERP")
state = {"up": True}
ids = itertools.count(1000)


@app.get("/health")
def health(response: Response):
    response.status_code = 200 if state["up"] else 503
    return {"up": state["up"]}


@app.post("/orders")
def book(order: dict, response: Response):
    if not state["up"]:
        response.status_code = 503
        return {"error": "maintenance"}
    return {"erp_id": f"ERP-{next(ids)}", "order_id": order.get("order_id")}


@app.post("/toggle")
def toggle():
    state["up"] = not state["up"]
    return state
