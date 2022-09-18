from typing import Union

from fastapi import FastAPI

from supervisor import (
    Node,
    Edge,
    Graph,
    Supervisor
)


app = FastAPI()


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/rules/{peer_id}")
async def get_rules(peer_id: int):
    return {"peer_id": peer_id}

