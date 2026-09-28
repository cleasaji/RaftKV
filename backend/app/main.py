"""
The RaftKV API: a 5-node simulated cluster you can elect a leader on,
propose SET/DEL commands to, partition/heal, and read committed KV
state from -- a hands-on demo of the consensus algorithm actually working.
"""

from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.cluster import Cluster
from app.kv_store import KVStateMachine

app = FastAPI(title="RaftKV", description="Distributed consensus key-value store (Raft).", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

NODE_IDS = ["n1", "n2", "n3", "n4", "n5"]
_cluster = Cluster(NODE_IDS)
_kv = KVStateMachine()


def _sync_kv():
    leader_id = _cluster.current_leader()
    if leader_id:
        _kv.apply_committed(_cluster.nodes[leader_id])


class ElectRequest(BaseModel):
    candidate_id: str


class ProposeRequest(BaseModel):
    op: str    # SET or DEL
    key: str
    value: Optional[str] = None


class NodeAction(BaseModel):
    node_id: str


@app.post("/election")
def run_election(req: ElectRequest):
    won = _cluster.run_election(req.candidate_id)
    return {"candidate_id": req.candidate_id, "won": won, "current_leader": _cluster.current_leader()}


@app.post("/propose")
def propose(req: ProposeRequest):
    leader_id = _cluster.current_leader()
    if not leader_id:
        raise HTTPException(status_code=409, detail="no leader elected -- run an election first")
    index = _cluster.propose(leader_id, {"op": req.op, "key": req.key, "value": req.value})
    _sync_kv()
    return {"log_index": index, "leader": leader_id, "committed": _cluster.nodes[leader_id].commit_index >= index}


@app.post("/isolate")
def isolate(req: NodeAction):
    _cluster.isolate(req.node_id)
    return {"isolated": sorted(_cluster.isolated)}


@app.post("/heal")
def heal(req: NodeAction):
    _cluster.heal(req.node_id)
    return {"isolated": sorted(_cluster.isolated)}


@app.get("/status")
def status():
    return {
        "nodes": {
            nid: {"state": n.state, "term": n.current_term, "log_length": n.last_log_index(),
                 "commit_index": n.commit_index, "isolated": nid in _cluster.isolated}
            for nid, n in _cluster.nodes.items()
        },
        "current_leader": _cluster.current_leader(),
        "kv_data": _kv.data,
    }


@app.post("/reset")
def reset():
    global _cluster, _kv
    _cluster = Cluster(NODE_IDS)
    _kv = KVStateMachine()
    return {"status": "cleared"}


@app.get("/health")
def health():
    return {"status": "ok"}
