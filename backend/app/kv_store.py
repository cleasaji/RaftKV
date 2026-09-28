"""
A replicated key-value store applied as a state machine over the
committed Raft log -- committed entries (and ONLY committed entries)
are applied, in log order, which is what gives every node in the
cluster the same final key-value state regardless of which node it is,
as long as they've applied the same committed prefix.
"""

from typing import Optional

from app.raft_node import RaftNode


class KVStateMachine:
    def __init__(self):
        self.data: dict = {}
        self.applied_index = 0

    def apply_committed(self, node: RaftNode) -> None:
        while self.applied_index < node.commit_index:
            self.applied_index += 1
            entry = node.log[self.applied_index - 1]
            if not entry.command:
                continue
            op = entry.command.get("op")
            if op == "SET":
                self.data[entry.command["key"]] = entry.command["value"]
            elif op == "DEL":
                self.data.pop(entry.command["key"], None)

    def get(self, key: str) -> Optional[str]:
        return self.data.get(key)
