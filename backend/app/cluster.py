"""
Simulates a cluster of RaftNodes communicating over synchronous, direct
calls -- an in-memory "network" that can be partitioned on demand, which
is what lets tests deterministically exercise Raft's behavior under
network splits (a minority partition can never elect a leader that
commits new entries; a majority partition can).
"""

from typing import Dict, List, Optional

from app.raft_node import RaftNode, LogEntry


class Cluster:
    def __init__(self, node_ids: List[str]):
        self.nodes: Dict[str, RaftNode] = {
            nid: RaftNode(nid, [p for p in node_ids if p != nid]) for nid in node_ids
        }
        self.isolated: set = set()   # node ids currently cut off from every other node

    def isolate(self, node_id: str) -> None:
        self.isolated.add(node_id)

    def heal(self, node_id: str) -> None:
        self.isolated.discard(node_id)

    def heal_all(self) -> None:
        self.isolated.clear()

    def _can_communicate(self, a: str, b: str) -> bool:
        return a not in self.isolated and b not in self.isolated

    def run_election(self, candidate_id: str) -> bool:
        """Runs one full election round for `candidate_id`. Returns True if
        it won (received votes from a majority of the whole cluster,
        including itself) and became leader."""
        node = self.nodes[candidate_id]
        node.start_election()

        for peer_id in node.peer_ids:
            if not self._can_communicate(candidate_id, peer_id):
                continue
            peer = self.nodes[peer_id]
            term, granted = peer.handle_request_vote(
                node.current_term, candidate_id, node.last_log_index(), node.last_log_term()
            )
            if term > node.current_term:
                node.current_term = term
                node.state = "follower"
                return False
            if granted:
                node.votes_received.add(peer_id)

        majority = (len(self.nodes) // 2) + 1
        if len(node.votes_received) >= majority:
            node.become_leader()
            return True
        return False

    def replicate(self, leader_id: str) -> None:
        """One round of AppendEntries from the leader to every reachable
        follower, then advances commit_index if a majority now match."""
        leader = self.nodes[leader_id]
        if leader.state != "leader":
            return

        for peer_id in leader.peer_ids:
            if not self._can_communicate(leader_id, peer_id):
                continue
            peer = self.nodes[peer_id]
            next_idx = leader.next_index[peer_id]
            prev_log_index = next_idx - 1
            prev_log_term = leader.log[prev_log_index - 1].term if prev_log_index > 0 else 0
            entries = leader.log[prev_log_index:]

            term, success = peer.handle_append_entries(
                leader.current_term, leader_id, prev_log_index, prev_log_term, entries, leader.commit_index
            )
            if term > leader.current_term:
                leader.current_term = term
                leader.state = "follower"
                return
            if success:
                leader.match_index[peer_id] = prev_log_index + len(entries)
                leader.next_index[peer_id] = leader.match_index[peer_id] + 1
            else:
                # Log inconsistency -- back off and retry with an earlier index next round.
                leader.next_index[peer_id] = max(1, leader.next_index[peer_id] - 1)

        # Advance commit_index to the highest index replicated on a majority
        # of nodes, but ONLY if that entry was written in the leader's
        # current term (Raft's commit-safety rule -- never commit a prior
        # term's entry purely by count, only via a current-term entry).
        n = len(self.nodes)
        match_values = sorted(
            [leader.last_log_index()] + [leader.match_index[p] for p in leader.peer_ids], reverse=True
        )
        majority_match = match_values[n // 2]
        if majority_match > leader.commit_index and leader.log[majority_match - 1].term == leader.current_term:
            leader.commit_index = majority_match

    def propose(self, leader_id: str, command: dict) -> int:
        leader = self.nodes[leader_id]
        if leader.state != "leader":
            raise ValueError(f"{leader_id} is not the current leader")
        leader.log.append(LogEntry(term=leader.current_term, command=command))
        self.replicate(leader_id)
        return leader.last_log_index()

    def current_leader(self) -> Optional[str]:
        leaders = [nid for nid, n in self.nodes.items() if n.state == "leader"]
        return leaders[0] if leaders else None
