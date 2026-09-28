"""
A real (simplified, synchronously-simulated) implementation of the Raft
consensus algorithm's core node logic: leader election with the log-
up-to-date safety restriction, and AppendEntries log replication with
conflict resolution -- the same rules from the Raft paper (Ongaro &
Ousterhout), just driven by direct method calls instead of real network
sockets so the algorithm's correctness can be deterministically tested.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: Optional[dict] = None


class RaftNode:
    def __init__(self, node_id: str, peer_ids: List[str]):
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.state = "follower"   # follower, candidate, leader
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []          # 1-indexed conceptually; log[0] is index 1
        self.commit_index = 0
        self.current_leader: Optional[str] = None

        # leader-only volatile state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        self.votes_received: set = set()

    def last_log_index(self) -> int:
        return len(self.log)

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def start_election(self) -> None:
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}

    def become_leader(self) -> None:
        self.state = "leader"
        self.current_leader = self.node_id
        self.next_index = {p: self.last_log_index() + 1 for p in self.peer_ids}
        self.match_index = {p: 0 for p in self.peer_ids}

    def step_down_if_stale(self, term: int) -> bool:
        """If we see a higher term, we're stale -- revert to follower.
        Returns True if we stepped down."""
        if term > self.current_term:
            self.current_term = term
            self.state = "follower"
            self.voted_for = None
            return True
        return False

    def handle_request_vote(self, term: int, candidate_id: str,
                            last_log_index: int, last_log_term: int) -> (int, bool):
        self.step_down_if_stale(term)

        if term < self.current_term:
            return self.current_term, False

        # Election restriction (Raft safety): only vote for a candidate whose
        # log is at least as up-to-date as ours, so a leader can never lose a
        # committed entry -- this is THE core safety property of Raft.
        log_is_up_to_date = (
            last_log_term > self.last_log_term()
            or (last_log_term == self.last_log_term() and last_log_index >= self.last_log_index())
        )

        if (self.voted_for is None or self.voted_for == candidate_id) and log_is_up_to_date:
            self.voted_for = candidate_id
            return self.current_term, True

        return self.current_term, False

    def handle_append_entries(self, term: int, leader_id: str, prev_log_index: int,
                              prev_log_term: int, entries: List[LogEntry], leader_commit: int) -> (int, bool):
        self.step_down_if_stale(term)

        if term < self.current_term:
            return self.current_term, False

        # A valid AppendEntries from a current-term leader means this node is
        # definitely not the leader (or a stale candidate) -- revert to follower.
        self.state = "follower"
        self.current_leader = leader_id

        # Log consistency check: our log must already contain an entry at
        # prev_log_index matching prev_log_term, or we reject and the leader
        # will retry with an earlier prev_log_index (standard Raft backoff).
        if prev_log_index > 0:
            if prev_log_index > self.last_log_index():
                return self.current_term, False
            if self.log[prev_log_index - 1].term != prev_log_term:
                return self.current_term, False

        # Append new entries, truncating on the first term conflict.
        for i, entry in enumerate(entries):
            idx = prev_log_index + i  # 0-indexed position in self.log
            if idx < len(self.log):
                if self.log[idx].term != entry.term:
                    self.log = self.log[:idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.last_log_index())

        return self.current_term, True
