import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.cluster import Cluster
from app.kv_store import KVStateMachine
from app.raft_node import LogEntry


def build_cluster(n=5):
    return Cluster([f"n{i}" for i in range(1, n + 1)])


def test_election_succeeds_with_full_cluster_reachable():
    c = build_cluster()
    won = c.run_election("n1")
    assert won
    assert c.nodes["n1"].state == "leader"
    assert c.current_leader() == "n1"


def test_election_fails_when_candidate_isolated():
    c = build_cluster()
    c.isolate("n1")
    won = c.run_election("n1")
    assert not won
    assert c.nodes["n1"].state == "candidate"  # never reached majority, stays candidate


def test_election_succeeds_even_with_minority_partitioned():
    c = build_cluster()  # 5 nodes; isolating 2 still leaves a majority (3) reachable
    c.isolate("n4")
    c.isolate("n5")
    won = c.run_election("n1")
    assert won


def test_propose_replicates_and_commits_with_majority():
    c = build_cluster()
    c.run_election("n1")
    index = c.propose("n1", {"op": "SET", "key": "foo", "value": "bar"})
    leader = c.nodes["n1"]
    assert leader.commit_index >= index


def test_propose_fails_on_non_leader():
    c = build_cluster()
    try:
        c.propose("n2", {"op": "SET", "key": "x", "value": "1"})
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_partitioned_minority_leader_cannot_commit():
    c = build_cluster()
    c.run_election("n1")
    # Cut the leader off from a majority of followers -- only n2 stays reachable.
    c.isolate("n3")
    c.isolate("n4")
    c.isolate("n5")
    index = c.propose("n1", {"op": "SET", "key": "x", "value": "1"})
    leader = c.nodes["n1"]
    assert leader.commit_index < index  # can't reach majority (needs 3 of 5), stays uncommitted


def test_kv_state_machine_applies_only_committed_entries():
    c = build_cluster()
    c.run_election("n1")
    c.propose("n1", {"op": "SET", "key": "a", "value": "1"})
    kv = KVStateMachine()
    kv.apply_committed(c.nodes["n1"])
    assert kv.get("a") == "1"


def test_kv_state_machine_does_not_apply_uncommitted_entries():
    c = build_cluster()
    c.run_election("n1")
    c.isolate("n3")
    c.isolate("n4")
    c.isolate("n5")
    c.propose("n1", {"op": "SET", "key": "b", "value": "2"})  # can't commit, minority reachable
    kv = KVStateMachine()
    kv.apply_committed(c.nodes["n1"])
    assert kv.get("b") is None


def test_election_restriction_denies_vote_to_candidate_with_stale_log():
    c = build_cluster(n=3)
    # n1 becomes leader and commits an entry, advancing its log.
    c.run_election("n1")
    c.propose("n1", {"op": "SET", "key": "k", "value": "v"})

    # n2's log is now caught up (replicated). n3 requests a vote for itself
    # with an artificially stale log (term 0) -- n2 must refuse per the
    # election-restriction safety rule, even in a fresh term.
    n2 = c.nodes["n2"]
    term, granted = n2.handle_request_vote(
        term=n2.current_term + 1, candidate_id="n3", last_log_index=0, last_log_term=0
    )
    assert not granted


def test_log_conflict_is_truncated_and_overwritten():
    c = build_cluster(n=3)
    c.run_election("n1")
    leader = c.nodes["n1"]
    follower = c.nodes["n2"]

    # Manually inject a conflicting uncommitted entry into the follower's log
    # (simulating it having followed a different, now-defunct leader).
    follower.log.append(LogEntry(term=99, command={"op": "SET", "key": "stale", "value": "x"}))

    c.propose("n1", {"op": "SET", "key": "real", "value": "1"})
    # The follower's conflicting entry must have been overwritten by the
    # real leader's entry at that index, per Raft's log-matching property.
    assert follower.log[0].term == leader.current_term
