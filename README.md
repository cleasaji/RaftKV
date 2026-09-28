# 🗳️ RaftKV — Distributed Consensus Key-Value Store

A real, from-scratch implementation of the **Raft consensus algorithm**
(Ongaro & Ousterhout) — leader election with the safety-critical
election restriction, AppendEntries log replication with conflict
resolution, and majority-based commit — backing a replicated
key-value store. Proven under simulated network partitions, not just
the happy path.

---

## Run it yourself

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Or open `frontend/index.html` for a live 5-node cluster console — elect
leaders, isolate/heal nodes, and propose SETs while watching term/log/commit
state update in real time.

## The core safety property, actually implemented and tested

```python
def test_election_restriction_denies_vote_to_candidate_with_stale_log():
    # a candidate with an out-of-date log must be refused a vote,
    # even in a brand-new term -- this is THE property that keeps
    # a committed entry from ever being lost
    n2 = c.nodes["n2"]
    _, granted = n2.handle_request_vote(term=..., candidate_id="n3", last_log_index=0, last_log_term=0)
    assert not granted
```

This is Raft's central safety guarantee: a node only votes for a
candidate whose log is *at least as up-to-date* as its own
(`last_log_term > mine`, or equal term with `last_log_index >= mine`).
Without this check, a node that missed the last few committed entries
could become leader and silently overwrite them — this is the single
rule that prevents that.

## Split-brain is impossible by construction

```python
def test_partitioned_minority_leader_cannot_commit():
    c.run_election("n1")
    c.isolate("n3"); c.isolate("n4"); c.isolate("n5")   # leader stuck with 1 reachable follower
    index = c.propose("n1", {"op": "SET", "key": "x", "value": "1"})
    assert c.nodes["n1"].commit_index < index   # 2 of 5 reachable is not a majority -- never commits
```

`replicate()` only advances `commit_index` when a **majority of the
whole cluster** (not just reachable nodes) has replicated an entry
written in the leader's *current* term — the second half of that rule
(current-term-only) is Raft's other subtle safety requirement, and is
exercised by `test_propose_replicates_and_commits_with_majority`.

## Log conflicts are resolved, not just appended

```python
def test_log_conflict_is_truncated_and_overwritten():
    follower.log.append(LogEntry(term=99, command={...}))  # a stale, conflicting entry
    c.propose("n1", {"op": "SET", "key": "real", "value": "1"})
    assert follower.log[0].term == leader.current_term   # overwritten, not left in place
```

`handle_append_entries` implements the log-matching property from the
paper: when a follower's entry at some index conflicts with what the
leader sends, everything from that point on is truncated and replaced
— the mechanism that reconciles a follower that briefly followed a
different (now-defunct) leader.

## Tests

```bash
cd backend
pip install -r requirements.txt
pytest tests/ -v
```

10 tests: election success with a fully reachable cluster, election
failure when the candidate itself is isolated, election success with a
minority partitioned away (majority still reachable), propose/commit
via majority replication, proposing to a non-leader correctly raising,
a partitioned minority leader correctly failing to commit, the KV state
machine applying only committed entries (and correctly withholding
uncommitted ones), the election-restriction safety test, and log
conflict resolution.

## Project layout

```
backend/
  app/
    raft_node.py     # RequestVote + AppendEntries RPC handlers, election/leader state
    cluster.py           # simulated network (with partition support) + replication loop
    kv_store.py               # state machine applying only committed log entries
    main.py
  tests/
    test_raftkv.py
frontend/
  index.html
```

## Honest scope

Nodes communicate via direct synchronous method calls (a simulated
in-memory network), not real sockets/threads across processes — this
is the standard approach for testing Raft's *algorithm* deterministically
(the same technique MIT's 6.824 distributed systems labs start from)
rather than fighting real network nondeterminism. No log compaction/
snapshotting or cluster membership changes (both are real Raft
extensions beyond the paper's core). What's real: the election
restriction, majority-based commit with the current-term rule, and
log-matching conflict resolution — the three properties that actually
make Raft correct, all independently tested.
