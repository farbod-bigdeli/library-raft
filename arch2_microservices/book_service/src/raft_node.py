import threading
import time
import random
from typing import Dict

import grpc

import src.raft_pb2 as raft_pb2
import src.raft_pb2_grpc as raft_pb2_grpc


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id: int, peers: Dict[int, str]):
        """
        node_id: this node's ID (e.g., 1, 2, 3)
        peers: mapping {peer_id: "host:port"} for all nodes (may include self;
               we will filter self out defensively)
        """
        self.node_id = node_id

        # Ensure we never include ourselves in peers to avoid self-RPCs
        original_len = len(peers)
        self.peers = {pid: addr for pid, addr in peers.items() if pid != node_id}
        if len(self.peers) != original_len:
            print(f"[RaftNode] Node {node_id}: filtered itself out of peers list.")

        # Raft persistent/volatile state
        self.current_term = 0
        self.voted_for = None
        self.state = "FOLLOWER"  # "FOLLOWER" | "CANDIDATE" | "LEADER"
        self.votes_received = set()

        # NEW: log replication state
        # log[i] is a raft_pb2.LogEntry with index starting from 1
        self.log: list[raft_pb2.LogEntry] = []
        self.commit_index = 0  # c in the description
        self.last_applied = 0  # last log index we have executed
        self.leader_id = None  # known leader (for forwarding)

        # Timing
        self.heartbeat_interval = 1.0  # seconds (leader heartbeat)
        self._reset_election_timeout()
        self.last_heartbeat_or_election = time.monotonic()

        # gRPC stubs to peers
        self.stubs: Dict[int, raft_pb2_grpc.RaftServiceStub] = {}
        for pid, addr in self.peers.items():
            channel = grpc.insecure_channel(addr)
            self.stubs[pid] = raft_pb2_grpc.RaftServiceStub(channel)

        self.lock = threading.Lock()

    # ----------------------------------------------------------------------
    # Helpers for timeouts
    # ----------------------------------------------------------------------
    def _reset_election_timeout(self):
        # election timeout (randomized)
        self.election_timeout = random.uniform(1.5, 3)

    # ----------------------------------------------------------------------
    # RPC HANDLERS (SERVER SIDE)
    # ----------------------------------------------------------------------
    def RequestVote(self, request, context):
        # SERVER LOG
        print(
            f"Node {self.node_id} runs RPC RequestVote called by Node {request.candidate_id}"
        )

        with self.lock:
            if request.term < self.current_term:
                # Reply false if term < currentTerm
                return raft_pb2.RequestVoteResponse(
                    term=self.current_term, vote_granted=False
                )

            # If we see a newer term, update and become follower
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = "FOLLOWER"
                self.voted_for = None

            vote_granted = False
            # Simple rule: grant vote if we haven't voted or already voted for this candidate
            if self.voted_for is None or self.voted_for == request.candidate_id:
                self.voted_for = request.candidate_id
                vote_granted = True
                # Reset election timeout since we heard from a candidate
                self.last_heartbeat_or_election = time.monotonic()
                self._reset_election_timeout()

        return raft_pb2.RequestVoteResponse(
            term=self.current_term,
            vote_granted=vote_granted
        )

    def AppendEntries(self, request, context):
        # SERVER LOG
        print(
            f"Node {self.node_id} runs RPC AppendEntries called by Node {request.leader_id}"
        )

        with self.lock:
            # Reject old terms
            if request.term < self.current_term:
                return raft_pb2.AppendEntriesResponse(
                    term=self.current_term, success=False
                )

            # If we see a newer term, update and become follower
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = "FOLLOWER"
                self.voted_for = None

            # Track current leader
            if int(request.leader_id) != int(self.node_id):
                self.leader_id = int(request.leader_id)

            # Defensive: ignore our own heartbeats so we don't demote ourselves
            if int(request.leader_id) == int(self.node_id):
                return raft_pb2.AppendEntriesResponse(
                    term=self.current_term,
                    success=True
                )

            # ---- LOG REPLICATION PART ----
            # Simplified rule: copy entire log from leader
            # (this matches the assignment's simplified spec)
            self.log = list(request.entries)

            # Update commit index and execute as needed
            if request.commit_index > self.commit_index:
                self.commit_index = request.commit_index
                self._apply_entries_locked()

            self.state = "FOLLOWER"
            self.last_heartbeat_or_election = time.monotonic()
            self._reset_election_timeout()

        return raft_pb2.AppendEntriesResponse(
            term=self.current_term,
            success=True
        )

    def ClientRequest(self, request, context):
        """
        RPC used for *forwarding* client operations to the leader.

        External client can hit any node. If that node is not the leader,
        it forwards via this RPC to the current leader.

        We also use this RPC between nodes, so we log sender via source_id.
        """
        # SERVER LOG
        print(
            f"Node {self.node_id} runs RPC ClientRequest called by Node {request.source_id}"
        )

        with self.lock:
            is_leader = (self.state == "LEADER")
            leader_id = self.leader_id

        if not is_leader:
            # We are not leader: forward to known leader
            if leader_id is None or leader_id not in self.stubs:
                # No known leader or unreachable
                return raft_pb2.ClientOpResponse(
                    success=False,
                    message=f"Node {self.node_id}: no known leader to forward to."
                )

            # CLIENT LOG for forwarding
            print(
                f"Node {self.node_id} sends RPC ClientRequest to Node {leader_id}"
            )

            stub = self.stubs[leader_id]
            forward_req = raft_pb2.ClientOpRequest(
                source_id=self.node_id,
                operation=request.operation
            )
            try:
                resp = stub.ClientRequest(forward_req, timeout=5)
                return resp
            except grpc.RpcError as er:
                return raft_pb2.ClientOpResponse(
                    success=False,
                    message=f"Forwarding failed from {self.node_id} to leader {leader_id}: {er}"
                )

        # We are the leader: append to log and replicate
        success = self._append_and_replicate(request.operation)
        msg = "Operation committed" if success else "Operation replication failed"
        return raft_pb2.ClientOpResponse(success=success, message=msg)

    # ----------------------------------------------------------------------
    # RAFT LOGIC
    # ----------------------------------------------------------------------
    def start(self):
        """Start the background Raft loop in a thread."""
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

    def _run_loop(self):
        """Main Raft loop: handle election timeouts & heartbeats."""
        while True:
            time.sleep(1)

            now = time.monotonic()
            with self.lock:
                state = self.state
                elapsed = now - self.last_heartbeat_or_election
                election_timeout = self.election_timeout

            # Election timeout for followers/candidates
            if state in ("FOLLOWER", "CANDIDATE") and elapsed >= election_timeout:
                print(
                    f"Election timeout triggered for node: {self.node_id} "
                )
                self._start_election()

            # Heartbeat sending by leader (also carries log for replication)
            if state == "LEADER":
                # Use last_heartbeat_or_election as last heartbeat sent
                if elapsed >= self.heartbeat_interval:
                    self._send_heartbeats()

    def _start_election(self):
        with self.lock:
            self.state = "CANDIDATE"
            self.current_term += 1
            term = self.current_term

            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
            self.last_heartbeat_or_election = time.monotonic()
            self._reset_election_timeout()

        request = raft_pb2.RequestVoteRequest(
            term=term, candidate_id=int(self.node_id)
        )

        # Send RequestVote to all peers in parallel
        for peer_id, stub in self.stubs.items():
            threading.Thread(
                target=self._send_request_vote,
                args=(peer_id, stub, request),
                daemon=True,
            ).start()

    def _send_request_vote(self, peer_id, stub, request):
        # CLIENT LOG
        print(
            f"Node {self.node_id} sends RPC RequestVote to Node {peer_id}"
        )
        try:
            response = stub.RequestVote(request, timeout=1)
        except grpc.RpcError as er:
            return  # peer down / network issues, ignore

        with self.lock:
            # If we are no longer a candidate, ignore result
            if self.state != "CANDIDATE":
                return

            if response.term > self.current_term:
                # Discovered higher term -> step down
                self.current_term = response.term
                self.state = "FOLLOWER"
                self.voted_for = None
                self.last_heartbeat_or_election = time.monotonic()
                self._reset_election_timeout()
                return

            if response.vote_granted and response.term == self.current_term:
                self.votes_received.add(peer_id)
                # Majority? (peers + self)
                if len(self.votes_received) > (len(self.peers) + 1) // 2:
                    self._become_leader()

    def _become_leader(self):
        # Called with self.lock already held
        if self.state == "LEADER":
            return
        self.state = "LEADER"
        self.leader_id = self.node_id
        print(
            f"Node {self.node_id} becomes LEADER "
            f"for term {self.current_term}"
        )
        # Force immediate heartbeat
        self.last_heartbeat_or_election = 0.0

    # ----------------------------------------------------------------------
    # LOG REPLICATION HELPERS
    # ----------------------------------------------------------------------
    def _append_and_replicate(self, operation: str) -> bool:
        """
        Leader-only: append <operation, term, k+1> to log and replicate
        entire log to followers. When majority ACK, commit and apply.
        """
        with self.lock:
            new_index = len(self.log) + 1
            entry = raft_pb2.LogEntry(
                index=new_index,
                term=self.current_term,
                operation=operation
            )
            self.log.append(entry)
            term = self.current_term
            # c (commit_index) is not advanced yet
            current_commit_index = self.commit_index

        # Build request carrying entire log, current c
        request = raft_pb2.AppendEntriesRequest(
            term=int(term),
            leader_id=int(self.node_id),
            entries=self.log,
            commit_index=current_commit_index,
        )

        # Count self as ACK
        acks = 1

        for peer_id, stub in self.stubs.items():
            # CLIENT LOG
            print(
                f"Node {self.node_id} sends RPC AppendEntries to Node {peer_id}"
            )
            try:
                response = stub.AppendEntries(request, timeout=1)
            except grpc.RpcError as er:
                print(f"[AppendEntries ERROR] leader {self.node_id} → {peer_id}: {er}")
                continue

            if response.success and response.term == term:
                acks += 1

        # Check majority
        if acks > (len(self.peers) + 1) // 2:
            # Majority ACK: commit all pending entries
            with self.lock:
                self.commit_index = len(self.log)
                self._apply_entries_locked()
            return True

        return False

    def _apply_entries_locked(self):
        """
        Apply all log entries up to commit_index that
        have not yet been executed.
        Assumes self.lock is held.
        """
        while self.last_applied < self.commit_index and self.last_applied < len(self.log):
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]  # index starts at 1
            # "Execute" operation o -- here we just print,
            # but you could hook this into your state machine / DB.
            print(
                f"[APPLY] Node {self.node_id} executes log[{entry.index}] "
                f"(term={entry.term}) operation={entry.operation}"
            )

    def _send_heartbeats(self):
        with self.lock:
            term = self.current_term
            self.last_heartbeat_or_election = time.monotonic()
            # Heartbeat also carries log and current commit_index
            entries = self.log
            commit_index = self.commit_index

        request = raft_pb2.AppendEntriesRequest(
            term=int(term),
            leader_id=int(self.node_id),
            entries=entries,
            commit_index=commit_index,
        )

        for peer_id, stub in self.stubs.items():
            # CLIENT LOG
            print(
                f"Node {self.node_id} sends RPC AppendEntries to Node {peer_id}"
            )
            try:
                response = stub.AppendEntries(request, timeout=0.5)
            except grpc.RpcError:
                continue

            with self.lock:
                if response.term > self.current_term:
                    # Discovered newer term -> become follower
                    self.current_term = response.term
                    self.state = "FOLLOWER"
                    self.voted_for = None
                    self.leader_id = None
                    self.last_heartbeat_or_election = time.monotonic()
                    self._reset_election_timeout()

    # ----------------------------------------------------------------------
    # Helper for application layer (BookService, etc.)
    # ----------------------------------------------------------------------
    def handle_client_operation(self, operation: str) -> bool:
        """
        Called by the *application* when a client wants to execute operation o.

        If this node is leader, it appends and replicates the log.
        If this node is follower, it forwards via ClientRequest RPC to the leader.

        Returns True if committed, False otherwise.
        """
        with self.lock:
            is_leader = (self.state == "LEADER")
            leader_id = self.leader_id

        if is_leader:
            return self._append_and_replicate(operation)

        # We are follower: forward to leader
        if leader_id is None or leader_id not in self.stubs:
            print(f"[ClientOp] Node {self.node_id}: no known leader to forward to.")
            return False

        print(
            f"Node {self.node_id} sends RPC ClientRequest to Node {leader_id}"
        )
        stub = self.stubs[leader_id]
        req = raft_pb2.ClientOpRequest(
            source_id=self.node_id,
            operation=operation
        )
        try:
            resp = stub.ClientRequest(req, timeout=5)
        except grpc.RpcError as er:
            print(
                f"[ClientOp] Forward from {self.node_id} to leader {leader_id} failed: {er}"
            )
            return False

        print(
            f"[ClientOp] Node {self.node_id} got response from leader {leader_id}: "
            f"success={resp.success}, msg={resp.message}"
        )
        return resp.success

    def is_leader(self) -> bool:
        with self.lock:
            return self.state == "LEADER"
