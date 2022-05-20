/*
 * Copyright 2021-2022 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package exchange.core2.raftification;


import exchange.core2.raftification.messages.*;
import exchange.core2.raftification.repository.IRaftLogRepository;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RaftNode<T extends RsmRequest, S extends RsmResponse> {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    public static final int HEARTBEAT_TIMEOUT_MS = 2000 + (int) (Math.random() * 500);
    public static final int HEARTBEAT_LEADER_RATE_MS = 1000;
    public static final int ELECTION_TIMEOUT_MIN_MS = 2500;
    public static final int ELECTION_TIMEOUT_MAX_MS = 2800;
    public static final int APPEND_REPLY_TIMEOUT_MAX_MS = 1000;

    public static final int TRANSFER_ITEMS_NUM_LIMIT = 10;

    /* **** Persistent state on all servers: (Updated on stable storage before responding to RPCs) */

    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    private int currentTerm = 0;

    // candidateId that received vote in current term (or -1 if none)
    private int votedFor = -1;

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private final IRaftLogRepository<T> logRepository;

    /* **** Volatile state on all servers: */

    // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
    private long commitIndex = 0;

    // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)
    private long lastApplied = 0;

    private RaftNodeState currentState = RaftNodeState.FOLLOWER;

    /* **** Volatile state on leaders: (Reinitialized after election) */

    // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    //
    // The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will
    // send to that follower. When a leader first comes to power, it initializes all nextIndex values to the index just after the
    // last one in its log (11 in Figure 7). If a follower’s log is inconsistent with the leader’s, the AppendEntries consistency
    // check will fail in the next AppendEntries RPC. After a rejection, the leader decrements nextIndex and retries
    // the AppendEntries RPC. Eventually nextIndex will reach a point where the leader and follower logs match. When
    // this happens, AppendEntries will succeed, which removes any conflicting entries in the follower’s log and appends
    // entries from the leader’s log (if any). Once AppendEntries succeeds, the follower’s log is consistent with the leader’s,
    // and it will remain that way for the rest of the term.
    private final long[] nextIndex = new long[3];

    // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    private final long[] matchIndex = new long[3];

    // EXTRA: ending only one addRecordsMessage to each server
    private final long[] correlationIds = new long[3];
    private final long[] timeSent = new long[3];
    private final long[] sentUpTo = new long[3];
    private final long[] lastHeartBeatSentNs = new long[3];


    private final LongObjectHashMap<ClientAddress> clientResponsesMap = new LongObjectHashMap<>();

    /* ********************************************* */

    private final int currentNodeId;
    private final int[] otherNodes;

    private final RpcService<T, S> rpcService;

    private final ReplicatedStateMachine<T, S> rsm;

    // timers
    private long lastHeartBeatReceivedNs = System.nanoTime();
    private long electionEndNs = System.nanoTime();


    public RaftNode(int thisNodeId,
                    IRaftLogRepository<T> logRepository,
                    ReplicatedStateMachine<T, S> rsm,
                    RsmRequestFactory<T> msgFactory,
                    RsmResponseFactory<S> respFactory) {

        // localhost:3778, localhost:3779, localhost:3780
        final Map<Integer, String> remoteNodes = Map.of(
                0, "localhost:3778",
                1, "localhost:3779",
                2, "localhost:3780");

        this.logRepository = logRepository;
        this.currentNodeId = thisNodeId;
        this.rsm = rsm;
        this.otherNodes = remoteNodes.keySet().stream().mapToInt(x -> x).filter(nodeId -> nodeId != thisNodeId).toArray();

        final RpcHandler<T, S> handler = new RpcHandler<>() {
            @Override
            public RpcResponse handleNodeRequest(int fromNodeId, RpcRequest req) {
                log.debug("INCOMING REQ {} >>> {}", fromNodeId, req);

                if (req instanceof CmdRaftVoteRequest voteRequest) {

                    synchronized (this) {
                    /* Receiver implementation:
                    1. Reply false if term < currentTerm (§5.1)
                    2. If votedFor is null or candidateId, and candidate’s log is at
                    least as up-to-date as receiver’s log, grant vote (5.2, 5.4) */

                        if (voteRequest.term() < currentTerm) {
                            log.debug("Reject vote for {} - term is old", fromNodeId);
                            return new CmdRaftVoteResponse(currentTerm, false);
                        }

                        if (voteRequest.term() > currentTerm) {
                            log.debug("received newer term {} with vote request", voteRequest.term());
                            currentTerm = voteRequest.term();
                            votedFor = -1; // never voted in newer term
                            switchToFollower();
                            resetFollowerAppendTimer();
                        }

                        if (votedFor != -1) {
//                        if (votedFor != -1 && votedFor != currentNodeId) {
                            log.debug("Reject vote for {} - already voted for {}", fromNodeId, votedFor);
                            return new CmdRaftVoteResponse(currentTerm, false);
                        }

                        log.debug("VOTE GRANTED for {}", fromNodeId);
                        votedFor = fromNodeId;

                        return new CmdRaftVoteResponse(currentTerm, true);
                    }

                }
                if (req instanceof CmdRaftAppendEntries cmd) {

                    synchronized (this) {

                        // 1. Reply false if term < currentTerm
                        if (cmd.term() < currentTerm) {
                            log.debug("Ignoring leader with older term {} (current={}", cmd.term(), currentTerm);
                            return new CmdRaftAppendEntriesResponse(currentTerm, false);
                        }

                        if (currentState == RaftNodeState.CANDIDATE) {
                            /* While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
                            If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
                            then the candidate recognizes the leader as legitimate and returns to follower state.
                            If the term in the RPC is smaller than the candidate’s current term,
                            then the candidate rejects the RPC and continues in candidate state. */

                            log.debug("Switch from Candidate to follower");

                            switchToFollower();
                        }

                        if (cmd.term() > currentTerm) {
                            log.info("Update term {}->{}", currentTerm, cmd.term());
                            currentTerm = cmd.term();
                            switchToFollower();
                        }

                        if (currentState == RaftNodeState.FOLLOWER && votedFor != cmd.leaderId()) {
                            log.info("Changed votedFor to {}", cmd.leaderId());
                            votedFor = cmd.leaderId(); // to inform client who accessing followers
                        }

                        resetFollowerAppendTimer();

                        if (cmd.entries().isEmpty()) {
                            return new CmdRaftAppendEntriesResponse(currentTerm, true);
                        }

                        log.debug("Adding new records into the log...");

                        // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
                        final long prevLogIndex = cmd.prevLogIndex();
                        if (prevLogIndex > 0 && prevLogIndex != logRepository.getLastLogIndex()) {
                            log.warn("Reject - log doesn’t contain an entry at prevLogIndex={} (last is {}))", prevLogIndex, logRepository.getLastLogIndex());
                            return new CmdRaftAppendEntriesResponse(currentTerm, false);
                        }

                        final int lastLogTerm = logRepository.getLastLogTerm();
                        if (cmd.prevLogTerm() != lastLogTerm) {
                            log.warn("Reject - log last record has different term {}, expected prevLogTerm={}", lastLogTerm, cmd.prevLogTerm());
                            return new CmdRaftAppendEntriesResponse(currentTerm, false);
                        }

                        // 3. If an existing entry conflicts with a new one (same index but different terms),
                        // delete the existing entry and all that follow it
                        // 4. Append any new entries not already in the log
                        final List<RaftLogEntry<T>> entries = cmd.entries();
                        logRepository.appendOrOverride(entries, prevLogIndex);

                        log.debug("Added to log repository: {}", cmd.entries());

                        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                        if (cmd.leaderCommit() > commitIndex) {
                            commitIndex = Math.min(cmd.leaderCommit(), logRepository.getLastLogIndex());
                            log.debug("set commitIndex to {}", commitIndex);
                        }

                        // todo can do in different thread
                        applyPendingEntriesToStateMachine();

                        return new CmdRaftAppendEntriesResponse(currentTerm, true);
                    }
                }

                return null;
            }

            @Override
            public void handleNodeResponse(int fromNodeId, RpcResponse resp, long correlationId) {
                log.debug("INCOMING RESP {} >>> {}", fromNodeId, resp);

                /* A candidate wins an election if it receives votes from
                a majority of the servers in the full cluster for the same
                term. Each server will vote for at most one candidate in a
                given term, on a first-come-first-served basis
                (note: Section 5.4 adds an additional restriction on votes) */

                if (resp instanceof final CmdRaftVoteResponse voteResponse) {
                    synchronized (this) {
                        if (currentState == RaftNodeState.CANDIDATE && voteResponse.voteGranted() && voteResponse.term() == currentTerm) {
                            switchToLeader();
                        }
                    }
                } else if (resp instanceof final CmdRaftAppendEntriesResponse appendResponse) {
                    synchronized (this) {
                        if (correlationId == correlationIds[fromNodeId]) {


                            timeSent[fromNodeId] = 0L;

                            if (appendResponse.success()) {

                                log.debug("current sentUpTo[{}]={}", fromNodeId, sentUpTo[fromNodeId]);

                                matchIndex[fromNodeId] = sentUpTo[fromNodeId];
                                nextIndex[fromNodeId] = sentUpTo[fromNodeId] + 1;

                                // If there exists an N such that
                                // N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm:
                                // set commitIndex = N (5.3, 5.4).

                                if (matchIndex[fromNodeId] > commitIndex) {
                                    log.debug("lastEntryInTerm({}, {}, {});", commitIndex, matchIndex[fromNodeId], currentTerm);
                                    final long lastEntryInTerm = logRepository.findLastEntryInTerm(commitIndex, matchIndex[fromNodeId], currentTerm);

                                    final long newCommitIndex = Math.max(
                                            Math.max(commitIndex, matchIndex[fromNodeId]),
                                            lastEntryInTerm);

                                    if (commitIndex != newCommitIndex) {
                                        log.debug("updated commitIndex: {}->{}", commitIndex, newCommitIndex);
                                    }

                                    commitIndex = newCommitIndex;

                                    // TODO another thread
                                    applyPendingEntriesToStateMachine();
                                }

                            } else {
                                if (nextIndex[fromNodeId] > 1) {
                                    log.debug("decrementing nextIndex[{}] to {}", fromNodeId, nextIndex[fromNodeId] - 1);
                                    nextIndex[fromNodeId]--;
                                } else {
                                    log.warn("Can not decrement nextIndex[{}]", fromNodeId);
                                }
                            }
                        }
                    }
                }

            }


            @Override
            public CustomCommandResponse<S> handleClientRequest(final InetAddress address,
                                                                final int port,
                                                                final long correlationId,
                                                                final long timeReceived,
                                                                final CustomCommandRequest<T> request) {
                synchronized (this) {

                    if (currentState == RaftNodeState.LEADER) {
                        // If command received from client: append entry to local log,
                        // respond after entry applied to state machine (5.3)

                        // adding new record into the local log
                        final RaftLogEntry<T> logEntry = new RaftLogEntry<>(currentTerm, request.rsmRequest(), timeReceived);
                        final long index = logRepository.appendEntry(logEntry, true);

                        // remember client request (TODO !! on batch migration - should refer to the last record)
                        clientResponsesMap.put(index, new ClientAddress(address, port, correlationId));

                    } else {
                        log.debug("Redirecting client to leader nodeId={}", votedFor);
                        // inform client about different leader
                        return new CustomCommandResponse<>(respFactory.emptyResponse(), votedFor, false);
                    }
                }

                return null;
            }

        };

        // todo remove from constructor
        rpcService = new RpcService<>(remoteNodes, handler, msgFactory, respFactory, thisNodeId);

        log.info("HEARTBEAT_TIMEOUT_MS={}", HEARTBEAT_TIMEOUT_MS);
        log.info("ELECTION_TIMEOUT_MS={}..{}", ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);

        log.info("Starting node {} as follower...", thisNodeId);
        resetFollowerAppendTimer();

        new Thread(this::workerThread).start();

    }


    private void workerThread() {

        try {

            while (true) {

                synchronized (this) {

                    if (currentState == RaftNodeState.FOLLOWER) {

                        if (System.nanoTime() > lastHeartBeatReceivedNs + HEARTBEAT_TIMEOUT_MS * 1_000_000L) {
                            appendTimeout();
                        } else {

                        }

                    }

                    if (currentState == RaftNodeState.CANDIDATE) {
                        final long t = System.nanoTime();
                        if (t > electionEndNs) {
                            appendTimeout();
                        }
                    }

                    if (currentState == RaftNodeState.LEADER) {

                        // If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
                        // If successful: update nextIndex and matchIndex for follower (5.3)
                        // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (5.3)


                        final long prevLogIndex = logRepository.getLastLogIndex();

                        Arrays.stream(otherNodes).forEach(targetNodeId -> {

                            final long nextIndexForNode = nextIndex[targetNodeId];

                            final long t = System.nanoTime();
                            final boolean timeToSendHeartbeat = t > lastHeartBeatSentNs[targetNodeId] + HEARTBEAT_LEADER_RATE_MS * 1_000_000L;

                            //log.debug("timeToSendHeartbeat={}",timeToSendHeartbeat);

                            // have records and did not send batch recently
                            final boolean canRetry = prevLogIndex >= nextIndexForNode
                                    && (correlationIds[targetNodeId] == 0L || t > timeSent[targetNodeId] + APPEND_REPLY_TIMEOUT_MAX_MS * 1_000_000L);

                            //log.debug("canRetry={}",canRetry);

                            if (canRetry || timeToSendHeartbeat) {

                                final List<RaftLogEntry<T>> newEntries = logRepository.getEntries(nextIndexForNode, TRANSFER_ITEMS_NUM_LIMIT);

                                // avoid additional request
                                final int prevLogTerm = logRepository.getEntries(nextIndexForNode - 1, 1).stream()
                                        .findFirst()
                                        .map(RaftLogEntry::term)
                                        .orElse(0);

                                log.debug("node {} : nextIndexForNode={} newEntries={} prevLogTerm={}", targetNodeId, nextIndexForNode, newEntries, prevLogTerm);


                                final CmdRaftAppendEntries appendRequest = new CmdRaftAppendEntries(
                                        currentTerm,
                                        currentNodeId,
                                        nextIndexForNode - 1,
                                        prevLogTerm,
                                        newEntries,
                                        commitIndex);

                                final long corrId = rpcService.callRpcAsync(appendRequest, targetNodeId);
                                log.info("Sent {} entries to {}: {} corrId={}", newEntries.size(), targetNodeId, appendRequest, corrId);

                                if (!newEntries.isEmpty()) {
                                    correlationIds[targetNodeId] = corrId;
                                    timeSent[targetNodeId] = System.nanoTime();
                                    sentUpTo[targetNodeId] = nextIndexForNode - 1 + newEntries.size();
                                    log.debug("correlationIds[{}]={}", targetNodeId, corrId);
                                    log.debug("set sentUpTo[{}]={}", targetNodeId, sentUpTo[targetNodeId]);
                                }

                                lastHeartBeatSentNs[targetNodeId] = System.nanoTime();
                            }
                        });

                    }


                }

                Thread.sleep(10);
            }


        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


    private void switchToFollower() {

        if (currentState != RaftNodeState.FOLLOWER) {
            log.debug("Switching to follower (reset votedFor, start append timer)");
            currentState = RaftNodeState.FOLLOWER;
            votedFor = -1;
            resetFollowerAppendTimer();
        }
    }


    private synchronized void resetFollowerAppendTimer() {
//        logger.debug("reset append timer");
        lastHeartBeatReceivedNs = System.nanoTime();
    }

    /**
     * To begin an election, a follower increments its current
     * term and transitions to candidate state. It then votes for
     * itself and issues RequestVote RPCs in parallel to each of
     * the other servers in the cluster. A candidate continues in
     * this state until one of three things happens:
     * (a) it wins the election,
     * (b) another server establishes itself as leader, or
     * (c) a period of time goes by with no winner.
     */
    private synchronized void appendTimeout() {

        // TODO double-check last receiving time

        currentState = RaftNodeState.CANDIDATE;

        // On conversion to candidate, start election:
        // - Increment currentTerm
        // - Vote for self
        // - Reset election timer
        // - Send RequestVote RPCs to all other servers
        currentTerm++;

        log.info("heartbeat timeout - switching to CANDIDATE, term={}", currentTerm);

        votedFor = currentNodeId;

        final int prevLogTerm = logRepository.getLastLogTerm();
        final long prevLogIndex = logRepository.getLastLogIndex();

        final CmdRaftVoteRequest voteReq = new CmdRaftVoteRequest(
                currentTerm,
                currentNodeId,
                prevLogIndex,
                prevLogTerm);

        rpcService.callRpcAsync(voteReq, otherNodes[0]);
        rpcService.callRpcAsync(voteReq, otherNodes[1]);

        final int timeoutMs = ELECTION_TIMEOUT_MIN_MS + (int) (Math.random() * (ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS));
        log.debug("ElectionTimeout: {}ms", timeoutMs);
        electionEndNs = System.nanoTime() + timeoutMs * 1_000_000L;
    }

    private void switchToLeader() {
        log.info("Becoming a LEADER!");
        currentState = RaftNodeState.LEADER;

        // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
        final long next = logRepository.getLastLogIndex() + 1;
        Arrays.fill(nextIndex, next);

        // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
        Arrays.fill(matchIndex, 0);
    }

    private void applyPendingEntriesToStateMachine() {

        /*
        All Servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (5.3)
        */

        // TODO request range (up to commitIndex)
        final List<RaftLogEntry<T>> entries = logRepository.getEntries(lastApplied + 1, Integer.MAX_VALUE);
        int idx = 0;

        while (lastApplied < commitIndex) {
            lastApplied++;
//            final RaftLogEntry<T> raftLogEntry = logRepository.getEntryOpt(lastApplied)
//                    .orElseThrow(() -> new RuntimeException("Can not find pending entry index=" + lastApplied + " in the repository"));

            final RaftLogEntry<T> raftLogEntry = entries.get(idx++);

            log.debug("Applying to RSM: {}", raftLogEntry);
            final S result = rsm.applyCommand(raftLogEntry.cmd());

            if (currentState == RaftNodeState.LEADER) {

                // respond to client that batch has applied
                final ClientAddress c = clientResponsesMap.get(lastApplied);
                log.debug("Replying to client lastApplied={} c={}", lastApplied, c);
                rpcService.respondToClient(
                        c.address,
                        c.port,
                        c.correlationId,
                        new CustomCommandResponse<>(result, currentNodeId, true));
            }

        }


    }


    public enum RaftNodeState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    // TODO can move to RpcService
    private record ClientAddress(InetAddress address, int port, long correlationId) {
    }
}