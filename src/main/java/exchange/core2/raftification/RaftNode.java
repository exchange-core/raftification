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
import java.util.stream.IntStream;

public class RaftNode<T extends RsmCommand, Q extends RsmQuery, S extends RsmResponse> {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    public static final int HEARTBEAT_TIMEOUT_MS = 2000 + (int) (Math.random() * 500);
    public static final int HEARTBEAT_LEADER_RATE_MS = 1000;
    public static final int ELECTION_TIMEOUT_MIN_MS = 2500;
    public static final int ELECTION_TIMEOUT_MAX_MS = 2800;
    public static final int APPEND_REPLY_TIMEOUT_MAX_MS = 1000;

    public static final int TRANSFER_ITEMS_NUM_LIMIT = 10;

    /* **** Persistent state on all servers: (Updated on stable storage before responding to RPCs) */

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private final IRaftLogRepository<T> logRepository;

    /* **** Volatile state on all servers: */

    // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
    private long commitIndex = 0; // TODO initialize from Repository (or statemachine?)

    // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)
    private long lastApplied = 0; // TODO initialize from Repository (or statemachine?)

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

    private final RpcService<T, Q, S> rpcService;

    // replicated state machine implementation
    private final ReplicatedStateMachine<T, Q, S> rsm;

    // timers
    private long lastHeartBeatReceivedNs = System.nanoTime();
    private long electionEndNs = System.nanoTime();

    public RaftNode(List<String> remoteNodes,
                    int thisNodeId,
                    IRaftLogRepository<T> logRepository,
                    ReplicatedStateMachine<T, Q, S> rsm,
                    RsmRequestFactory<T, Q> msgFactory,
                    RsmResponseFactory<S> respFactory) {

        this.logRepository = logRepository;
        this.currentNodeId = thisNodeId;
        this.rsm = rsm;
        this.otherNodes = IntStream.range(0, 3).filter(nodeId -> nodeId != thisNodeId).toArray();

        final RpcHandler<T, Q, S> handler = new RpcHandler<>() {
            @Override
            public RpcResponse handleNodeRequest(int fromNodeId, RpcRequest req) {
                log.debug("INCOMING REQ {} >>> {}", fromNodeId, req);

                synchronized (rsm) {

                    if (req instanceof CmdRaftVoteRequest voteRequest) {

                    /* Receiver implementation:
                    1. Reply false if term < currentTerm (§5.1)
                    2. If votedFor is null or candidateId, and candidate’s log is at
                    least as up-to-date as receiver’s log, grant vote (5.2, 5.4) */

                        if (voteRequest.term() < logRepository.getCurrentTerm()) {
                            log.debug("Reject vote for {} - term is old", fromNodeId);
                            return new CmdRaftVoteResponse(logRepository.getCurrentTerm(), false);
                        }

                        if (voteRequest.term() > logRepository.getCurrentTerm()) {
                            log.debug("received newer term {} with vote request", voteRequest.term());
                            logRepository.setCurrentTerm(voteRequest.term());
                            logRepository.setVotedFor(-1); // never voted in newer term
                            switchToFollower();
                            resetFollowerAppendTimer();
                        }

                        if (logRepository.getVotedFor() != -1) {
//                        if (votedFor != -1 && votedFor != currentNodeId) {
                            log.debug("Reject vote for {} - already voted for {}", fromNodeId, logRepository.getVotedFor());
                            return new CmdRaftVoteResponse(logRepository.getCurrentTerm(), false);
                        }

                        log.debug("VOTE GRANTED for {}", fromNodeId);
                        logRepository.setVotedFor(fromNodeId);

                        return new CmdRaftVoteResponse(logRepository.getCurrentTerm(), true);

                    }
                    if (req instanceof CmdRaftAppendEntries cmd) {


                        // 1. Reply false if term < currentTerm
                        if (cmd.term() < logRepository.getCurrentTerm()) {
                            log.debug("Ignoring leader with older term {} (current={}", cmd.term(), logRepository.getCurrentTerm());
                            return CmdRaftAppendEntriesResponse.createFailed(logRepository.getCurrentTerm(), -1);
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

                        if (cmd.term() > logRepository.getCurrentTerm()) {
                            log.info("Update term {}->{}", logRepository.getCurrentTerm(), cmd.term());
                            logRepository.setCurrentTerm(cmd.term());
                            switchToFollower();
                        }

                        if (currentState == RaftNodeState.FOLLOWER && logRepository.getVotedFor() != cmd.leaderId()) {
                            log.info("Changed votedFor to {}", cmd.leaderId());
                            logRepository.setVotedFor(cmd.leaderId()); // to inform client who accessing followers
                        }

                        resetFollowerAppendTimer();

                        // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
                        final long prevLogIndex = cmd.prevLogIndex();
                        final long localLastLogIndex = logRepository.getLastLogIndex();
                        if (prevLogIndex > 0 && prevLogIndex > localLastLogIndex) {
                            log.warn("Reject - log does not contain an entry at prevLogIndex={} (last is {}))", prevLogIndex, localLastLogIndex);
                            return CmdRaftAppendEntriesResponse.createFailed(logRepository.getCurrentTerm(), localLastLogIndex);
                        }

                        final int termOfIndex = logRepository.findTermOfIndex(prevLogIndex);
                        if (cmd.prevLogTerm() != termOfIndex) {
                            log.warn("Reject - entry {} has term {}, leader provided prevLogTerm={}", prevLogIndex, termOfIndex, cmd.prevLogTerm());
                            return CmdRaftAppendEntriesResponse.createFailed(logRepository.getCurrentTerm(), localLastLogIndex);
                        }

                        if (prevLogIndex < lastApplied) {
                            log.error("Already applied index {} to RSM, can not accept prevLogIndex={}", lastApplied, prevLogIndex);
                            System.exit(-1); // TODO fix
                        }

                        final List<RaftLogEntry<T>> entries = cmd.entries();

                        if (entries.isEmpty()) {
                            return CmdRaftAppendEntriesResponse.createSuccess(logRepository.getCurrentTerm());
                        }

                        log.debug("Adding new records into the log...");

                        // 3. If an existing entry conflicts with a new one (same index but different terms),
                        // delete the existing entry and all that follow it
                        // 4. Append any new entries not already in the log
                        logRepository.appendOrOverride(entries, prevLogIndex);

                        log.debug("Added to log repository: {}", entries);

                        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                        if (cmd.leaderCommit() > commitIndex) {
                            commitIndex = Math.min(cmd.leaderCommit(), localLastLogIndex);
                            log.debug("set commitIndex to {}", commitIndex);
                        }

                        return CmdRaftAppendEntriesResponse.createSuccess(logRepository.getCurrentTerm());
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

                synchronized (rsm) {

                    final int currentTerm = logRepository.getCurrentTerm();

                    if (resp instanceof final CmdRaftVoteResponse voteResponse) {
                        if (currentState == RaftNodeState.CANDIDATE && voteResponse.voteGranted() && voteResponse.term() == currentTerm) {
                            switchToLeader();
                        }
                    } else if (resp instanceof final CmdRaftAppendEntriesResponse appendResponse) {

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
                                    log.debug("requesting lastEntryInTerm(commitIndex={}, matchIndex[{}]={}, currentTerm={});", commitIndex, fromNodeId, matchIndex[fromNodeId], currentTerm);
                                    final long lastEntryInTerm = logRepository.findLastEntryInTerm(commitIndex, matchIndex[fromNodeId], currentTerm);

                                    final long newCommitIndex = Math.max(
                                            Math.max(commitIndex, matchIndex[fromNodeId]),
                                            lastEntryInTerm);

                                    if (commitIndex != newCommitIndex) {
                                        log.debug("updated commitIndex: {}->{}", commitIndex, newCommitIndex);
                                    }

                                    commitIndex = newCommitIndex;

                                }

                            } else {

                                // failed to apply by remote node in that case it will rewind one by one message,
                                // until term and index will be matching with leader (meaning all previous entries are also matching

                                if (nextIndex[fromNodeId] > 1) {

                                    if (appendResponse.lastKnownIndex() == -1) {
                                        log.debug("decrementing nextIndex[{}] to {}", fromNodeId, nextIndex[fromNodeId] - 1);
                                        nextIndex[fromNodeId]--;
                                    } else {

                                        final long newNextIndex = Math.min(nextIndex[fromNodeId] - 1, appendResponse.lastKnownIndex() + 1);

                                        log.debug("setting nextIndex[{}] {}->{} min({},{})", fromNodeId, nextIndex[fromNodeId], newNextIndex, nextIndex[fromNodeId] - 1, appendResponse.lastKnownIndex() + 1);
                                        nextIndex[fromNodeId] = newNextIndex;
                                    }

                                } else {
                                    log.warn("Can not decrement nextIndex[{}]", fromNodeId);
                                }
                            }

                            // notify worker thread to send updates to followers
                            rsm.notifyAll();

                        }else{
                            log.warn("Unexpected correlationId={} (should be correlationId[{}]={})", correlationId, fromNodeId, correlationIds[fromNodeId]);
                        }
                    }
                }
            }


            @Override
            public CustomResponse<S> handleClientCommand(final InetAddress address,
                                                         final int port,
                                                         final long correlationId,
                                                         final long timeReceived,
                                                         final CustomCommand<T> command) {
                synchronized (rsm) {

                    if (currentState == RaftNodeState.LEADER) {
                        // If command received from client: append entry to local log,
                        // respond after entry applied to state machine (5.3)

                        // adding new record into the local log
                        log.debug("adding new record into the local log...");
                        final RaftLogEntry<T> logEntry = new RaftLogEntry<>(logRepository.getCurrentTerm(), command.rsmCommand(), timeReceived);
                        final long index = logRepository.appendEntry(logEntry, true);

                        // remember client request (TODO !! on batch migration - should refer to the last record)
                        final ClientAddress clientAddress = new ClientAddress(address, port, correlationId);
                        log.debug("Saving {} for index {} to reply", index, clientAddress);
                        clientResponsesMap.put(index, clientAddress);

                        // notify worker thread to send updates to followers
                        rsm.notifyAll();

                        // response will be returned later
                        return null;

                    } else {
                        log.debug("Redirecting client to leader nodeId={}", logRepository.getVotedFor());
                        // inform client about different leader
                        return new CustomResponse<>(respFactory.emptyResponse(), logRepository.getVotedFor(), false);
                    }

                }
            }

            @Override
            public CustomResponse<S> handleClientQuery(InetAddress address, int port, long correlationId, CustomQuery<Q> query) {

                synchronized (rsm) {

                    if (query.leaderOnly() && currentState != RaftNodeState.LEADER) {
                        // can not execute - required to be a leader
                        return new CustomResponse<>(respFactory.emptyResponse(), logRepository.getVotedFor(), false);
                    } else {
                        // just apply query instantly
                        final Q query1 = query.rsmQuery();
                        log.debug("query1: {}", query1);
                        final S result = rsm.applyQuery(query1);
                        log.debug("result: {}", result);
                        return new CustomResponse<>(result, logRepository.getVotedFor(), true);
                    }
                }
            }

            @Override
            public NodeStatusResponse handleNodeStatusRequest(final InetAddress address,
                                                              final int port,
                                                              final long correlationId,
                                                              final NodeStatusRequest request) {
                synchronized (rsm) {

                    log.debug("NodeStatusRequest received correlationId={}", correlationId);

                    final int committedLogHash = logRepository.calculateLogHash(lastApplied);

                    final boolean isLeader = currentState == RaftNodeState.LEADER;
                    final int leaderNodeId = isLeader ? thisNodeId : logRepository.getVotedFor();


                    final NodeStatusResponse response = new NodeStatusResponse(committedLogHash,
                            logRepository.getLastLogIndex(),
                            lastApplied,
                            leaderNodeId,
                            isLeader);

                    log.debug("Responding: {}", response);

                    return response;
                }
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

                synchronized (rsm) {

                    // wait for notification from UDP messages handler
                    rsm.wait(100);

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

                        // TODO why timer is not triggered when leader alone??

                        final long prevLogIndex = logRepository.getLastLogIndex();
                        Arrays.stream(otherNodes)
                                .forEach(targetNodeId ->
                                        notifyFollowerAsLeader(prevLogIndex, targetNodeId));
                    }

                    applyPendingEntriesToStateMachine();

                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            log.debug("Worker thread finished");
        }


    }

    private void notifyFollowerAsLeader(final long prevLogIndex, final int targetNodeId) {

        // If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
        // If successful: update nextIndex and matchIndex for follower (5.3)
        // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (5.3)

        final long nextIndexForNode = nextIndex[targetNodeId];

        final long t = System.nanoTime();
        final boolean shouldSendHeartbeat = t > lastHeartBeatSentNs[targetNodeId] + HEARTBEAT_LEADER_RATE_MS * 1_000_000L;

        //log.debug("shouldSendHeartbeat={}",shouldSendHeartbeat);

        // have new records for the follower and did not send batch recently
        final boolean canRetry = prevLogIndex >= nextIndexForNode
                && (correlationIds[targetNodeId] == 0L || t > timeSent[targetNodeId] + APPEND_REPLY_TIMEOUT_MAX_MS * 1_000_000L);

        //log.debug("canRetry={}",canRetry);

        if (canRetry || shouldSendHeartbeat) {

            log.debug("---------- NOTIFY NODE={} canRetry={} shouldSendHeartbeat={} ----------", targetNodeId, canRetry, shouldSendHeartbeat);

            final List<RaftLogEntry<T>> newEntries = logRepository.getEntries(nextIndexForNode, TRANSFER_ITEMS_NUM_LIMIT);
            final int prevLogTerm = logRepository.findTermOfIndex(nextIndexForNode - 1);

            newEntries.forEach(entry -> log.debug("  new entry to send to {}: {}", targetNodeId, entry));

            log.debug("node {} : nextIndexForNode={} prevLogTerm={}", targetNodeId, nextIndexForNode, prevLogTerm);

            final CmdRaftAppendEntries<T> appendRequest = new CmdRaftAppendEntries<>(
                    logRepository.getCurrentTerm(),
                    currentNodeId,
                    nextIndexForNode - 1,
                    prevLogTerm,
                    newEntries,
                    commitIndex);

            final long corrId = rpcService.callRpcAsync(appendRequest, targetNodeId);
            log.info("Sent {} entries to {}: {} corrId={}", newEntries.size(), targetNodeId, appendRequest, corrId);

            correlationIds[targetNodeId] = corrId;
            timeSent[targetNodeId] = System.nanoTime();
            sentUpTo[targetNodeId] = nextIndexForNode - 1 + newEntries.size();
            log.debug("correlationIds[{}]={}", targetNodeId, corrId);
            log.debug("set sentUpTo[{}]={}", targetNodeId, sentUpTo[targetNodeId]);

            lastHeartBeatSentNs[targetNodeId] = System.nanoTime();
        }
    }


    private void switchToFollower() {

        if (currentState != RaftNodeState.FOLLOWER) {
            log.debug("Switching to follower (reset votedFor, start append timer)");
            currentState = RaftNodeState.FOLLOWER;
            logRepository.setVotedFor(-1);
            resetFollowerAppendTimer();
        }
    }


    private void resetFollowerAppendTimer() {
//        logger.debug("reset append timer");
        synchronized (rsm) {

            lastHeartBeatReceivedNs = System.nanoTime();

        }
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
    private void appendTimeout() {
        synchronized (rsm) {
            // TODO double-check last receiving time

            currentState = RaftNodeState.CANDIDATE;

            // On conversion to candidate, start election:
            // - Increment currentTerm
            // - Vote for self
            // - Reset election timer
            // - Send RequestVote RPCs to all other servers
            final int currentTerm = logRepository.getCurrentTerm() + 1;
            logRepository.setCurrentTerm(currentTerm);

            log.info("heartbeat timeout - switching to CANDIDATE, term={}", currentTerm);

            logRepository.setVotedFor(currentNodeId);

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
    }

    private void switchToLeader() {
        log.info("============= Becoming a LEADER! ===============");
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

        final long numEntriesToRequest = commitIndex - lastApplied;

        if (numEntriesToRequest < 1) {
            return;
        }

        log.debug("Requesting {} ({}..{}) entries to apply to state machine...", numEntriesToRequest, lastApplied + 1, lastApplied + numEntriesToRequest);
        final List<RaftLogEntry<T>> entriesToApply = logRepository.getEntries(lastApplied + 1, (int) numEntriesToRequest);
        int idx = 0;

        if (entriesToApply.size() != numEntriesToRequest) {
            throw new IllegalStateException("Unexpected number of entries: " + entriesToApply.size());
        }

        log.debug("entries to apply: {}", entriesToApply);

        while (lastApplied < commitIndex) {

            lastApplied++;
//            final RaftLogEntry<T> raftLogEntry = logRepository.getEntryOpt(lastApplied)
//                    .orElseThrow(() -> new RuntimeException("Can not find pending entry index=" + lastApplied + " in the repository"));

            final RaftLogEntry<T> raftLogEntry = entriesToApply.get(idx++);

            log.debug("Applying to RSM idx={} {}", lastApplied, raftLogEntry);
            final S result = rsm.applyCommand(raftLogEntry.cmd());

            if (currentState == RaftNodeState.LEADER) {

                // respond to client that batch has applied
                final ClientAddress clientAddress = clientResponsesMap.get(lastApplied);

                if (clientAddress != null) {

                    log.debug("Replying to client lastApplied={} c={}", lastApplied, clientAddress);

                    rpcService.respondToClient(
                            clientAddress.address,
                            clientAddress.port,
                            clientAddress.correlationId,
                            new CustomResponse<>(result, currentNodeId, true));

                    log.debug("Response sent to the client");
                } else {
                    log.warn("Can not reply client - unknown requestor for lastApplied={}", lastApplied);
                }
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