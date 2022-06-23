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
import org.agrona.PrintBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RpcClient<C extends RsmCommand, Q extends RsmQuery, S extends RsmResponse> {

    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);

    private final AtomicLong correlationIdCounter = new AtomicLong(1L);
    private final Map<Long, CompletableFuture<RpcResponse>> futureMap = new ConcurrentHashMap<>();
    private final List<RaftUtils.RemoteUdpSocket> socketMap;
    private final RsmResponseFactory<S> msgFactory;


    private final List<RpcNodeReadinessRecord> nodesReadiness;


    private final DatagramSocket serverSocket;

    private volatile boolean active = true; // TODO implement


    public RpcClient(final List<String> remoteNodes,
                     final RsmResponseFactory<S> msgFactory) {

        this.socketMap = RaftUtils.createHostMap(remoteNodes);
        this.msgFactory = msgFactory;

        // assume 0 is a leader initially and every node is ready
        this.nodesReadiness = IntStream.range(0, 3)
                .mapToObj(nodeId -> new RpcNodeReadinessRecord(nodeId, 0L, true, nodeId == 0))
                .collect(Collectors.toCollection(CopyOnWriteArrayList::new));

        try {
            this.serverSocket = new DatagramSocket();
        } catch (final SocketException ex) {
            throw new RuntimeException(ex);
        }

        Thread t = new Thread(this::run);
        t.setDaemon(true);
        t.setName("ListenerUDP");
        t.start();

    }

    public void run() {

        final byte[] receiveData = new byte[256]; // TODO set proper value

        final DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

        while (active) {

            try {
                serverSocket.receive(receivePacket);

                final ByteBuffer bb = ByteBuffer.wrap(receivePacket.getData(), 0, receivePacket.getLength());

                final int msgType = bb.getInt();

                final long correlationId = bb.getLong();

                logger.debug("RECEIVED from {} (c={}): {}", receivePacket.getAddress(), correlationId, PrintBufferUtil.hexDump(receivePacket.getData(), 0, receivePacket.getLength()));

                final CompletableFuture<RpcResponse> future = futureMap.remove(correlationId);
                if (future != null) {
                    // complete future for future-based-calls

                    final RpcResponse msg = (msgType == RpcMessage.RESPONSE_NODE_STATUS)
                            ? NodeStatusResponse.create(bb)
                            : CustomResponse.create(bb, msgFactory);
                    future.complete(msg);

                } else {
                    logger.warn("Unexpected response with correlationId={}", correlationId);
                }

            } catch (final Exception ex) {
                String message = PrintBufferUtil.hexDump(receivePacket.getData(), 0, receivePacket.getLength());
                logger.error("Failed to process message from {}: {}", receivePacket.getAddress().getHostAddress(), message, ex);
            }
        }

        logger.info("UDP server shutdown");
        serverSocket.close();
    }

    public S callRpcSync(final C command, final int timeoutMs) throws TimeoutException {

        // Find initial leader. There is no strong consistency - use 0 by default (very small chance).

        RpcNodeReadinessRecord leader = nodesReadiness.stream()
                .filter(node -> node.isLeader)
                .findFirst()
                .orElse(nodesReadiness.get(0));

        for (int i = 0; i < 3; i++) {

            final long correlationId = correlationIdCounter.incrementAndGet();
            final CompletableFuture<RpcResponse> future = new CompletableFuture<>();
            futureMap.put(correlationId, future);

            final CustomCommand<C> request = new CustomCommand<>(command);

            // send request to last known leader
            callRequest(request, leader.nodeId, correlationId);

            try {

                // block waiting for response
                final CustomResponse<S> response = (CustomResponse<S>) future.get(timeoutMs, TimeUnit.MILLISECONDS);

                // can be redirected
                final int suggestedLeaderId = response.leaderNodeId();

                if (suggestedLeaderId != leader.nodeId) {
                    logger.info("Redirected to new leader {}->{}", leader.nodeId, suggestedLeaderId);

                    final RpcNodeReadinessRecord suggestedLeader = nodesReadiness.get(suggestedLeaderId).asSuggestedLeader();
                    nodesReadiness.set(suggestedLeaderId, suggestedLeader);
                    leader = suggestedLeader;
                }

                if (response.success()) {
                    return response.rsmResponse();
                }

            } catch (TimeoutException ex) {

                logger.info("Timeout from " + leader.nodeId);

                nodesReadiness.set(leader.nodeId, leader.asFailedFollower(System.nanoTime()));

                final int index = leader.nodeId != 2 ? leader.nodeId + 1 : 0;
                logger.info("index={} list={}", index, nodesReadiness);
                leader = nodesReadiness.get(index);

                logger.info("Switched to {}", leader.nodeId);

                // extra rotation if next leader is recently failed
                if (!leader.isAlive) {
                    long msSinceLastCheck = (System.nanoTime() - leader.lastTimeCheckedNs) / 1_000_000;
                    if (msSinceLastCheck < 100) {
                        leader = nodesReadiness.get(leader.nodeId != 2 ? leader.nodeId + 1 : 0);
                        logger.info("Extra switched to {} (inactivity {}ms < 100ms)", leader.nodeId, msSinceLastCheck);
                    } else {
                        logger.info("Will try inactive node {}, as {}ms >= 100ms", leader.nodeId, msSinceLastCheck);
                    }
                }

            } catch (Exception ex) {

                logger.info("Request failed ({})", ex.getMessage());
                throw new RuntimeException(ex);
            } finally {
                // double-check if correlationId removed
                futureMap.remove(correlationId);
            }
        }

        throw new TimeoutException();
    }

    public CompletableFuture<? extends RpcResponse> callRpcAsync(final int nodeId,
                                                                 final RpcRequest request) {

        final long correlationId = correlationIdCounter.incrementAndGet();
        final CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        futureMap.put(correlationId, future);

        // send request to last known leader
        callRequest(request, nodeId, correlationId);

        return future;
    }


    private void callRequest(RpcRequest request, int toNodeId, long correlationId) {

        final byte[] array = new byte[64];
        ByteBuffer bb = ByteBuffer.wrap(array);

        bb.putInt(-1);
        bb.putInt(request.getMessageType());
        bb.putLong(correlationId);

        request.serialize(bb);

        sendData(toNodeId, array, bb.position());
    }

    private void sendData(int nodeId, byte[] data, int length) {

        final RaftUtils.RemoteUdpSocket remoteUdpSocket = socketMap.get(nodeId);

        final DatagramPacket packet = new DatagramPacket(data, length, remoteUdpSocket.address, remoteUdpSocket.port);

        logger.debug("SENDING to {} : {}", nodeId, PrintBufferUtil.hexDump(data, 0, length));

        try {
            serverSocket.send(packet);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    private static record RpcNodeReadinessRecord(int nodeId,
                                                 long lastTimeCheckedNs,
                                                 boolean isAlive,
                                                 boolean isLeader) {

        RpcNodeReadinessRecord asSuggestedLeader() {
            return new RpcNodeReadinessRecord(nodeId, lastTimeCheckedNs, isAlive, true);
        }

        RpcNodeReadinessRecord asFailedFollower(long timeNs) {
            return new RpcNodeReadinessRecord(nodeId, timeNs, false, false);
        }


    }

}
