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
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RpcClient<T extends RsmCommand, S extends RsmResponse> {

    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);

    private final AtomicLong correlationIdCounter = new AtomicLong(1L);
    private final Map<Long, CompletableFuture<RpcResponse>> futureMap = new ConcurrentHashMap<>();
    private final Map<Integer, RaftUtils.RemoteUdpSocket> socketMap;
    private final RsmResponseFactory<S> msgFactory;

    private volatile int leaderNodeId = 0;

    private final DatagramSocket serverSocket;

    private volatile boolean active = true; // TODO implement


    public RpcClient(final Map<Integer, String> remoteNodes,
                     final RsmResponseFactory<S> msgFactory) {

        this.socketMap = RaftUtils.createHostMap(remoteNodes);
        this.msgFactory = msgFactory;

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

    public S callRpcSync(final T data, final int timeoutMs) throws TimeoutException {

        final int leaderNodeIdInitial = leaderNodeId;
        int leaderNodeIdLocal = leaderNodeIdInitial;

        final Queue<Integer> remainingServers = socketMap.keySet().stream()
                .filter(id -> id != leaderNodeIdInitial)
                .collect(Collectors.toCollection(LinkedList::new));

        for (int i = 0; i < 5; i++) {

            final long correlationId = correlationIdCounter.incrementAndGet();
            final CompletableFuture<RpcResponse> future = new CompletableFuture<>();
            futureMap.put(correlationId, future);

            final CustomCommand<T> request = new CustomCommand<>(data);

            // send request to last known leader
            callRequest(request, leaderNodeIdLocal, correlationId);

            try {

                // block waiting for response
                final CustomResponse<S> response = (CustomResponse<S>) future.get(timeoutMs, TimeUnit.MILLISECONDS);

                if (response.success()) {

                    // update only if changed (volatile write)
                    if (leaderNodeIdInitial != leaderNodeIdLocal) {
                        leaderNodeId = leaderNodeIdLocal;
                    }

                    return response.rsmResponse();

                } else {

                    // can be redirected
                    if (response.leaderNodeId() != leaderNodeIdLocal) {
                        logger.info("Redirected to new leader {}->{}", leaderNodeIdLocal, response.leaderNodeId());
                        leaderNodeIdLocal = response.leaderNodeId();
                    }
                }

            } catch (TimeoutException ex) {

                logger.info("Timeout from " + leaderNodeIdLocal);

                final Integer nextNode = remainingServers.poll();
                if (nextNode != null) {
                    leaderNodeIdLocal = nextNode;
                } else {
                    throw ex;
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

        DatagramPacket packet = new DatagramPacket(data, length, remoteUdpSocket.address, remoteUdpSocket.port);

        try {
            serverSocket.send(packet);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


}
