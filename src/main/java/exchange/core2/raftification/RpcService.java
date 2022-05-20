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
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RpcService<T extends RsmRequest, S extends RsmResponse> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RpcService.class);

    private final AtomicLong correlationIdCounter = new AtomicLong(1L);
    private final Map<Long, CompletableFuture<RpcResponse>> futureMap = new ConcurrentHashMap<>();
    private final Map<Integer, RaftUtils.RemoteUdpSocket> socketMap;
    private final int serverPort;
    private final int serverNodeId;
    private final RpcHandler<T, S> handler;
    private final RsmRequestFactory<T> msgFactory;
    private final RsmResponseFactory<S> responseFactory;

    private final DatagramSocket serverSocket;

    private volatile boolean active = true;

    public RpcService(Map<Integer, String> remoteNodes,
                      RpcHandler<T, S> handler,
                      RsmRequestFactory<T> msgFactory,
                      RsmResponseFactory<S> responseFactory,
                      int serverNodeId) {

        this.socketMap = RaftUtils.createHostMap(remoteNodes);
        this.handler = handler;
        this.serverPort = socketMap.get(serverNodeId).port;
        this.serverNodeId = serverNodeId;
        this.msgFactory = msgFactory;
        this.responseFactory = responseFactory;

        try {
            this.serverSocket = new DatagramSocket(serverPort);
        } catch (final SocketException ex) {
            throw new RuntimeException(ex);
        }

        Thread t = new Thread(this::run);
        t.setDaemon(true);
        t.setName("ListenerUDP");
        t.start();


    }


    public void run() {

        try {
            logger.info("Listening at UDP {}:{}", InetAddress.getLocalHost().getHostAddress(), serverPort);
        } catch (UnknownHostException ex) {
            logger.warn("UnknownHostException: ", ex);
        }

        final byte[] receiveData = new byte[256]; // TODO set proper value

        final DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

        while (active) {

            try {
                serverSocket.receive(receivePacket);

                final ByteBuffer bb = ByteBuffer.wrap(receivePacket.getData(), 0, receivePacket.getLength());

                final int nodeId = bb.getInt();
                final int messageType = bb.getInt();
                final long correlationId = bb.getLong();

                logger.debug("RECEIVED from {} mt={}: {}", nodeId, messageType, PrintBufferUtil.hexDump(receivePacket.getData(), 0, receivePacket.getLength()));

                final RpcMessage msg = RaftUtils.createMessageByType(messageType, bb, msgFactory, responseFactory);
                // TODO use msgFactory

                if (messageType < 0) {
                    // handler response

                    final CompletableFuture<RpcResponse> future = futureMap.remove(correlationId);
                    if (future != null) {
                        // complete future for future-based-calls
                        future.complete((RpcResponse) msg);
                    } else {
                        // handle response for full-async-calls
                        handler.handleNodeResponse(nodeId, (RpcResponse) msg, correlationId);
                    }

                } else {

                    if (msg instanceof CustomCommandRequest) {
                        // request from client

                        final InetAddress address = receivePacket.getAddress();
                        final int port = receivePacket.getPort();

                        final CustomCommandRequest<T> msgT = (CustomCommandRequest<T>) msg;

                        final long timeReceived = System.currentTimeMillis();

                        final CustomCommandResponse<S> response = handler.handleClientRequest(
                                address,
                                port,
                                correlationId,
                                timeReceived,
                                msgT);

                        if (response != null) {
                            respondToClient(address, port, correlationId, response);
                        }

                    } else {
                        // handle request
                        final RpcResponse response = handler.handleNodeRequest(nodeId, (RpcRequest) msg);
                        // send response
                        if (response != null) {
                            sendResponse(nodeId, correlationId, response);
                        }
                    }


                }

            } catch (final Exception ex) {
                String message = PrintBufferUtil.hexDump(receivePacket.getData(), 0, receivePacket.getLength());
                logger.error("Failed to process message from {}: {}", receivePacket.getAddress().getHostAddress(), message, ex);
            }
        }

        logger.info("UDP server shutdown");
        serverSocket.close();
    }


    private void sendResponse(int callerNodeId, long correlationId, RpcResponse response) {
        final byte[] array = new byte[64];
        ByteBuffer bb = ByteBuffer.wrap(array);

        bb.putInt(serverNodeId);
        bb.putInt(response.getMessageType());
        bb.putLong(correlationId);
        response.serialize(bb);

        send(callerNodeId, array, bb.position());
    }


    public long callRpcAsync(RpcRequest request, int toNodeId) {

        final long correlationId = correlationIdCounter.incrementAndGet();
        callRpc(request, toNodeId, correlationId);
        return correlationId;
    }

    public CompletableFuture<RpcResponse> callRpcSync(RpcRequest request, int toNodeId) {

        final long correlationId = correlationIdCounter.incrementAndGet();

        final CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        futureMap.put(correlationId, future);

        callRpc(request, toNodeId, correlationId);

        return future;
    }

    private void callRpc(RpcRequest request, int toNodeId, long correlationId) {

        final byte[] array = new byte[256];
        ByteBuffer bb = ByteBuffer.wrap(array);

        bb.putInt(serverNodeId);
        bb.putInt(request.getMessageType());
        bb.putLong(correlationId);

        request.serialize(bb);

        send(toNodeId, array, bb.position());
    }


    private void send(int nodeId, byte[] data, int length) {

        final RaftUtils.RemoteUdpSocket remoteUdpSocket = socketMap.get(nodeId);
        final DatagramPacket packet = new DatagramPacket(data, length, remoteUdpSocket.address, remoteUdpSocket.port);

        try {
            serverSocket.send(packet);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void respondToClient(InetAddress address, int port, long correlationId, RpcResponse response) {

        final byte[] array = new byte[64];
        final ByteBuffer bb = ByteBuffer.wrap(array);

        // put only correlationId into the header
        bb.putLong(correlationId);
        response.serialize(bb);

        respondToClient(address, port, array, bb.position());
    }

    private void respondToClient(InetAddress address, int port, byte[] data, int length) {

        final DatagramPacket packet = new DatagramPacket(data, length, address, port);

        try {

            serverSocket.send(packet);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    @Override
    public void close() {

        active = false;
    }


}
