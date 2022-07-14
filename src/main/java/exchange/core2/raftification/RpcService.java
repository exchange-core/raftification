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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RpcService<T extends RsmCommand, Q extends RsmQuery, S extends RsmResponse> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RpcService.class);

    private final AtomicLong correlationIdCounter = new AtomicLong(1L);
    private final Map<Long, CompletableFuture<RpcResponse>> futureMap = new ConcurrentHashMap<>();
    private final List<RaftUtils.RemoteUdpSocket> socketMap;
    private final int serverPort;
    private final int serverNodeId;
    private final RpcHandler<T, Q, S> handler;
    private final RsmRequestFactory<T, Q> requestFactory;
    private final RsmResponseFactory<S> responseFactory;

    private final DatagramSocket serverSocket;

    private volatile boolean active = true;

    public RpcService(List<String> remoteNodes,
                      RpcHandler<T, Q, S> handler,
                      RsmRequestFactory<T, Q> requestFactory,
                      RsmResponseFactory<S> responseFactory,
                      int serverNodeId) {

        this.socketMap = RaftUtils.createHostMap(remoteNodes);
        this.handler = handler;
        this.serverPort = socketMap.get(serverNodeId).port;
        this.serverNodeId = serverNodeId;
        this.requestFactory = requestFactory;
        this.responseFactory = responseFactory;

        try {
            this.serverSocket = new DatagramSocket(serverPort);
        } catch (final SocketException ex) {
            throw new RuntimeException(ex);
        }

        final Thread thread = new Thread(this::run);
        thread.setDaemon(true);
        thread.setName("ListenerUDP");
        thread.start();
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

                // TODO use msgFactory

                if (messageType < 0) {
                    // handler response

                    final RpcResponse msg = RaftUtils.createResponseByType(messageType, bb, responseFactory);

                    final CompletableFuture<RpcResponse> future = futureMap.remove(correlationId);
                    if (future != null) {
                        // complete future for future-based-calls
                        future.complete(msg);
                    } else {
                        // handle response for full-async-calls
                        handler.handleNodeResponse(nodeId, msg, correlationId);
                    }

                } else {

                    final RpcRequest msg = RaftUtils.createRequestByType(messageType, bb, requestFactory);

                    logger.debug("msg: {}", msg);

                    if (msg instanceof CustomCommand msgCmd) {
                        // command from client

                        final InetAddress address = receivePacket.getAddress();
                        final int port = receivePacket.getPort();

                        // todo change to more precise timestamp (provider?)
                        final long timeReceived = System.currentTimeMillis();

                        final CustomResponse<S> response = handler.handleClientCommand(
                                address,
                                port,
                                correlationId,
                                timeReceived,
                                (CustomCommand<T>) msgCmd);

                        if (response != null) {
                            respondToClient(address, port, correlationId, response);
                        }

                    } else if (msg instanceof CustomQuery msgQ) {

                        // query from client

                        final InetAddress address = receivePacket.getAddress();
                        final int port = receivePacket.getPort();

                        logger.debug("address:{} port:{}", address, port);

                        final CustomResponse<S> response =
                                handler.handleClientQuery(
                                        address,
                                        port,
                                        correlationId,
                                        (CustomQuery<Q>) msgQ);

                        logger.debug("response q: {}", response);

                        if (response != null) {
                            respondToClient(address, port, correlationId, response);
                        }


                    } else if (msg instanceof NodeStatusRequest nodeStatusRequest) {
                        // request from client

                        final InetAddress address = receivePacket.getAddress();
                        final int port = receivePacket.getPort();

                        final NodeStatusResponse response = handler.handleNodeStatusRequest(
                                address,
                                port,
                                correlationId,
                                nodeStatusRequest);

                        if (response != null) {
                            respondToClient(address, port, correlationId, response);
                        }

                    } else {
                        // handle request
                        final RpcResponse response = handler.handleNodeRequest(nodeId, msg);
                        // send response
                        if (response != null) {
                            sendResponse(nodeId, correlationId, response);
                        }
                    }


                }

            } catch (final Exception ex) {
                String message = PrintBufferUtil.hexDump(receivePacket.getData(), 0, receivePacket.getLength());
                final String hostAddress = receivePacket.getAddress().getHostAddress();
                final int port = receivePacket.getPort();
                logger.error("Failed to process message from {}:{} {}", hostAddress, port, message, ex);
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
        bb.putInt(response.getMessageType());
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
