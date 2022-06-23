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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public final class RaftUtils {

    public static <T extends RsmCommand, Q extends RsmQuery> RpcRequest createRequestByType(
            final int messageType,
            final ByteBuffer buffer,
            final RsmRequestFactory<T, Q> requestFactory) {

        return switch (messageType) {
            case RpcMessage.REQUEST_APPEND_ENTRIES -> CmdRaftAppendEntries.create(buffer, requestFactory);
            case RpcMessage.REQUEST_VOTE -> CmdRaftVoteRequest.create(buffer);
            case RpcMessage.COMMAND_CUSTOM -> CustomCommand.create(buffer, requestFactory);
            case RpcMessage.QUERY_CUSTOM -> CustomQuery.create(buffer, requestFactory);
            case RpcMessage.REQUEST_NODE_STATUS -> NodeStatusRequest.create(buffer);
            default -> throw new IllegalArgumentException("Unknown request messageType: " + messageType);
        };
    }


    public static <S extends RsmResponse> RpcResponse createResponseByType(
            final int messageType,
            final ByteBuffer buffer,
            final RsmResponseFactory<S> responseFactory) {

        return switch (messageType) {
            case RpcMessage.RESPONSE_APPEND_ENTRIES -> CmdRaftAppendEntriesResponse.create(buffer);
            case RpcMessage.RESPONSE_VOTE -> CmdRaftVoteResponse.create(buffer);
            case RpcMessage.RESPONSE_CUSTOM -> CustomResponse.create(buffer, responseFactory);
            case RpcMessage.RESPONSE_NODE_STATUS -> NodeStatusResponse.create(buffer);
            default -> throw new IllegalArgumentException("Unknown request messageType: " + messageType);
        };
    }

    public static List<RemoteUdpSocket> createHostMap(final List<String> remoteNodes) {

        return remoteNodes.stream()
                .map(address -> {
                    try {
                        final String[] split = address.split(":");

                        final InetAddress host = InetAddress.getByName(split[0]);
                        final int port = Integer.parseInt(split[1]);

                        return new RemoteUdpSocket(host, port);

                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toList());
    }


    public static final class RemoteUdpSocket {

        public final InetAddress address;
        public final int port;

        public RemoteUdpSocket(InetAddress address, int port) {
            this.address = address;
            this.port = port;
        }
    }

}
