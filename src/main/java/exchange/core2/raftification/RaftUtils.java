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
import java.util.HashMap;
import java.util.Map;

public class RaftUtils {


    public static <T extends RsmRequest, S extends RsmResponse> RpcMessage createMessageByType(
            int messageType,
            ByteBuffer buffer,
            RsmRequestFactory<T> factory,
            RsmResponseFactory<S> responseFactory) {

        return switch (messageType) {
            case RpcMessage.REQUEST_APPEND_ENTRIES -> CmdRaftAppendEntries.create(buffer, factory);
            case RpcMessage.RESPONSE_APPEND_ENTRIES -> CmdRaftAppendEntriesResponse.create(buffer);
            case RpcMessage.REQUEST_VOTE -> CmdRaftVoteRequest.create(buffer);
            case RpcMessage.RESPONSE_VOTE -> CmdRaftVoteResponse.create(buffer);
            case RpcMessage.REQUEST_CUSTOM -> CustomCommandRequest.create(buffer, factory);
            case RpcMessage.RESPONSE_CUSTOM -> CustomCommandResponse.create(buffer, responseFactory);
            default -> throw new IllegalArgumentException("Unknown messageType: " + messageType);
        };
    }

    public static Map<Integer, RemoteUdpSocket> createHostMap(Map<Integer, String> remoteNodes) {

        final Map<Integer, RemoteUdpSocket> socketMap = new HashMap<>();

        remoteNodes.forEach((id, address) -> {

            try {
                final String[] split = address.split(":");

                final InetAddress host = InetAddress.getByName(split[0]);
                final int port = Integer.parseInt(split[1]);

                RemoteUdpSocket remoteUdpSocket = new RemoteUdpSocket(host, port);

                socketMap.put(id, remoteUdpSocket);

            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        return socketMap;
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
