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

public interface RpcHandler<T extends RsmRequest, S extends RsmResponse> {

    RpcResponse handleNodeRequest(int nodeId, RpcRequest request);

    void handleNodeResponse(int nodeId, RpcResponse response, long correlationId);

    CustomCommandResponse<S> handleClientRequest(InetAddress address,
                                                 int port,
                                                 long correlationId,
                                                 long timeReceived,
                                                 CustomCommandRequest<T> request);

}
