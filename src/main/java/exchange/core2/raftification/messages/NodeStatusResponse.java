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

package exchange.core2.raftification.messages;

import java.nio.ByteBuffer;

public record NodeStatusResponse(int committedLogHash,
                                 long lastReceivedIndex,
                                 long lastCommittedIndex,
                                 int leaderNodeId,
                                 boolean isLeader) implements RpcResponse {

    @Override
    public int getMessageType() {
        return RESPONSE_NODE_STATUS;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(committedLogHash);
        buffer.putLong(lastReceivedIndex);
        buffer.putLong(lastCommittedIndex);
        buffer.putInt(leaderNodeId);
        buffer.put((byte) (isLeader ? 1 : 0));
    }

    public static NodeStatusResponse create(ByteBuffer buffer) {

        final int committedLogHash = buffer.getInt();
        final long lastReceivedIndex = buffer.getLong();
        final long lastCommittedIndex = buffer.getLong();
        final int leaderNodeId = buffer.getInt();
        final boolean isLeader = buffer.get() == 1;

        return new NodeStatusResponse(committedLogHash, lastReceivedIndex, lastCommittedIndex, leaderNodeId, isLeader);
    }

    @Override
    public String toString() {
        return "NodeStatusResponse{" +
                "committedLogHash=" + committedLogHash +
                ", lastReceivedIndex=" + lastReceivedIndex +
                ", lastCommittedIndex=" + lastCommittedIndex +
                ", leaderNodeId=" + leaderNodeId +
                ", isLeader=" + isLeader +
                '}';
    }
}
