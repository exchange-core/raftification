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

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 */
public record CmdRaftVoteRequest(int term, int candidateId, long lastLogIndex, int lastLogTerm) implements RpcRequest {

    @Override
    public int getMessageType() {
        return 2;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putInt(candidateId);
        buffer.putLong(lastLogIndex);
        buffer.putInt(lastLogTerm);
    }


    public static CmdRaftVoteRequest create(ByteBuffer buffer) {

        final int term = buffer.getInt();
        final int leaderId = buffer.getInt();
        final long prevLogIndex = buffer.getLong();
        final int prevLogTerm = buffer.getInt();

        return new CmdRaftVoteRequest(term, leaderId, prevLogIndex, prevLogTerm);
    }

    @Override
    public String toString() {
        return "CmdRaftVoteRequest{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
