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
 * Invoked by candidates to gather votes (5.2).
 */
public record CmdRaftVoteResponse(int term, boolean voteGranted) implements RpcResponse {

    @Override
    public int getMessageType() {
        return RESPONSE_VOTE;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.put(voteGranted ? (byte) 1 : (byte) 0);
    }

    public static CmdRaftVoteResponse create(ByteBuffer buffer) {

        final int term = buffer.getInt();
        final boolean voteGranted = buffer.get() == 1;

        return new CmdRaftVoteResponse(term, voteGranted);
    }

    @Override
    public String toString() {
        return "CmdRaftVoteResponse{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                '}';
    }
}
