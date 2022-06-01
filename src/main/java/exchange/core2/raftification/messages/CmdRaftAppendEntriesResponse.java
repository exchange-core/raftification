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
 * <p>
 * TODO If desired, the protocol can be optimized to reduce the number of rejected AppendEntries RPCs. For example,
 * when rejecting an AppendEntries request, the follower can include the term of the conflicting entry and the first
 * index it stores for that term. With this information, the leader can decrement nextIndex to bypass all of the
 * conflicting entries in that term; one AppendEntries RPC will be required for each term with conflicting entries, rather
 * than one RPC per entry. In practice, we doubt this optimization is necessary, since failures happen infrequently
 * and it is unlikely that there will be many inconsistent entries.
 */
public record CmdRaftAppendEntriesResponse(int term, long lastKnownIndex, boolean success) implements RpcResponse {

    @Override
    public int getMessageType() {
        return RESPONSE_APPEND_ENTRIES;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(lastKnownIndex);
        buffer.putInt(term);
        buffer.put(success ? (byte) 1 : (byte) 0);
    }

    public static CmdRaftAppendEntriesResponse createFailed(int term, long lastKnownIndex) {
        return new CmdRaftAppendEntriesResponse(term, lastKnownIndex, false);
    }

    public static CmdRaftAppendEntriesResponse createSuccess(int term) {
        return new CmdRaftAppendEntriesResponse(term, -1, true);
    }

    public static CmdRaftAppendEntriesResponse create(ByteBuffer bb) {
        final long lastKnownIndex = bb.getLong();
        final int term = bb.getInt();
        final boolean success = bb.get() == 1;
        return new CmdRaftAppendEntriesResponse(term, lastKnownIndex, success);
    }

    @Override
    public String toString() {
        return "CmdRaftAppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                ", lastKnownIndex=" + lastKnownIndex +
                '}';
    }
}
