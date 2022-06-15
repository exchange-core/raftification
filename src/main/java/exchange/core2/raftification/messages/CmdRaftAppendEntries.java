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

import exchange.core2.raftification.RsmRequestFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 */
public record CmdRaftAppendEntries<T extends RsmCommand>(int term,
                                                         int leaderId,
                                                         long prevLogIndex,
                                                         int prevLogTerm,
                                                         List<RaftLogEntry<T>> entries,
                                                         long leaderCommit) implements RpcRequest {

    @Override
    public int getMessageType() {
        return 1;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putInt(leaderId);
        buffer.putLong(prevLogIndex);
        buffer.putInt(prevLogTerm);
        buffer.putInt(entries.size());
        entries.forEach(entry -> entry.serialize(buffer));
        buffer.putLong(leaderCommit);
    }

    public static <T extends RsmCommand> CmdRaftAppendEntries<T> create(
            ByteBuffer buffer,
            RsmRequestFactory<T, ?> factory) {

        final int term = buffer.getInt();
        final int leaderId = buffer.getInt();
        final long prevLogIndex = buffer.getLong();
        final int prevLogTerm = buffer.getInt();
        final int numEntries = buffer.getInt();

        final List<RaftLogEntry<T>> entries = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {

            entries.add(RaftLogEntry.create(buffer, factory));
        }

        final long leaderCommit = buffer.getLong();

        return new CmdRaftAppendEntries<>(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    @Override
    public String toString() {
        return "CmdRaftAppendEntries{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries=" + entries +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}
