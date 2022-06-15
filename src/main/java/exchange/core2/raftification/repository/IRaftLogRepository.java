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

package exchange.core2.raftification.repository;

import exchange.core2.raftification.messages.RaftLogEntry;
import exchange.core2.raftification.messages.RsmCommand;

import java.util.List;

/**
 * RAFT log persistent storage repository
 *
 * @param <T> - request records type for particular Replicated State Machine
 */
public interface IRaftLogRepository<T extends RsmCommand> extends AutoCloseable {

    /**
     * @return last entry index (starting from 1), or 0 if there are no entries.
     */
    long getLastLogIndex();

    /**
     * @return term of last entry, or 0 if there are no entries.
     */
    int getLastLogTerm();

    /**
     * Get entry from log
     *
     * @param indexFrom - RAFT record index (starting from 1)
     * @param limit     - max number of entries to retrieve
     * @return records (if found)
     */
    List<RaftLogEntry<T>> getEntries(long indexFrom, int limit);


    /**
     * Find last known entry index in certain term inside specific interval of indexes
     *
     * @param indexAfter      - entry should be last
     * @param indexBeforeIncl - entry should not exceed this index
     * @param term            - term
     * @return entry index if such entry exists, or indexAfter otherwise (even if term is unknown)
     */
    long findLastEntryInTerm(long indexAfter, long indexBeforeIncl, int term);

    /**
     * Find term of the specific entry by index
     *
     * @param index - index of the entry
     * @return term or 0 for unknown indexes
     */
    int findTermOfIndex(long index);

    /**
     * Append single entry
     *
     * @param logEntry   - RAFT Replicated State Machine entry
     * @param endOfBatch - force writing to disk
     * @return index of added entry
     */
    long appendEntry(RaftLogEntry<T> logEntry, boolean endOfBatch);


    /**
     * Append multiple entries from leader to follower log.
     * Override entries that are different.
     *
     * @param newEntries   list of new entries
     * @param prevLogIndex index of entry after which provided newEntries should be attached
     */
    void appendOrOverride(List<RaftLogEntry<T>> newEntries, long prevLogIndex);


    int calculateLogHash(long lastApplied);

    // latest term server has seen (initialized to 0 on first boot, increases monotonically)

    int getCurrentTerm();

    void setCurrentTerm(int term);


    // candidateId that received vote in current term (or -1 if none)

    int getVotedFor();

    void setVotedFor(int nodeId);

}
