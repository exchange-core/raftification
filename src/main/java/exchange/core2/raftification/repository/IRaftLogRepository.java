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
import exchange.core2.raftification.messages.RsmRequest;

import java.util.List;

/**
 * RAFT log persistent storage repository
 *
 * @param <T> - request records type for particular Replicated State Machine
 */
public interface IRaftLogRepository<T extends RsmRequest> extends AutoCloseable {

    long getLastLogIndex();

    int getLastLogTerm();

    /**
     * Get entry from log
     *
     * @param indexFrom - RAFT record index (starting from 1)
     * @param limit     - max number of entries to retrieve
     * @return records (if found)
     */
    List<RaftLogEntry<T>> getEntries(long indexFrom, int limit);


    long findLastEntryInTerm(long indexAfter, long indexBeforeIncl, int term);

    /**
     * Append single entry
     *
     * @param logEntry   - RAFT Replicated State Machine entry
     * @param endOfBatch - force writing to disk
     * @return index of added entry
     */
    long appendEntry(RaftLogEntry<T> logEntry, boolean endOfBatch);


    void appendOrOverride(List<RaftLogEntry<T>> newEntries, long prevLogIndex);


}
