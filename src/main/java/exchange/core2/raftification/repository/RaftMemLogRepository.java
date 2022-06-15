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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RaftMemLogRepository<T extends RsmCommand> implements IRaftLogRepository<T> {

    private static final Logger log = LoggerFactory.getLogger(RaftMemLogRepository.class);

    private final List<RaftLogEntry<T>> logEntries = new ArrayList<>(); // TODO change to persistent storage with long-index

    @Override
    public long findLastEntryInTerm(long indexAfter, long indexBeforeIncl, int term) {

        int idx = (int) indexAfter;

        for (int i = (int) indexAfter + 1; i <= indexBeforeIncl; i++) {
            //log.debug("i={}", i);
            if (logEntries.get(i - 1).term() == term) {
                idx = i;
            }
        }
        return idx;
    }

    @Override
    public int findTermOfIndex(long index) {
        // avoid additional request
        return getEntries(index, 1).stream()
                .findFirst()
                .map(RaftLogEntry::term)
                .orElse(0);
    }

    @Override
    public long getLastLogIndex() {
        return logEntries.size(); // 0 = no records
    }

    @Override
    public int getLastLogTerm() {
        if (logEntries.isEmpty()) {
            return 0; // return term 0 by default
        } else {
            return logEntries.get(logEntries.size() - 1).term();
        }
    }

    @Override
    public long appendEntry(final RaftLogEntry<T> logEntry, final boolean endOfBatch) {
        logEntries.add(logEntry);
        return getLastLogIndex(); // starting from index=1
    }

    @Override
    public void appendOrOverride(final List<RaftLogEntry<T>> newEntries, long prevLogIndex) {

        log.debug("appendOrOverride(newEntries={} , prevLogIndex={}", newEntries, prevLogIndex);

        for (int i = 0; i < newEntries.size(); i++) {
            final RaftLogEntry<T> newEntry = newEntries.get(i);

            if ((prevLogIndex + i) < logEntries.size()) {


                final int pos = (int) prevLogIndex + i;
                final int existingTerm = logEntries.get(pos).term();

                log.debug("Validating older record with index={}: existingTerm={} newEntry.term={}", pos + 1, existingTerm, newEntry.term());

                // 3. If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it

                if (newEntry.term() != existingTerm) {
                    log.debug("Remove all records after index={}, because term is different: {} (old={})", pos + 1, newEntry.term(), existingTerm);
                    int lastIdxToRemove = logEntries.size();
                    if (lastIdxToRemove > pos + 1) {
                        logEntries.subList(pos + 1, lastIdxToRemove).clear();
                    }
                }
            } else {
                log.debug("appendOrOverride - added {}", newEntry);
                logEntries.add(newEntry); // TODO inefficient, because normally records are simply appended as batch
            }
        }
    }

    @Override
    public int calculateLogHash(long lastApplied) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public int getCurrentTerm() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCurrentTerm(int term) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getVotedFor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setVotedFor(int nodeId) {
        throw new UnsupportedOperationException();
    }

    // 1
    @Override
    public List<RaftLogEntry<T>> getEntries(long indexFrom, int limit) {

        if (indexFrom == 0L && limit == 1) {
            return List.of();
        }

        if (getLastLogIndex() < indexFrom) {
            return List.of();
        }

        log.debug("getEntriesStartingFrom({}): logEntries: {}", indexFrom, logEntries);

        final long indexTo = indexFrom + limit;

        final int indexToMin = (int) Math.min(indexTo, logEntries.size());

        log.debug("indexTo={} indexToMin={}", indexTo, indexToMin);

        final List<RaftLogEntry<T>> sublistView = logEntries.subList((int) indexFrom - 1, indexToMin);
        return new ArrayList<>(sublistView);
    }

    @Override
    public void close() {
        // do nothing
    }
}
