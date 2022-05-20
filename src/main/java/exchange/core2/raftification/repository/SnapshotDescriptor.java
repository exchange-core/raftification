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

import org.jetbrains.annotations.NotNull;

import java.util.NavigableMap;
import java.util.TreeMap;

public class SnapshotDescriptor implements Comparable<SnapshotDescriptor>{


    private final long snapshotId; // 0 means empty snapshot (clean start)

    // sequence when snapshot was made
    private final long seq;
    private final long timestampNs;

    // next and previous snapshots
    private final SnapshotDescriptor prev;
    private SnapshotDescriptor next = null; // TODO can be a list

    private final int numMatchingEngines;
    private final int numRiskEngines;

    // all journals based on this snapshot
    // mapping: startingSeq -> JournalDescriptor
    private final NavigableMap<Long, JournalDescriptor> journals = new TreeMap<>();


    public SnapshotDescriptor(long snapshotId, long seq, long timestampNs, SnapshotDescriptor prev, int numMatchingEngines, int numRiskEngines) {
        this.snapshotId = snapshotId;
        this.seq = seq;
        this.timestampNs = timestampNs;
        this.prev = prev;
        this.numMatchingEngines = numMatchingEngines;
        this.numRiskEngines = numRiskEngines;
    }

    /**
     * Create initial empty snapshot descriptor
     *
     * @param initialNumME - number of matching engine instances
     * @param initialNumRE - number of risk engine instances
     * @return new instance
     */
    public static SnapshotDescriptor createEmpty(int initialNumME, int initialNumRE) {
        return new SnapshotDescriptor(0, 0, 0, null, initialNumME, initialNumRE);
    }

    public SnapshotDescriptor createNext(long snapshotId, long seq, long timestampNs) {
        return new SnapshotDescriptor(snapshotId, seq, timestampNs, this, numMatchingEngines, numRiskEngines);
    }

    @Override
    public int compareTo(@NotNull SnapshotDescriptor o) {
        return Long.compare(this.seq, o.seq);
    }

}
