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

public class JournalDescriptor {

    private final long timestamp;
    private final long seqFirst;
    private long seqLast = -1; // -1 if not finished yet

    private final SnapshotDescriptor baseSnapshot;

    private final JournalDescriptor prev; // can be null

    private JournalDescriptor next = null; // can be null

    public JournalDescriptor(long timestamp, long seqFirst, SnapshotDescriptor baseSnapshot, JournalDescriptor prev) {
        this.timestamp = timestamp;
        this.seqFirst = seqFirst;
        this.baseSnapshot = baseSnapshot;
        this.prev = prev;
    }
}
