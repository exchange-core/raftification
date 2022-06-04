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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class DiskTermIndex {

    private static final Logger log = LoggerFactory.getLogger(DiskTermIndex.class);

    private final ByteBuffer termIndexWriteBuffer = ByteBuffer.allocateDirect(64);

    // maps every known term to last index of entries of previous term
    // so for each key K it has reference to element preceding to first element in term K
    // TODO can use binary search (both in the same array)
    private final NavigableMap<Integer, Long> termLastIndex = new TreeMap<>();

    // TODO can use binary search (both)
    // maps last index of each known term to its number
    // this is how leader can quickly resolve term of previous record known to follower
    private final NavigableMap<Long, Integer> indexToTermMap = new TreeMap<>();

    private int lastLogTerm = 0; // cached term of the last entry

    private RandomAccessFile termIndexRaf;
    private FileChannel termIndexChannel;

    public DiskTermIndex(final Path termIndexFileName) throws IOException {

            log.debug("Reading terms index file: {} ...", termIndexFileName);

            termIndexRaf = new RandomAccessFile(termIndexFileName.toString(), "rwd");
            termIndexChannel = termIndexRaf.getChannel();

            final InputStream is = Channels.newInputStream(termIndexChannel);
            final BufferedInputStream bis = new BufferedInputStream(is);
            final DataInputStream dis = new DataInputStream(bis);

            lastLogTerm = 0;

            while (dis.available() != 0) {

                final long lastIndex = dis.readLong();
                final int prevTerm = dis.readInt();
                final int term = dis.readInt();

                log.debug("  term {}->{} lastIndex={}", prevTerm, term, lastIndex);

                termLastIndex.put(term, lastIndex);
                indexToTermMap.put(lastIndex, prevTerm);

                lastLogTerm = term;
            }

            log.debug("termLastIndex={}", termLastIndex);
            log.debug("indexToTermMap={}", indexToTermMap);

            log.debug("lastLogTerm={}", lastLogTerm);

    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public long findLastEntryInTerm(long indexAfter, long indexBeforeIncl, int term) {

        if (term == lastLogTerm) {
            final long lastIndexForTerm = Math.max(indexAfter, Math.min(indexBeforeIncl, lastLogTerm));
            log.debug("findLastEntryInTerm for current term is {}", lastIndexForTerm);
            return lastIndexForTerm;
        }

        log.debug("Using termLastIndex (term={}): {}", term, termLastIndex);
        final Long lastIdxForTerm = termLastIndex.get(term);
        if (lastIdxForTerm == null) {
            log.debug("Last index not found for term {}", term);
            return indexAfter;
        } else {
            final long lastIndexForTerm = Math.max(indexAfter, Math.min(indexBeforeIncl, lastIdxForTerm));
            log.debug("findLastEntryInTerm from index is {}", lastIndexForTerm);
            return lastIndexForTerm;
        }
    }

    public int findTermOfIndex(long index) {

        if (index == 0) {
            return 0;
        }

        final Map.Entry<Long, Integer> indexTermRecord = indexToTermMap.ceilingEntry(index);
        log.debug("findTermOfIndex({}) -> longIntegerEntry={} lastLogTerm={}", index, indexTermRecord, lastLogTerm);
        return (indexTermRecord != null) ? indexTermRecord.getValue() : lastLogTerm;
    }

    public void updateTermByAppendedEntry(int term, long lastIndex) {
        if (term < lastLogTerm) {
            throw new IllegalStateException("TBC: appendEntry - term " + lastLogTerm + " is less than last frame " + lastLogTerm);
        } else if (term > lastLogTerm) {

            log.debug("Updating term: {} -> {}", lastLogTerm, term);

            termLastIndex.put(term, lastIndex);

            // saving previous term
            indexToTermMap.put(lastIndex, lastLogTerm);

            log.debug("termLastIndex: {}", termLastIndex);
            log.debug("indexToTermMap: {}", indexToTermMap);

            try {

                termIndexWriteBuffer.putLong(lastIndex);
                termIndexWriteBuffer.putInt(lastLogTerm);
                termIndexWriteBuffer.putInt(term);
                termIndexWriteBuffer.flip();
                termIndexChannel.write(termIndexWriteBuffer);
                termIndexWriteBuffer.clear();

            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            lastLogTerm = term;

            log.info("termIndex: {}", termLastIndex);
        }
    }


    public void truncateStart(int newLastTerm) {

        log.debug("termLastIndex before: {}", termLastIndex);
        // maps every known term to last index of entries of previous term
        termLastIndex.tailMap(newLastTerm, false).clear(); // forget about any newer terms
        log.debug("termLastIndex after: {}", termLastIndex);

        final long lastTermSwitchIndex = termLastIndex.lastEntry().getValue(); // assume there is at least one entry
        log.debug("lastTermSwitchIndex: {}", lastTermSwitchIndex);

        log.debug("indexToTermMap before: {}", indexToTermMap);
        // maps last index of each known term to its number
        indexToTermMap.tailMap(lastTermSwitchIndex, false).clear(); // remove all later indexes
        log.debug("indexToTermMap after: {}", indexToTermMap);

        if (indexToTermMap.size() != termLastIndex.size()) {
            throw new IllegalStateException("indexToTermMap size does not match to termLastIndex");
        }

        lastLogTerm = newLastTerm;

        final int newTermIndexSize = indexToTermMap.size() * 16;
        log.debug("performing term index truncation: {} bytes ...", newTermIndexSize);
        try {
            termIndexChannel.position(newTermIndexSize);
            termIndexChannel.truncate(newTermIndexSize);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void truncateFinalize() {

        final int newTermIndexSize = indexToTermMap.size() * 16;
        log.debug("performing term index truncation: {} bytes ...", newTermIndexSize);
        try {
            termIndexChannel.position(newTermIndexSize);
            termIndexChannel.truncate(newTermIndexSize);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void closeCurrentFiles() throws IOException {
        if (termIndexChannel != null) {
            termIndexChannel.close();
            termIndexRaf.close();
        }
    }
}
