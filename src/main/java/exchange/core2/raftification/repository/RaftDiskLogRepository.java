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

import exchange.core2.raftification.RsmRequestFactory;
import exchange.core2.raftification.messages.RaftLogEntry;
import exchange.core2.raftification.messages.RsmRequest;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public final class RaftDiskLogRepository<T extends RsmRequest> implements IRaftLogRepository<T> {

    private static final Logger log = LoggerFactory.getLogger(RaftDiskLogRepository.class);

    // TODO put into configuration class

    private final RaftDiskLogConfig config;

    private RandomAccessFile logRaf;
    private FileChannel logWriteChannel;
    private FileChannel logReadChannel;

    private RandomAccessFile offsetIndexRaf;
    private FileChannel offsetIndexChannel;

    private RandomAccessFile termIndexRaf;
    private FileChannel termIndexChannel;

    private RandomAccessFile stateRaf;

    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    private int currentTerm = 0;

    // candidateId that received vote in current term (or -1 if none)
    private int votedFor = -1;

    // index -> position in file // TODO keep term in the index?
    // TODO implement multi-file index
    private NavigableMap<Long, Long> offsetIndex = new TreeMap<>();

    // TODO ReadWrite locks

    private final long baseSnapshotId; // TODO implement snapshots

    private final int filesCounter = 0; // TODO implement file counter

    private long writtenBytes = 0;
    private long lastIndexWrittenAt = 0;

    private long lastIndex = 0L; // last entry index (counter)
    private int lastLogTerm = 0; // cached term of the last entry

    private final ByteBuffer journalWriteBuffer = ByteBuffer.allocateDirect(512 * 1024);
    private final ByteBuffer offsetIndexWriteBuffer = ByteBuffer.allocateDirect(64);
    private final ByteBuffer termIndexWriteBuffer = ByteBuffer.allocateDirect(64);


    // maps every known term to last index of entries of previous term
    // so for each key K it has reference to element preceding to first element in term K
    // TODO load from file
    // TODO can use binary search (both in the same array)
    private NavigableMap<Integer, Long> termLastIndex = new TreeMap<>();

    // TODO load from file
    // TODO can use binary search (both)
    // maps last index of each known term to its number
    // this is how leader can quickly resolve term of previous record known to follower
    private NavigableMap<Long, Integer> indexToTermMap = new TreeMap<>();


    // TODO cleanup cache (after confirmation + size limit)
    // always contains at least all records after index point
    private NavigableMap<Long, RaftLogEntry<T>> entriesCache = new TreeMap<>();


    private SnapshotDescriptor lastSnapshotDescriptor = null; // todo implemnt
    private JournalDescriptor lastJournalDescriptor;


    private final RsmRequestFactory<T> rsmRequestFactory;

    public RaftDiskLogRepository(RsmRequestFactory<T> rsmRequestFactory, RaftDiskLogConfig raftDiskLogConfig) {
        this.rsmRequestFactory = rsmRequestFactory;
        this.config = raftDiskLogConfig;

        initialize();

        baseSnapshotId = 0L;
    }


    @Override
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

    @Override
    public int findTermOfIndex(long index) {

        if (index == 0) {
            return 0;
        }

        final Map.Entry<Long, Integer> longIntegerEntry = indexToTermMap.ceilingEntry(index);
        log.debug("findTermOfIndex({}) -> longIntegerEntry={} lastLogTerm={}", index, longIntegerEntry, lastLogTerm);
        return (longIntegerEntry != null) ? longIntegerEntry.getValue() : lastLogTerm;
    }

    @Override
    public long getLastLogIndex() {
        return lastIndex;
    }

    @Override
    public int getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public long appendEntry(RaftLogEntry<T> logEntry, boolean endOfBatch) {

        log.debug("appendEntry: eob={} {}", endOfBatch, logEntry);

        final int term = logEntry.term();

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

        lastIndex++;

        entriesCache.put(lastIndex, logEntry);

        logEntry.serialize(journalWriteBuffer);

        if (endOfBatch || journalWriteBuffer.position() >= config.getJournalBufferFlushTrigger()) {

            // flushing on end of batch or when buffer is full
            flushBufferSync(false, logEntry.timestamp());
        }

        log.debug("assigned index {}", lastIndex);
        return lastIndex;
    }

    @Override
    public void appendOrOverride(List<RaftLogEntry<T>> newEntries, long prevLogIndex) {

        log.debug("appendOrOverride(newEntries={} , prevLogIndex={}", newEntries, prevLogIndex);

        try {

            // check for missed records
            if (prevLogIndex > lastIndex) {
                throw new IllegalStateException("Can not accept prevLogIndex=" + prevLogIndex + " because=" + lastIndex);
            }

            // check if leader is overriding some records
            if (prevLogIndex < lastIndex) {

                log.debug("Overriding {} records...", lastIndex - prevLogIndex);

                // TODO loading just to compare term - can be done faster
                final long removeIdx = verifyTerms(newEntries, prevLogIndex);
                log.debug("removeIdx={}", removeIdx);
                if (removeIdx != -1) {
                    truncateLog(removeIdx - 1);
                }
            }

            // adding missing records

            // lastIndex may change after verification - skip messages that are matching (were not removed)
            final int skipEntries = (int) (lastIndex - prevLogIndex);

            log.debug("skipEntries = {} (lastIndex={} - prevLogIndex={})", skipEntries, lastIndex, prevLogIndex);

            if (skipEntries >= 0) {
                final int size = newEntries.size();
                log.debug("lastIndex={}", size);
                for (int i = skipEntries; i < size; i++) {
                    log.debug("i={} newEntries.get(i)={} eob={}", i, newEntries.get(i), i == size - 1);

                    appendEntry(newEntries.get(i), i == size - 1);
                }
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public void setCurrentTerm(int term) {

        try {
            stateRaf.seek(0);
            stateRaf.writeInt(term);
            currentTerm = term;
        } catch (IOException ex) {
            throw new IllegalStateException("Can not save state: term=" + term, ex);
        }
    }

    @Override
    public int getVotedFor() {
        return votedFor;
    }

    @Override
    public void setVotedFor(int nodeId) {

        try {
            stateRaf.seek(4);
            stateRaf.writeInt(votedFor);
            votedFor = nodeId;
        } catch (IOException ex) {
            throw new IllegalStateException("Can not save state: votedFor=" + nodeId, ex);
        }
    }

    private long findLogTruncateOffset(long removeAfter) throws IOException {

        final LongLongPair startingIndexPoint = findStartingIndexPoint(removeAfter + 1);
        final long startOffset = startingIndexPoint.getOne();
        final long floorIndex = startingIndexPoint.getTwo();

        log.debug("Last index record: floorIndex={} startOffset={}", floorIndex, startOffset);


        if (floorIndex == removeAfter + 1) {
            log.debug("Index hit: using startOffset={}", startOffset);
            return startOffset;
        }

        long idx = floorIndex;

        logReadChannel.position(startOffset);

        final InputStream is = Channels.newInputStream(logReadChannel);
        final BufferedInputStream bis = new BufferedInputStream(is);
        final DataInputStream dis = new DataInputStream(bis);

        while (dis.available() != 0) {

            final RaftLogEntry<T> entry = RaftLogEntry.create(dis, rsmRequestFactory);

            // log.debug("{}. {}", idx, entry);

            idx++;

            if (idx == removeAfter) {
                final long position = logReadChannel.position();
                log.debug("Found entry idx={} ends at position={}", removeAfter, position);
                return position;
            }
        }

        throw new IllegalStateException("Can not reach index removeAfter=" + removeAfter);
    }


    private void truncateLog(long removeAfter) throws IOException {

        final long logTruncateOffset = findLogTruncateOffset(removeAfter);

        log.debug("Removing log data after idx={} logTruncateOffset: {}", removeAfter, logTruncateOffset);


        // clean cache
        log.debug("entriesCache before: {}", entriesCache);
        entriesCache.tailMap(removeAfter, false).clear();
        log.debug("entriesCache after: {}", entriesCache);

        // remove invalid index records
        log.debug("offsetIndex before: {}", offsetIndex);
        offsetIndex.tailMap(removeAfter, false).clear();
        log.debug("offsetIndex after: {}", offsetIndex);

        // updating offset where last index was written
        final Map.Entry<Long, Long> lastIndexEntry = offsetIndex.lastEntry();
        lastIndexWrittenAt = (lastIndexEntry == null) ? 0L : lastIndexEntry.getValue();
        log.debug("lastIndexWrittenAt new: {}", lastIndexWrittenAt);

        // term of new last element
        final int newLastTerm = findTermOfIndex(removeAfter);
        log.debug("newLastTerm: {}", newLastTerm);

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

        lastIndex = removeAfter;
        lastLogTerm = newLastTerm;

        // doing log truncation
        log.debug("performing log truncation: {} bytes ...", logTruncateOffset);
        logWriteChannel.position(logTruncateOffset);
        logWriteChannel.truncate(logTruncateOffset);
        writtenBytes = logTruncateOffset;

        final int newOffsetIndexSize = offsetIndex.size() * 16;
        log.debug("performing offset index truncation: {} bytes ...", newOffsetIndexSize);
        offsetIndexChannel.position(newOffsetIndexSize);
        offsetIndexChannel.truncate(newOffsetIndexSize);

        final int newTermIndexSize = indexToTermMap.size() * 16;
        log.debug("performing term index truncation: {} bytes ...", newTermIndexSize);
        termIndexChannel.position(newTermIndexSize);
        termIndexChannel.truncate(newTermIndexSize);

        log.debug("truncateLog completed");
    }

    /**
     * Possible scenario when every entry is not matching - it is okay, because term/index verified previously and common entry is found
     */
    private long verifyTerms(List<RaftLogEntry<T>> newEntries, long prevLogIndex) {

        log.info("Verify terms: requesting {} entries starting from {}", newEntries.size(), prevLogIndex);

        final List<RaftLogEntry<T>> existingEntries = getEntries(prevLogIndex, newEntries.size());
        final int intersectionLength = Math.min(existingEntries.size(), newEntries.size());

        for (int i = 0; i < intersectionLength; i++) {
            if (existingEntries.get(i).term() != newEntries.get(i).term()) {
                log.debug("Items not matching at position {}: existing:{} new:{}", prevLogIndex + i, existingEntries.get(i), newEntries.get(i));
                return prevLogIndex + i;
            }
        }

        log.info("All items are matching");
        return -1;
    }

    /**
     * @return offset+index to start looking from
     */
    private LongLongPair findStartingIndexPoint(long indexFrom) {
        final Map.Entry<Long, Long> entry = offsetIndex.floorEntry(indexFrom);
        final long startOffset = (entry == null) ? 0L : entry.getValue();
        final long floorIndex = (entry == null) ? 1L : entry.getKey();
        return PrimitiveTuples.pair(startOffset, floorIndex);
    }

    @Override
    public List<RaftLogEntry<T>> getEntries(long indexFrom, int limit) {

        log.debug("getEntries(from {}, limit {})", indexFrom, limit);

        if (indexFrom == 0L && limit == 1) {
            return List.of();
        }

        // check if repository have records range
        if (indexFrom > lastIndex) {
            return List.of();
        }

        // try using cache first

        final SortedMap<Long, RaftLogEntry<T>> subMap = entriesCache.tailMap(indexFrom);

        // log.debug("subMap: {}", subMap);

        if (!subMap.isEmpty()) {
            final Long firstKey = subMap.firstKey();
            log.debug("Trying getting values from cache, firstKey = {} (expecting {})", firstKey, indexFrom);
            if (firstKey == indexFrom) {
                // if cache contains indexFrom - it contains everything
                return subMap.values().stream().limit(limit).collect(Collectors.toList());
            }
        }

        // using disk (cache did not work)

        final LongLongPair indexStartingIndex = findStartingIndexPoint(indexFrom);
        final long startOffset = indexStartingIndex.getOne();
        final long floorIndex = indexStartingIndex.getTwo();

        try {
            log.debug("Using disk: reading from {} - floorIndex:{} offset:{}", indexFrom, floorIndex, startOffset);
            logReadChannel.position(startOffset);
            log.debug("Position ok");
        } catch (IOException ex) {
            throw new RuntimeException("can not read log at offset " + startOffset, ex);
        }

        // dont use try-catch as we dont want to close readChannel
        final InputStream is = Channels.newInputStream(logReadChannel);
        final BufferedInputStream bis = new BufferedInputStream(is);
        final DataInputStream dis = new DataInputStream(bis);
        try {

            return readCommands(dis, floorIndex, indexFrom, limit);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    // TODO convert to static
    private void initialize() {

        try {

            {
                final Path offsetIndexFileName = resolveOffsetIndexPath(filesCounter, baseSnapshotId);

                log.debug("Reading offset index file: {} ...", offsetIndexFileName);

                offsetIndexRaf = new RandomAccessFile(offsetIndexFileName.toString(), "rwd");
                offsetIndexChannel = offsetIndexRaf.getChannel();

                final InputStream is = Channels.newInputStream(offsetIndexChannel);
                final BufferedInputStream bis = new BufferedInputStream(is);
                final DataInputStream dis = new DataInputStream(bis);

                while (dis.available() != 0) {

                    // read index record (16 bytes)
                    final long idx = dis.readLong();
                    final long offset = dis.readLong();

                    log.debug("  index {} -> offset {}", idx, offset);
                    offsetIndex.put(idx, offset);
                }
            }

            {
                final Map.Entry<Long, Long> lastIndexEntry = offsetIndex.lastEntry();
                final long floorIndex = (lastIndexEntry == null) ? 0L : lastIndexEntry.getKey();
                final long startOffset = (lastIndexEntry == null) ? 0L : lastIndexEntry.getValue();

                lastIndexWrittenAt = startOffset;
                log.debug("lastIndexWrittenAt={}", lastIndexWrittenAt);

                final Path logFileName = resolveJournalPath(filesCounter, baseSnapshotId);

                log.debug("Reading raft log file: {} ...", logFileName);

                logRaf = new RandomAccessFile(logFileName.toString(), "rwd");
                logWriteChannel = logRaf.getChannel();
                logReadChannel = logRaf.getChannel();

                writtenBytes = logRaf.length();
                log.debug("writtenBytes={}", writtenBytes);

                try {
                    log.debug("Last index record - floorIndex:{} offset:{}", floorIndex, startOffset);
                    logReadChannel.position(startOffset);
                    log.debug("Position ok");
                } catch (IOException ex) {
                    throw new RuntimeException("can not read log at offset " + startOffset, ex);
                }

                final InputStream is = Channels.newInputStream(logReadChannel);
                final BufferedInputStream bis = new BufferedInputStream(is);
                final DataInputStream dis = new DataInputStream(bis);

                final List<RaftLogEntry<T>> entries = readCommands(dis, floorIndex, floorIndex, Integer.MAX_VALUE);


                long index = floorIndex;
                for (RaftLogEntry<T> entry : entries) {
                    index++;
                    entriesCache.put(index, entry);
                    log.debug("{}. {}", index, entry);
                }

                lastIndex = index;
                log.debug("lastIndex={}", lastIndex);
            }

            {
                final Path termIndexFileName = resolveTermIndexPath(filesCounter, baseSnapshotId);

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

            {
                final Path statePath = resolveStatePath(filesCounter, baseSnapshotId);

                log.debug("Reading state file: {} ...", statePath);

                stateRaf = new RandomAccessFile(statePath.toString(), "rwd");

                if (stateRaf.length() == 8L) {

                    stateRaf.seek(0);
                    currentTerm = stateRaf.readInt();

                    stateRaf.seek(4);
                    votedFor = stateRaf.readInt();

                    log.info("State: currentTerm={} votedFor={}", currentTerm, votedFor);
                } else {
                    log.info("stateRaf.available()={} setting length to 8", stateRaf.length());
                    stateRaf.setLength(8L);
                    stateRaf.seek(0);
                }

            }

            // TODO validations

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    /**
     * call only from journal thread
     */
    private void registerNextJournal(long seq, long timestamp) {

        lastJournalDescriptor = new JournalDescriptor(timestamp, seq, lastSnapshotDescriptor, lastJournalDescriptor);
    }


    private Path resolveJournalPath(int partitionId, long snapshotId) {
        return config.getWorkFolder().resolve(String.format("%s_%d_%04X.ecrl", config.getExchangeId(), snapshotId, partitionId));
    }

    private Path resolveOffsetIndexPath(int partitionId, long snapshotId) {
        return config.getWorkFolder().resolve(String.format("%s_%d_%04X.oidx", config.getExchangeId(), snapshotId, partitionId));
    }

    private Path resolveTermIndexPath(int partitionId, long snapshotId) {
        return config.getWorkFolder().resolve(String.format("%s_%d_%04X.tidx", config.getExchangeId(), snapshotId, partitionId));
    }

    private Path resolveStatePath(int partitionId, long snapshotId) {
        return config.getWorkFolder().resolve(String.format("%s_%d_%04X.ecst", config.getExchangeId(), snapshotId, partitionId));
    }

    private void flushBufferSync(final boolean forceStartNextFile,
                                 final long timestampNs) {

        try {

            log.debug("Flushing buffer forceStartNextFile={}, timestampNs={} ...", forceStartNextFile, timestampNs);

            // uncompressed write for single messages or small batches
            writtenBytes += journalWriteBuffer.position();
            journalWriteBuffer.flip();
            logWriteChannel.write(journalWriteBuffer);
            journalWriteBuffer.clear();

            //
            if (writtenBytes > lastIndexWrittenAt + config.getIndexRecordEveryNBytes()) {

                lastIndexWrittenAt = writtenBytes;

                final long nextIndex = lastIndex + 1;

                log.debug("Adding index record:{}->{}", nextIndex, writtenBytes);

                offsetIndex.put(nextIndex, writtenBytes);

                offsetIndexWriteBuffer.putLong(nextIndex);
                offsetIndexWriteBuffer.putLong(writtenBytes);
                offsetIndexWriteBuffer.flip();
                offsetIndexChannel.write(offsetIndexWriteBuffer);
                offsetIndexWriteBuffer.clear();

            }

            if (forceStartNextFile || writtenBytes >= config.getJournalFileMaxSize()) {

//            log.info("RAW {}", LatencyTools.createLatencyReportFast(hdrRecorderRaw.getIntervalHistogram()));
//            log.info("LZ4-compression {}", LatencyTools.createLatencyReportFast(hdrRecorderLz4.getIntervalHistogram()));

                log.error("New file not implemented yet");
                System.exit(-5);

                // todo start preparing new file asynchronously, but ONLY ONCE
                // startNewFile(timestampNs);
                writtenBytes = 0;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    private List<RaftLogEntry<T>> readCommands(final DataInputStream dis,
                                               long floorIndex,
                                               final long readFromIndex,
                                               final int limit) throws IOException {

        final List<RaftLogEntry<T>> list = new ArrayList<>();

        log.debug("Reading commands: floorIndex={} indexFrom={} limit={} dis.available()={}", floorIndex, readFromIndex, limit, dis.available());

        int c = 0;
        while (dis.available() != 0) {

            c++;

            final long idx = floorIndex++;

//             log.debug("  idx={} available={}", idx, dis.available());

            final RaftLogEntry<T> logEntry = RaftLogEntry.create(dis, rsmRequestFactory);

            if (idx >= readFromIndex) {
                // log.debug("Adding record into collection idx={} {}", idx, logEntry);
                list.add(logEntry);
            }

            // log.debug("list.size()={} limit={}", list.size(), limit);
            if (list.size() == limit) {
                break;
            }
        }

        log.debug("Performed {} read iterations, found {} (total) matching entries", c, list.size());
        return list;
    }


    @Override
    public void close() throws IOException {
        closeCurrentFiles();
    }

    private void closeCurrentFiles() throws IOException {

        log.debug("Closing current files");

        if (logWriteChannel != null) {
            logWriteChannel.close();
            logReadChannel.close();
            logRaf.close();
        }

        if (offsetIndexChannel != null) {
            offsetIndexChannel.close();
            offsetIndexRaf.close();
        }

        if (termIndexChannel != null) {
            termIndexChannel.close();
            termIndexRaf.close();
        }

        stateRaf.close();
    }
}
