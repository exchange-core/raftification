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
import exchange.core2.raftification.messages.RsmCommand;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public final class RaftDiskLogRepository<T extends RsmCommand> implements IRaftLogRepository<T> {

    private static final Logger log = LoggerFactory.getLogger(RaftDiskLogRepository.class);

    // TODO put into configuration class

    private final RaftDiskLogConfig config;

    // indexes
    private final DiskOffsetIndex diskOffsetIndex;
    private final DiskTermIndex diskTermIndex;

    // request factory
    private final RsmRequestFactory<T, ?> rsmRequestFactory;

    private RandomAccessFile logRaf;
    private FileChannel logWriteChannel;
    private FileChannel logReadChannel;

    private RandomAccessFile stateRaf;

    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    private int currentTerm = 0;

    // candidateId that received vote in current term (or -1 if none)
    private int votedFor = -1;

    // TODO ReadWrite locks

    private final long baseSnapshotId; // TODO implement snapshots
    private final int filesCounter = 0; // TODO implement file counter

    private long writtenBytes = 0L;
    private long lastIndex = 0L; // last entry index (counter)

    private final ByteBuffer journalWriteBuffer = ByteBuffer.allocateDirect(512 * 1024);

    // TODO cleanup cache (after confirmation + size limit)
    // always contains at least all records after index point
    private final NavigableMap<Long, RaftLogEntry<T>> entriesCache = new TreeMap<>();

    private SnapshotDescriptor lastSnapshotDescriptor = null; // todo implement
    private JournalDescriptor lastJournalDescriptor;

    public RaftDiskLogRepository(RsmRequestFactory<T, ?> rsmRequestFactory, RaftDiskLogConfig raftDiskLogConfig) {
        this.rsmRequestFactory = rsmRequestFactory;
        this.config = raftDiskLogConfig;

        baseSnapshotId = 0L;

        try {

            if (Files.notExists(config.getWorkFolder())) {
                log.info("Directory {} does not exist, creating ...", config.getWorkFolder());
                Files.createDirectories(config.getWorkFolder());
            }

            final Path termIndexFileName = resolveTermIndexPath(raftDiskLogConfig, filesCounter, baseSnapshotId);
            this.diskTermIndex = new DiskTermIndex(termIndexFileName);

            final Path offsetIndexFileName = resolveOffsetIndexPath(raftDiskLogConfig, filesCounter, baseSnapshotId);
            this.diskOffsetIndex = new DiskOffsetIndex(offsetIndexFileName, raftDiskLogConfig);

            final Path logFileName = resolveJournalPath(config, filesCounter, baseSnapshotId);
            initializeLog(diskOffsetIndex, logFileName);

            final Path statePath = resolveStatePath(config, filesCounter, baseSnapshotId);
            initializeState(statePath);

            // TODO validations

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    @Override
    public long findLastEntryInTerm(long indexAfter, long indexBeforeIncl, int term) {
        return diskTermIndex.findLastEntryInTerm(indexAfter, indexBeforeIncl, term);
    }

    @Override
    public int findTermOfIndex(long index) {
        return diskTermIndex.findTermOfIndex(index);
    }

    @Override
    public long getLastLogIndex() {
        return lastIndex;
    }

    @Override
    public int getLastLogTerm() {
        return diskTermIndex.getLastLogTerm();
    }

    @Override
    public long appendEntry(RaftLogEntry<T> logEntry, boolean endOfBatch) {

        log.debug("appendEntry: eob={} {}", endOfBatch, logEntry);

        final int term = logEntry.term();

        diskTermIndex.updateTermByAppendedEntry(term, lastIndex);

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
    public int calculateLogHash(long lastApplied) {

        // TODO take initial hash from most recent snapshot

        final long indexFrom = 1L;

        final LongLongPair indexStartingIndex = diskOffsetIndex.findStartingIndexPoint(indexFrom);
        final long startOffset = indexStartingIndex.getOne();
        final long floorIndex = indexStartingIndex.getTwo();

        log.debug("calculateLogHash reading from {} - floorIndex:{} offset:{}", indexFrom, floorIndex, startOffset);

        final int limit = (int) lastApplied;

        final List<RaftLogEntry<T>> raftLogEntries = getRaftLogEntries(floorIndex, indexFrom, limit, startOffset);

        int hash = 0;

        for (RaftLogEntry<T> entry : raftLogEntries) {
            final int entryHash = entry.hashCode();
            hash = Objects.hash(hash, entryHash);
            log.debug("hash: {} <- {}", hash, entryHash);
        }

        return hash;
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

        final LongLongPair startingIndexPoint = diskOffsetIndex.findStartingIndexPoint(removeAfter + 1);
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

        // clean offset index
        diskOffsetIndex.truncateStart(removeAfter);


        // term of new last element
        final int newLastTerm = findTermOfIndex(removeAfter);
        log.debug("newLastTerm: {}", newLastTerm);

        diskTermIndex.truncateStart(newLastTerm);

        lastIndex = removeAfter;

        // doing log truncation
        log.debug("performing log truncation: {} bytes ...", logTruncateOffset);
        logWriteChannel.position(logTruncateOffset);
        logWriteChannel.truncate(logTruncateOffset);
        writtenBytes = logTruncateOffset;


        diskOffsetIndex.truncateFinalize();

        diskTermIndex.truncateFinalize();

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

        final LongLongPair indexStartingIndex = diskOffsetIndex.findStartingIndexPoint(indexFrom);
        final long startOffset = indexStartingIndex.getOne();
        final long floorIndex = indexStartingIndex.getTwo();

        log.debug("Using disk: reading from {} - floorIndex:{} offset:{}", indexFrom, floorIndex, startOffset);

        return getRaftLogEntries(floorIndex, indexFrom, limit, startOffset);
    }

    // TODO convert to static
    private void initializeLog(DiskOffsetIndex diskOffsetIndex,
                               Path logFileName) throws IOException {

        final LongLongPair lastIndexRecord = diskOffsetIndex.findLastIndexRecord();
        final long readFromIndex = lastIndexRecord.getTwo();
        final long startOffset = lastIndexRecord.getOne();

        log.debug("Reading raft log file: {} ...", logFileName);

        logRaf = new RandomAccessFile(logFileName.toString(), "rwd");
        logWriteChannel = logRaf.getChannel();
        logReadChannel = logRaf.getChannel();

        writtenBytes = logRaf.length();
        log.debug("writtenBytes={}", writtenBytes);

        log.debug("Last index record - readFromIndex:{} offset:{}", readFromIndex, startOffset);

        final List<RaftLogEntry<T>> entries = getRaftLogEntries(readFromIndex, readFromIndex, Integer.MAX_VALUE, startOffset);

        long index = readFromIndex;
        for (RaftLogEntry<T> entry : entries) {
            entriesCache.put(index, entry);
            log.debug("{}. {}", index, entry);
            index++;
        }

        lastIndex = index - 1;
        log.debug("lastIndex={}", lastIndex);
    }

    private void initializeState(Path statePath) throws IOException {
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


    /**
     * call only from journal thread
     */
    private void registerNextJournal(long seq, long timestamp) {

        lastJournalDescriptor = new JournalDescriptor(timestamp, seq, lastSnapshotDescriptor, lastJournalDescriptor);
    }


    private static Path resolveJournalPath(RaftDiskLogConfig config, int partitionId, long snapshotId) {
        return config.getWorkFolder().resolve(String.format("%s_%d_%04X.ecrl", config.getExchangeId(), snapshotId, partitionId));
    }

    private static Path resolveOffsetIndexPath(RaftDiskLogConfig config, int partitionId, long snapshotId) {
        return config.getWorkFolder().resolve(String.format("%s_%d_%04X.oidx", config.getExchangeId(), snapshotId, partitionId));
    }

    private static Path resolveTermIndexPath(RaftDiskLogConfig config, int partitionId, long snapshotId) {
        return config.getWorkFolder().resolve(String.format("%s_%d_%04X.tidx", config.getExchangeId(), snapshotId, partitionId));
    }

    private static Path resolveStatePath(RaftDiskLogConfig config, int partitionId, long snapshotId) {
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

            // append index record if required
            diskOffsetIndex.appendIndex(writtenBytes, lastIndex + 1);

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

    /**
     * TODO convert to Stream ?
     * <p>
     * Read entries from the log file
     *
     * @param floorIndex    - index of the first entry where the stream is positioned to
     * @param readFromIndex - index of first entry to add into the response
     * @param limit         - max number of entries to add into a response collection
     * @return list of entries
     */
    private List<RaftLogEntry<T>> getRaftLogEntries(long floorIndex,
                                                    long readFromIndex,
                                                    int limit,
                                                    long startOffset) {
        try {
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

            final List<RaftLogEntry<T>> list = new ArrayList<>();

            log.debug("Reading commands: floorIndex={} indexFrom={} limit={} dis.available()={}", floorIndex, readFromIndex, limit, dis.available());

            long idx = floorIndex;

            int c = 0;
            while (dis.available() != 0) {

                c++;

                log.debug("  idx={} available={}", idx, dis.available());

                final RaftLogEntry<T> logEntry = RaftLogEntry.create(dis, rsmRequestFactory);

                if (idx >= readFromIndex) {
                    log.debug("Adding record into collection idx={} {}", idx, logEntry);
                    list.add(logEntry);
                }

                log.debug("list.size()={} limit={}", list.size(), limit);
                if (list.size() == limit) {
                    break;
                }

                idx++;
            }

            log.debug("Performed {} read iterations, found {} (total) matching entries", c, list.size());
            return list;

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
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

        diskOffsetIndex.closeCurrentFiles();
        diskTermIndex.closeCurrentFiles();

        stateRaf.close();
    }
}
