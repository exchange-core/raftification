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
import org.agrona.collections.MutableLong;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
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

public final class RaftDiskLogRepository<T extends RsmRequest> implements IRaftLogRepository<T> {

    private static final Logger log = LoggerFactory.getLogger(RaftDiskLogRepository.class);

    // TODO put into configuration class

    private final RaftDiskLogConfig config;

    private RandomAccessFile raf;
    private FileChannel writeChannel;
    private FileChannel readChannel;

    private RandomAccessFile offsetIndexRaf;
    private FileChannel offsetIndexWriteChannel;

    private RandomAccessFile termIndexRaf;
    private FileChannel termIndexWriteChannel;


    // index -> position in file // TODO keep term in the index?
    // TODO implement multi-file index
    private NavigableMap<Long, Long> currentOffsetIndex = new TreeMap<>(); // TODO  use ART ?

    // TODO ReadWrite locks

    private long baseSnapshotId;

    private int filesCounter = 0;

    private long writtenBytes = 0;
    private long lastIndexWrittenAt = 0;

    private long lastIndex = 0L;
    private int lastLogTerm = 0;

    private final ByteBuffer journalWriteBuffer = ByteBuffer.allocateDirect(512 * 1024);
    private final ByteBuffer offsetIndexWriteBuffer = ByteBuffer.allocateDirect(64);
    private final ByteBuffer termIndexWriteBuffer = ByteBuffer.allocateDirect(64);


    // maps every known term to last index of entries of this term
    // TODO load from file
    // TODO can use binary search (both in the same array)
    private NavigableMap<Integer, Long> termLastIndex = new TreeMap<>();

    // TODO load from file
    // TODO can use binary search (both)
    // maps last index of each known term to its number
    // this is how leader can quickly resolve term of previous record known to follower
    private NavigableMap<Long, Integer> indexToTermMap = new TreeMap<>();


    // TODO cleanup cache (after confirmation + size limit)
    private NavigableMap<Long, RaftLogEntry<T>> entriesCache = new TreeMap<>();


    private SnapshotDescriptor lastSnapshotDescriptor = null; // todo implemnt
    private JournalDescriptor lastJournalDescriptor;


    private final RsmRequestFactory<T> rsmRequestFactory;

    public RaftDiskLogRepository(RsmRequestFactory<T> rsmRequestFactory, RaftDiskLogConfig raftDiskLogConfig) {
        this.rsmRequestFactory = rsmRequestFactory;
        this.config = raftDiskLogConfig;

        final long timestamp = System.currentTimeMillis();

        baseSnapshotId = timestamp;

        startNewFile(timestamp);
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

        if (writeChannel == null) {
            log.debug("startNewFile({})..", logEntry.timestamp());
            startNewFile(logEntry.timestamp());
        }

        final ByteBuffer buffer = journalWriteBuffer;

        lastIndex++;

        final int term = logEntry.term();

        if (term < lastLogTerm) {
            throw new IllegalStateException("TBC: appendEntry - term " + lastLogTerm + " is less than last frame " + lastLogTerm);
        } else if (term > lastLogTerm) {

            log.debug("Updating term: {} -> {}", lastLogTerm, term);

            if (lastLogTerm != 0) {
                termLastIndex.put(term, lastIndex - 1);

                // saving previous term
                indexToTermMap.put(lastIndex - 1, lastLogTerm);

                try {

                    termIndexWriteBuffer.putLong(lastIndex);
                    termIndexWriteBuffer.putLong(writtenBytes);
                    termIndexWriteBuffer.flip();
                    termIndexWriteChannel.write(offsetIndexWriteBuffer);
                    termIndexWriteBuffer.clear();

                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

            }

            lastLogTerm = term;

            log.info("termIndex: {}", termLastIndex);
        }

        entriesCache.put(lastIndex, logEntry);

        logEntry.serialize(buffer);

        if (endOfBatch || buffer.position() >= config.getJournalBufferFlushTrigger()) {

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

                // TODO loading just to compare term - can be done faster
                final long removeAfter = verifyTerms(newEntries, prevLogIndex);

                if (removeAfter != -1) {

                    final long position = findPosition(removeAfter);
                    log.debug("Removing after position: {}", position);
                    writeChannel.position(position);
                    writeChannel.truncate(position);
                    writtenBytes = position;

                    truncateIndexRecords(removeAfter);

                    if (true) { // TODO implement
                        log.error("termLastIndex truncation/loading is not implemented");
                        System.exit(-1);
                    }

                    lastIndex = removeAfter;
                    lastLogTerm = getEntries(lastIndex, 1).get(0).term();
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

    private long findPosition(long removeAfter) throws IOException {

        final LongLongPair startingIndexPoint = findStartingIndexPoint(removeAfter);
        final long startOffset = startingIndexPoint.getOne();
        final long floorIndex = startingIndexPoint.getTwo();

        readChannel.position(startOffset);

        long idx = floorIndex;

        try (final InputStream is = Channels.newInputStream(readChannel);
             final BufferedInputStream bis = new BufferedInputStream(is);
             final DataInputStream dis = new DataInputStream(bis)) {

            while (dis.available() != 0) {

                RaftLogEntry.create(dis, rsmRequestFactory);

                idx++;

                if (idx == removeAfter) {
                    return readChannel.position();
                }
            }
        }

        throw new RuntimeException("Can not reach index " + removeAfter);
    }

    private void truncateIndexRecords(long removeAfterIndex) throws IOException {

        // TODO truncate term index

        // clean offset index
        currentOffsetIndex.tailMap(removeAfterIndex, false).clear();

        // clean cache
        entriesCache.tailMap(removeAfterIndex, false).clear();

        final Map.Entry<Long, Long> lastIndexEntry = currentOffsetIndex.lastEntry();

        offsetIndexWriteChannel.position(0L);

        if (lastIndexEntry == null) {
            // empty tree - just clean all file
            lastIndexWrittenAt = 0L;
            offsetIndexWriteChannel.truncate(0L);

        } else {

            // set bytes offset to last known value (maybe not very exact?)
            lastIndexWrittenAt = lastIndexEntry.getValue();

            // remove all records after
            try (final InputStream is = Channels.newInputStream(offsetIndexWriteChannel);
                 final BufferedInputStream bis = new BufferedInputStream(is);
                 final DataInputStream dis = new DataInputStream(bis)) {

                while (dis.available() != 0) {

                    // read index record (16 bytes)
                    final long lastIndex = dis.readLong();
                    dis.readLong();

                    if (lastIndex > lastIndexEntry.getKey()) {
                        final long pos = offsetIndexWriteChannel.position() - 16;
                        offsetIndexWriteChannel.position(pos);
                        offsetIndexWriteChannel.truncate(pos);
                        return;
                    }
                }
            }
        }
    }

    private long verifyTerms(List<RaftLogEntry<T>> newEntries, long prevLogIndex) {
        final List<RaftLogEntry<T>> existingEntries = getEntries(prevLogIndex, newEntries.size());
        final int intersectionLength = Math.min(existingEntries.size(), newEntries.size());

        for (int i = 0; i < intersectionLength; i++) {
            if (existingEntries.get(i).term() != newEntries.get(i).term()) {
                return prevLogIndex + i;
            }
        }

        return -1;
    }

    /**
     * @return offset+index to start looking from
     */
    private LongLongPair findStartingIndexPoint(long indexFrom) {
        final Map.Entry<Long, Long> entry = currentOffsetIndex.floorEntry(indexFrom);
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

        final SortedMap<Long, RaftLogEntry<T>> longRaftLogEntrySortedMap = entriesCache.tailMap(indexFrom);
        final Long firstKey = longRaftLogEntrySortedMap.firstKey();
        log.debug("Trying getting values from cache, firstKey = {}", firstKey);
        if (firstKey != null && firstKey == indexFrom) {
            // if cache contains indexFrom - it contains everything
            return longRaftLogEntrySortedMap.values().stream().limit(limit).collect(Collectors.toList());
        }

        // using disk (cache did not work)

        final LongLongPair indexStartingIndex = findStartingIndexPoint(indexFrom);
        final long startOffset = indexStartingIndex.getOne();
        final long floorIndex = indexStartingIndex.getTwo();

        try {
            log.debug("Using disk: reading from {} - floorIndex:{} offset:{}", indexFrom, floorIndex, startOffset);
            readChannel.position(startOffset);
            log.debug("Position ok");
        } catch (IOException ex) {
            throw new RuntimeException("can not read log at offset " + startOffset, ex);
        }

        final List<RaftLogEntry<T>> entries = new ArrayList<>();

        final MutableLong indexCounter = new MutableLong(floorIndex);
        log.debug("indexCounter={}", indexCounter);


        // dont use try-catch as we dont want to close readChannel
        final InputStream is = Channels.newInputStream(readChannel);
        final BufferedInputStream bis = new BufferedInputStream(is);
        final DataInputStream dis = new DataInputStream(bis);
        try {

            readCommands(dis, entries, indexCounter, indexFrom, limit);
            return entries;

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void startNewFile(final long timestamp) {

        try {

            filesCounter++;

            closeCurrentFiles();

            final Path logFileName = resolveJournalPath(filesCounter, baseSnapshotId);
            final Path offsetIndexFileName = resolveOffsetIndexPath(filesCounter, baseSnapshotId);
            final Path termIndexFileName = resolveTermIndexPath(filesCounter, baseSnapshotId);

            log.debug("Starting new raft log file: {} offset index file: {}", logFileName, offsetIndexFileName);

            if (Files.exists(logFileName)) {
                throw new IllegalStateException("File already exists: " + logFileName);
            }

            if (Files.exists(offsetIndexFileName)) {
                throw new IllegalStateException("File already exists: " + offsetIndexFileName);
            }

            if (Files.exists(termIndexFileName)) {
                throw new IllegalStateException("File already exists: " + termIndexFileName);
            }

            raf = new RandomAccessFile(logFileName.toString(), "rwd");
            writeChannel = raf.getChannel();
            readChannel = raf.getChannel();

            offsetIndexRaf = new RandomAccessFile(offsetIndexFileName.toString(), "rwd");
            offsetIndexWriteChannel = offsetIndexRaf.getChannel();

            termIndexRaf = new RandomAccessFile(termIndexFileName.toString(), "rwd");
            termIndexWriteChannel = termIndexRaf.getChannel();

            registerNextJournal(baseSnapshotId, timestamp); // TODO fix time


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


    private void flushBufferSync(final boolean forceStartNextFile,
                                 final long timestampNs) {

        try {

            log.debug("Flushing buffer forceStartNextFile={}, timestampNs={} ...", forceStartNextFile, timestampNs);

            // uncompressed write for single messages or small batches
            writtenBytes += journalWriteBuffer.position();
            journalWriteBuffer.flip();
            writeChannel.write(journalWriteBuffer);
            journalWriteBuffer.clear();

            //
            if (writtenBytes > lastIndexWrittenAt + config.getIndexRecordEveryNBytes()) {

                lastIndexWrittenAt = writtenBytes;

                final long nextIndex = lastIndex + 1;

                log.debug("Adding index record:{}->{}", nextIndex, writtenBytes);

                currentOffsetIndex.put(nextIndex, writtenBytes);

                offsetIndexWriteBuffer.putLong(nextIndex);
                offsetIndexWriteBuffer.putLong(writtenBytes);
                offsetIndexWriteBuffer.flip();
                offsetIndexWriteChannel.write(offsetIndexWriteBuffer);
                offsetIndexWriteBuffer.clear();

            }

            if (forceStartNextFile || writtenBytes >= config.getJournalFileMaxSize()) {

//            log.info("RAW {}", LatencyTools.createLatencyReportFast(hdrRecorderRaw.getIntervalHistogram()));
//            log.info("LZ4-compression {}", LatencyTools.createLatencyReportFast(hdrRecorderLz4.getIntervalHistogram()));

                // todo start preparing new file asynchronously, but ONLY ONCE
                startNewFile(timestampNs);
                writtenBytes = 0;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


//    private List<RaftLogEntry<T>> readData(final long baseSnapshotId,
//                                           final long indexFrom,
//                                           final int limit) throws IOException {
//
//
//        final List<RaftLogEntry<T>> entries = new ArrayList<>();
//
//        final MutableLong currentIndex = new MutableLong(0L);
//
//        int partitionCounter = 1;
//
//        while (true) {
//
//            final Path path = resolveJournalPath(partitionCounter, baseSnapshotId);
//
//            // TODO Use index
//
//            log.debug("Reading RAFT log file: {}", path.toFile());
//
//            try (final FileInputStream fis = new FileInputStream(path.toFile());
//                 final BufferedInputStream bis = new BufferedInputStream(fis);
//                 final DataInputStream dis = new DataInputStream(bis)) {
//
//                final boolean done = readCommands(dis, entries, currentIndex, indexFrom, limit - entries.size());
//                if (done) {
//                    return entries;
//                }
//
//
//                partitionCounter++;
//                log.debug("EOF reached, reading next partition {}...", partitionCounter);
//
//            } catch (FileNotFoundException ex) {
//                log.debug("FileNotFoundException: currentIndex={}, {}", currentIndex, ex.getMessage());
//                throw ex;
//
//            } catch (EOFException ex) {
//                // partitionCounter++;
//                log.debug("File end reached through exception, currentIndex={} !!!", currentIndex);
//                throw ex;
//            }
//        }
//
//    }


    private boolean readCommands(final DataInputStream dis,
                                 final List<RaftLogEntry<T>> collector,
                                 final MutableLong indexCounter,
                                 final long indexFrom,
                                 final int limit) throws IOException {


        log.debug("Reading commands: indexCounter={} indexFrom={} limit={} dis.available()={}", indexCounter, indexFrom, limit, dis.available());

        int c = 0;
        boolean foundAll = false;
        while (dis.available() != 0) {

            c++;

            final long idx = indexCounter.getAndIncrement();

//             log.debug("  idx={} available={}", idx, dis.available());

            final RaftLogEntry<T> logEntry = RaftLogEntry.create(dis, rsmRequestFactory);

            if (idx >= indexFrom) {
                // log.debug("Adding record into collection idx={} {}", idx, logEntry);
                collector.add(logEntry);
            }

            // log.debug("collector.size()={} limit={}", collector.size(), limit);
            if (collector.size() == limit) {
                foundAll = true;
            }
        }

        log.debug("Performed {} read iterations, found {} (total) matching entries, foundAll={} ", c, collector.size(), foundAll);
        return foundAll;
    }


    @Override
    public void close() throws IOException {
        closeCurrentFiles();
    }

    private void closeCurrentFiles() throws IOException {

        log.debug("Closing current files");

        if (writeChannel != null) {
            writeChannel.close();
            readChannel.close();
            raf.close();
        }

        if (offsetIndexWriteChannel != null) {
            offsetIndexWriteChannel.close();
            offsetIndexRaf.close();
        }

        if (termIndexWriteChannel != null) {
            termIndexWriteChannel.close();
            termIndexRaf.close();
        }
    }
}
