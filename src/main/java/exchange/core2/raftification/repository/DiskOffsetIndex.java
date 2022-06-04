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

import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
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

public final class DiskOffsetIndex {

    private static final Logger log = LoggerFactory.getLogger(DiskOffsetIndex.class);

    private final RaftDiskLogConfig config;

    private RandomAccessFile offsetIndexRaf;
    private FileChannel offsetIndexChannel;

    private final ByteBuffer offsetIndexWriteBuffer = ByteBuffer.allocateDirect(64);

    private long lastIndexWrittenAt = 0;


    // index -> position in file // TODO keep term in the index?
    // TODO implement multi-file index
    private NavigableMap<Long, Long> offsetIndex = new TreeMap<>();

    public DiskOffsetIndex(final Path offsetIndexFileName, RaftDiskLogConfig config) throws IOException {


        this.config = config;

        log.debug("Reading offset index file: {} ...", offsetIndexFileName);

        this.offsetIndexRaf = new RandomAccessFile(offsetIndexFileName.toString(), "rwd");
        this.offsetIndexChannel = offsetIndexRaf.getChannel();

        final InputStream is = Channels.newInputStream(offsetIndexChannel);
        final BufferedInputStream bis = new BufferedInputStream(is);
        final DataInputStream dis = new DataInputStream(bis);

        while (dis.available() != 0) {

            // read index record (16 bytes)
            final long idx = dis.readLong();
            final long offset = dis.readLong();

            log.debug("  index {} -> offset {}", idx, offset);
            offsetIndex.put(idx, offset);

            lastIndexWrittenAt = offset;
        }

        log.debug("lastIndexWrittenAt={}", lastIndexWrittenAt);

    }

    /**
     * @return offset+index to start looking from
     */
    public LongLongPair findStartingIndexPoint(long indexFrom) {
        final Map.Entry<Long, Long> entry = offsetIndex.floorEntry(indexFrom);
        final long floorIndex = (entry == null) ? 1L : entry.getKey();
        final long startOffset = (entry == null) ? 0L : entry.getValue();
        return PrimitiveTuples.pair(startOffset, floorIndex);
    }

    public LongLongPair findLastIndexRecord() {
        final Map.Entry<Long, Long> lastIndexEntry = offsetIndex.lastEntry();
        final long floorIndex = (lastIndexEntry == null) ? 1L : lastIndexEntry.getKey();
        final long startOffset = (lastIndexEntry == null) ? 0L : lastIndexEntry.getValue();
        return PrimitiveTuples.pair(startOffset, floorIndex);
    }

    public void appendIndex(long writtenBytes, long nextIndex) throws IOException {

        if (writtenBytes > lastIndexWrittenAt + config.getIndexRecordEveryNBytes()) {

            lastIndexWrittenAt = writtenBytes;

            log.debug("Adding index record:{}->{}", nextIndex, writtenBytes);

            offsetIndex.put(nextIndex, writtenBytes);

            offsetIndexWriteBuffer.putLong(nextIndex);
            offsetIndexWriteBuffer.putLong(writtenBytes);
            offsetIndexWriteBuffer.flip();
            offsetIndexChannel.write(offsetIndexWriteBuffer);
            offsetIndexWriteBuffer.clear();

            log.debug("lastIndexWrittenAt={}", lastIndexWrittenAt);
        }
    }

    public void truncateStart(long removeAfter) {
        // remove invalid index records
        log.debug("offsetIndex before: {}", offsetIndex);
        offsetIndex.tailMap(removeAfter, false).clear();
        log.debug("offsetIndex after: {}", offsetIndex);

        // updating offset where last index was written
        final Map.Entry<Long, Long> lastIndexEntry = offsetIndex.lastEntry();
        lastIndexWrittenAt = (lastIndexEntry == null) ? 0L : lastIndexEntry.getValue();
        log.debug("lastIndexWrittenAt new: {}", lastIndexWrittenAt);
    }

    public void truncateFinalize() {
        final int newOffsetIndexSize = offsetIndex.size() * 16;
        log.debug("performing offset index truncation: {} bytes ...", newOffsetIndexSize);
        try {
            offsetIndexChannel.position(newOffsetIndexSize);
            offsetIndexChannel.truncate(newOffsetIndexSize);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void closeCurrentFiles() throws IOException {
        if (offsetIndexChannel != null) {
            offsetIndexChannel.close();
            offsetIndexRaf.close();
        }
    }
}
