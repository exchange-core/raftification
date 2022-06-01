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

import java.nio.file.Path;

public class RaftDiskLogConfig {

    private final Path workFolder;

    private final int journalBufferFlushTrigger = 65536;
    private final long journalFileMaxSize = 2_000_000_000;

    private final int indexRecordEveryNBytes = 1024; // 4096

    private final String exchangeId;

    public RaftDiskLogConfig(Path workFolder, String exchangeId) {
        this.workFolder = workFolder;
        this.exchangeId = exchangeId;
    }


    public Path getWorkFolder() {
        return workFolder;
    }

    public int getJournalBufferFlushTrigger() {
        return journalBufferFlushTrigger;
    }

    public long getJournalFileMaxSize() {
        return journalFileMaxSize;
    }

    public int getIndexRecordEveryNBytes() {
        return indexRecordEveryNBytes;
    }

    public String getExchangeId() {
        return exchangeId;
    }
}
