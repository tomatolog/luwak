package uk.co.flax.luwak;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class Monitor {

    private static final Logger log = LoggerFactory.getLogger(Monitor.class);

    public static final List<MonitorQuery> EMPTY_QUERY_LIST = new ArrayList<>();

    private Directory directory;
    private DirectoryReader reader = null;
    private IndexSearcher searcher = null;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_50, null);

    private final Map<String, MonitorQuery> queries = new HashMap<>();

    public static class FIELDS {
        public static final String id = "id";
        public static final String del_id = "del_id";
    }

    public Monitor() {
        directory = new RAMDirectory();
    }

    public Monitor(List<MonitorQuery> queries) {
        this();
        update(queries);
    }

    public Monitor(MonitorQuery... queries) {
        this();
        update(Arrays.asList(queries));
    }

    private void openSearcher() throws IOException {
        if (reader != null)
            reader.close();
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
    }

    public void reset() {
        lock.writeLock().lock();
        try {
            directory.close();
            directory = new RAMDirectory();
            queries.clear();
        } catch (IOException e) {
            // Shouldn't happen, we're a RAM directory
            throw new RuntimeException(e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void update(List<MonitorQuery> queriesToAdd) {
        update(queriesToAdd, EMPTY_QUERY_LIST);
    }

    public void update(List<MonitorQuery> queriesToAdd, List<MonitorQuery> queriesToDelete) {
        try {
            lock.writeLock().lock();
            log.info("Adding {} queries, deleting {} queries",
                            queriesToAdd.size(), queriesToDelete.size());
            IndexWriter writer = new IndexWriter(directory, iwc);
            for (MonitorQuery mq : queriesToAdd) {
                writer.addDocument(mq.asIndexableDocument());
                queries.put(mq.getId(), mq);
            }
            for (MonitorQuery mq : queriesToDelete) {
                writer.deleteDocuments(mq.getDeletionQuery());
                queries.remove(mq.getId());
            }
            writer.commit();
            writer.close();
            openSearcher();
            log.info("Update completed");
        }
        catch (IOException e) {
            // Shouldn't happen, because we're using a RAMDirectory...
            throw new RuntimeException(e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public DocumentMatches match(final InputDocument document) {
        try {
            lock.readLock().lock();

            long starttime = System.currentTimeMillis(), prebuild, monitor, tick;

            MonitorQueryCollector collector = new MonitorQueryCollector(queries, document);
            Query presearcherQuery = document.getPresearcherQuery();

            tick = System.currentTimeMillis();
            prebuild = tick - starttime;
            starttime = tick;

            searcher.search(presearcherQuery, collector);

            monitor = System.currentTimeMillis() - starttime;

            return collector.getMatches(prebuild, monitor);

        }
        catch (IOException e) {
            // Shouldn't happen, because we're using a RAMDirectory
            throw new RuntimeException(e);
        }
        finally {
            lock.readLock().unlock();
        }
    }

}
