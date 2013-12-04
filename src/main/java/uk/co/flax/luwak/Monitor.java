package uk.co.flax.luwak;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import uk.co.flax.luwak.util.WhitelistTokenFilter;

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

/**
 * A Monitor matches {@link uk.co.flax.luwak.InputDocument}s to registered
 * {@link uk.co.flax.luwak.MonitorQuery} objects.
 */
public class Monitor {

    public static final List<MonitorQuery> EMPTY_QUERY_LIST = new ArrayList<>();

    private Directory directory;
    private DirectoryReader reader = null;
    private IndexSearcher searcher = null;
    private SetMultimap<String, String> terms = null;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_50, null);

    private final Map<String, MonitorQuery> queries = new HashMap<>();

    private final Presearcher presearcher;

    public static class FIELDS {
        public static final String id = "id";
        public static final String del_id = "del_id";
    }

    public Monitor(Presearcher presearcher) {
        directory = new RAMDirectory();
        this.presearcher = presearcher;
        presearcher.setMonitor(this);
    }

    private void openSearcher() throws IOException {
        if (reader != null)
            reader.close();
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        buildTokenSet();
    }

    /**
     * Remove all currently registered queries from the monitor
     */
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

    /**
     * Register new queries with the monitor
     * @param queriesToAdd an {@link Iterable} of {@link MonitorQuery} objects
     */
    public void update(Iterable<? extends MonitorQuery> queriesToAdd) {
        update(queriesToAdd, EMPTY_QUERY_LIST);
    }

    /**
     * Register new queries with the monitor
     * @param queries an array of {@link MonitorQuery} objects
     */
    public void update(MonitorQuery... queries) {
        update(Arrays.asList(queries));
    }

    /**
     * Register new queries and delete existing queries from the the monitor
     * @param queriesToAdd an {@link Iterable} of {@link MonitorQuery} objects to add
     * @param queriesToDelete an {@link Iterable} of {@link MonitorQuery} objects to remove
     */
    public void update(Iterable<? extends MonitorQuery> queriesToAdd, Iterable<? extends MonitorQuery> queriesToDelete) {
        try {
            lock.writeLock().lock();
            IndexWriterConfig iwc = this.iwc.clone();
            IndexWriter writer = new IndexWriter(directory, iwc);
            for (MonitorQuery mq : queriesToAdd) {
                try {
                    writer.addDocument(mq.asIndexableDocument(presearcher));
                }
                catch (Exception e) {
                    throw new RuntimeException("Couldn't index query " + mq.getId() + " [" + mq.getQuery() + "]", e);
                }
                queries.put(mq.getId(), mq);
            }
            for (MonitorQuery mq : queriesToDelete) {
                writer.deleteDocuments(mq.getDeletionQuery());
                queries.remove(mq.getId());
            }
            writer.commit();
            writer.close();
            openSearcher();
        }
        catch (IOException e) {
            // Shouldn't happen, because we're using a RAMDirectory...
            throw new RuntimeException(e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Match an {@link InputDocument} against the queries registered in this monitor
     * @param document the Document to match
     * @return a {@link DocumentMatches} object describing the queries that matched
     */
    public DocumentMatches match(final InputDocument document) {
        try {
            lock.readLock().lock();

            if (searcher == null)
                throw new IllegalStateException("Monitor has no registered queries!");

            long starttime = System.currentTimeMillis(), prebuild, monitor, tick;

            MonitorQueryCollector collector = new MonitorQueryCollector(queries, document);
            Query presearcherQuery = presearcher.buildQuery(document);

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

    /**
     * Get the query registered under a specific ID, or null if there is no corresponding query
     * @param queryId the id of the MonitorQuery
     * @return the query with the passed in ID
     */
    public MonitorQuery getQuery(String queryId) {
        return queries.get(queryId);
    }

    /**
     * Get the number of queries registered in the monitor
     * @return the number of queries registered in the monitor
     */
    public long getQueryCount() {
        return queries.size();
    }

    /**
     * Create a new {@link TokenStream} that removes any tokens in its input that are not present
     * in the monitor's internal index.  Used by {@link Presearcher#buildQuery(InputDocument)} to
     * trim the BooleanQuery created from an InputDocument.
     * @param field the field this TokenStream is for
     * @param ts the input TokenStream
     * @return a filtered TokenStream
     */
    public TokenStream filterTokenStream(String field, TokenStream ts) {
        return new WhitelistTokenFilter(ts, terms.get(field));
    }

    private void buildTokenSet() throws IOException {
        SetMultimap<String, String> terms = HashMultimap.create();
        BytesRef termBytes;
        for (AtomicReaderContext ctx : reader.leaves()) {
            for (String field : ctx.reader().fields()) {
                TermsEnum te = ctx.reader().terms(field).iterator(null);
                while ((termBytes = te.next()) != null) {
                    terms.put(field, termBytes.utf8ToString());
                }
            }
        }
        this.terms = terms;
    }

}
