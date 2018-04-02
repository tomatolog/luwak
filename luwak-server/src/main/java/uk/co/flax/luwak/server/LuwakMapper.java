package uk.co.flax.luwak.server;
/*
 *   Copyright (c) 2017 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import uk.co.flax.luwak.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuwakMapper extends SimpleModule {

    public static final Logger logger = LoggerFactory.getLogger(LuwakMapper.class);

    public static ObjectMapper addMappings(ObjectMapper in) {
        in.registerModule(INSTANCE);
        return in;
    }

    private static final LuwakMapper INSTANCE = new LuwakMapper();

    private LuwakMapper() {
        super("LuwakMapper");
        addDeserializer(MonitorQuery.class, new MonitorQueryDeserializer());
        addDeserializer(InputDocument.class, new InputDocumentDeserializer());
        addDeserializer(DocumentBatch.class, new InputDocumentBatchDeserializer());
        addSerializer(Matches.class, new MatchesSerializer());
    }

    private static class MatchesSerializer extends JsonSerializer<Matches> {

        // TODO: stats, errors, more or less everything...

        @Override
        public void serialize(Matches dm, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
            Matches<QueryMatch> documentMatches = (Matches<QueryMatch>) dm;

            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("took", documentMatches.getSearchTime());
            jsonGenerator.writeNumberField("took_query_build", documentMatches.getQueryBuildTime());
            jsonGenerator.writeNumberField("took_search", documentMatches.getSearchTime());
            jsonGenerator.writeNumberField("took_total", documentMatches.getQueryBuildTime() + documentMatches.getSearchTime());
            jsonGenerator.writeFieldName("hits");
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("hits");
            jsonGenerator.writeStartArray();
            for (DocumentMatches<QueryMatch> doc : documentMatches) {
                jsonGenerator.writeStartObject();
                jsonGenerator.writeStringField("doc", doc.getDocId());
                jsonGenerator.writeFieldName("matches");
                jsonGenerator.writeStartArray();
                for (QueryMatch qm : doc) {
                    jsonGenerator.writeString(qm.getQueryId());
                }
                jsonGenerator.writeEndArray();
                jsonGenerator.writeEndObject();
            }
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndObject();
        }
    }

    private static class InputDocumentDeserializer extends JsonDeserializer<InputDocument> {

        @Override
        public InputDocument deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String id = null;
            Map<String, String> fields = Collections.emptyMap();

            Analyzer analyzer = new StandardAnalyzer();
            
            InputDocument doc = null;

            JsonNode node = jsonParser.readValueAsTree();
            id = node.get("id").asText();

            JsonNode nodeFields = node.get("fields");
            JsonNode nodeQuery = node.get("query");
            if (nodeFields!=null) {
                fields = readMap(nodeFields);
                doc = readDoc(id, fields, analyzer);
            } else if (nodeQuery!=null) {
                fields = readMap(nodeQuery.get("percolate").get("document"));
                doc = readDoc(id, fields, analyzer);
            }

            logger.info("id: {}, doc: {}, {}", id, fields, doc);
            return doc;
        }
    }

    private static class InputDocumentBatchDeserializer extends JsonDeserializer<DocumentBatch> {

        @Override
        public DocumentBatch deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            DocumentBatch.Builder builder = new DocumentBatch.Builder();
            Analyzer analyzer = new StandardAnalyzer();

            JsonNode node = jsonParser.readValueAsTree();
            Integer id = node.get("id").asInt();

            ArrayList<InputDocument> docs = new ArrayList<InputDocument>();

            JsonNode nodeDocs = node.get("query").get("percolate").get("documents");
            Iterator<JsonNode> nodeDoc = nodeDocs.iterator();
            while (nodeDoc.hasNext()) {
                Map<String, String> fields = readMap(nodeDoc.next());
                InputDocument doc = readDoc(Integer.toString(id), fields, analyzer);
                docs.add(doc);
                id++;
                logger.info("{}", fields);
            }

            logger.info("id: {}, docs: {}", id, docs.size());

            builder.addAll(docs);
            return builder.build();
        }
    }

    private static class MonitorQueryDeserializer extends JsonDeserializer<MonitorQuery> {
        @Override
        public MonitorQuery deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String id = null;
            String query = null;
            Map<String, String> metadata = Collections.emptyMap();

            JsonNode node = jsonParser.readValueAsTree();
            id = node.get("id").asText();
            if (node.get("query").isObject()) {
                query = node.get("query").get("ql").asText();
            } else {
                query = node.get("query").asText();
            }

            return new MonitorQuery(id, query, metadata);
        }

    }

    private static Map<String, String> readMap(JsonNode node) throws IOException {
        Map<String, String> metadata = new HashMap<>();

        Iterator<Map.Entry<String, JsonNode>> children = node.fields();
        while (children.hasNext()) {
            Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) children.next();
            metadata.put(entry.getKey(), entry.getValue().asText());
        }        
        return metadata;
    }

    private static InputDocument readDoc(String id, Map<String, String> fields, Analyzer analyzer) {
        InputDocument.Builder builder = new InputDocument.Builder(id);
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            builder.addField(entry.getKey(), entry.getValue(), analyzer);
        }
        return builder.build();
    }
}
