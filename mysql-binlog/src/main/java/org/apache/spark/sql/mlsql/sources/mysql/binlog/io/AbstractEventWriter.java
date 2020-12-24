package org.apache.spark.sql.mlsql.sources.mysql.binlog.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.spark.sql.mlsql.sources.mysql.binlog.RawBinlogEvent;
import org.apache.spark.sql.mlsql.sources.mysql.binlog.TableInfo;

import java.io.IOException;
import java.io.StringWriter;

public abstract class AbstractEventWriter {

    private final JsonFactory JSON_FACTORY = new JsonFactory();
    protected JsonGenerator jsonGenerator;

    // Common method to create a JSON generator and start the root object. Should be called by sub-classes unless they need their own generator and such.
    protected void startJson(StringWriter outputStream, RawBinlogEvent event) throws IOException {
        jsonGenerator = createJsonGenerator(outputStream);
        jsonGenerator.writeStartObject();
        writeJsonField("type",event.getEventType());
        writeJsonField("timestamp",event.getTimestamp());
        String bingLogFileName = event.getBinlogFilename();
        if (bingLogFileName != null) {
            String[] prefixAndIndex = bingLogFileName.split("\\.");
            writeJsonField("bingLogNamePrefix",prefixAndIndex[0]);
            if (prefixAndIndex.length == 2){
                writeJsonField("binlogIndex",prefixAndIndex[1]);
            }
        }
        writeJsonField("binlogFileOffset",event.getPos());
        TableInfo tableInfo = event.getTableInfo();
        if (tableInfo != null) {
            writeJsonField("databaseName",tableInfo.getDatabaseName());
            writeJsonField("tableName",tableInfo.getTableName());
            writeJsonField("schema",tableInfo.getSchema());
        }
    }

    private <T> void writeJsonField(String fieldName, T fieldValue) throws IOException {
        if (fieldValue == null) {
            jsonGenerator.writeNullField(fieldName);
        } else {
            if (fieldValue instanceof Long) {
                jsonGenerator.writeNumberField(fieldName, (Long)fieldValue);
            } else {
                jsonGenerator.writeStringField(fieldName, (String)fieldValue);
            }
        }
    }

    protected void endJson() throws IOException {
        if (jsonGenerator == null) {
            throw new IOException("endJson called without a JsonGenerator");
        }
        jsonGenerator.writeEndObject();
        jsonGenerator.flush();
        jsonGenerator.close();
    }

    private JsonGenerator createJsonGenerator(StringWriter out) throws IOException {
        return JSON_FACTORY.createGenerator(out);
    }

    public abstract java.util.List<String> writeEvent(RawBinlogEvent event)  throws IOException;
}
