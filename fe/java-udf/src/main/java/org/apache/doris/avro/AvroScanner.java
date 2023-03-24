// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.avro;

import org.apache.doris.jni.JniScanner;
import org.apache.doris.jni.vec.ColumnType;
import org.apache.doris.jni.vec.ColumnValue;
import org.apache.doris.jni.vec.ScanPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import static org.apache.doris.avro.AvroConstants.DEFAULT_FS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class AvroScanner extends JniScanner {

    private final Integer fetchSize;
    private final String[] columnTypes;
    private final String[] requiredFields;
    private int[] requiredColumnIds;
    private final String serde;
    private ColumnType[] requiredTypes;
    private final AvroReader avroReader;
    private StructObjectInspector objectInspector;
    private Deserializer deserializer;
    private StructField[] structFields;
    private ObjectInspector[] fieldInspectors;
    private final Configuration conf;
    private final String defaultFs;
    private Properties properties;
    private Schema schema;


    public AvroScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.columnTypes = params.get("columns_types").split("#");
        this.requiredFields = params.get("required_fields").split(",");
        String uri = params.get("uri");
        defaultFs = params.get(DEFAULT_FS);
        this.structFields = new StructField[requiredFields.length];
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.serde = params.get("serde");
        this.conf = new Configuration();
        this.properties = new Properties();
        this.requiredColumnIds = new int[requiredFields.length];
        this.avroReader = new AvroHDFSFileReader(uri, requiredFields, columnTypes, defaultFs);
    }

    private void buildParams() {
        requiredTypes = new ColumnType[requiredFields.length];
        requiredColumnIds = new int[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            requiredColumnIds[i] = i;
            requiredTypes[i] = ColumnType.parseType(requiredFields[i], columnTypes[i]);
        }

//        properties.setProperty("hive.io.file.readcolumn.ids", Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf).collect(Collectors.joining(",")));
//        properties.setProperty("hive.io.file.readcolumn.names", String.join(",", this.requiredFields));
//        properties.setProperty("serialization.lib", this.serde);
//        properties.setProperty("avro.schema.namespace", schema.getNamespace());
//        properties.setProperty("avro.schema.name", schema.getName());
//        properties.setProperty("avro.schema.doc", schema.getDoc());

//        StringBuilder sb = new StringBuilder();
//        for (Field field : schema.getFields()) {
//            sb.append(field)
//        }
        properties.setProperty("columns", String.join(",", requiredFields));
        properties.setProperty("columns.types", String.join(",", columnTypes));
    }

    @Override
    public void open() throws IOException {
        conf.set(DEFAULT_FS, defaultFs);

        this.avroReader.open(conf);
        this.schema = avroReader.getSchema();
        buildParams();


        initializeDeserializer();

        initTableInfo(requiredTypes, requiredFields, new ScanPredicate[0], fetchSize);
    }

    private void initializeDeserializer() {
        try {
            this.deserializer = getDeserializer(conf, properties, serde);
            this.objectInspector = getTableObjectInspector(deserializer);
        } catch (Exception e) {
            //todo
            throw new RuntimeException(e);
        }
        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = objectInspector.getStructFieldRef(requiredFields[i]);
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
        }
    }

    @Override
    public void close() throws IOException {
        avroReader.close();
    }

    @Override
    protected int getNext() throws IOException {
        int numRows = 0;
        for (; numRows < getBatchSize(); numRows++) {
            if (!avroReader.hasNext()) {
                break;
            }
//            Object rowRecord = avroReader.getNext();
            GenericRecord rowRecord = (GenericRecord) avroReader.getNext();
            for (int i = 0; i < requiredFields.length; i++) {
//                Object fieldData = objectInspector.getStructFieldData(rowRecord, structFields[i]);
                Object fieldData = rowRecord.get(requiredFields[i]);
                if (fieldData == null) {
                    appendData(i, null);
                } else {
                    ColumnValue fieldValue = new AvroColumnValue(fieldInspectors[i], fieldData);
                    appendData(i, fieldValue);
                }
            }
        }
        return numRows;
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer) throws Exception {
        ObjectInspector inspector = deserializer.getObjectInspector();
        checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT,
                "expected STRUCT: %s", inspector.getCategory());
        return (StructObjectInspector) inspector;
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name) throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(name, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

}
