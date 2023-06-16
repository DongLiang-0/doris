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

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.VectorTable;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestAvroJNIScanner {

    private final Map<String, String> params = new HashMap<>();

    @Test
    public void testHDFSFileReader() throws IOException {
        buildHDFSParams();
        startAvroJNIScanner();
    }

    @Test
    public void testS3Reader() throws IOException {
        buildS3Params();
        startAvroJNIScanner();
    }

    @Test
    public void testTableSchema() throws IOException {
        buildHDFSTableParams();
        AvroJNIScanner avroScanner = new AvroJNIScanner(0, params);
        avroScanner.open();
        String tableSchema = avroScanner.getTableSchema();
        System.out.println(tableSchema);
    }

    private void buildHDFSTableParams() {
        params.put("file_type", "4"); // FILE_HDFS
        params.put("uri", "hdfs://127.0.0.1:9000/input/person2.avro");
        params.put("is_get_table_schema", "true");
    }

    @Test
    public void testS3ReaderToGetTableSchema() throws IOException {
        buildS3Params();
        OffHeap.setTesting();
        AvroJNIScanner avroScanner = new AvroJNIScanner(0, params);
        avroScanner.open();
        String tableSchema = avroScanner.getTableSchema();
        System.out.println(tableSchema);
    }

    private void startAvroJNIScanner() throws IOException {
        OffHeap.setTesting();
        AvroJNIScanner avroScanner = new AvroJNIScanner(1, createScanTestParams());
        avroScanner.open();

        long metaAddress = 0;
        do {
            metaAddress = avroScanner.getNextBatchMeta();
            if (metaAddress != 0) {
                long rows = OffHeap.getLong(null, metaAddress);
                VectorTable restoreTable = new VectorTable(avroScanner.getTable().getColumnTypes(),
                        avroScanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows));
            }
            avroScanner.resetTable();
        } while (metaAddress != 0);
        avroScanner.releaseTable();
        avroScanner.close();
    }

    private Map<String, String> createScanTestParams() {
        params.put("required_fields", "name,boolean_type,double_type,long_type");
        params.put("columns_types", "string#boolean#double#bigint");

        // params.put("required_fields", "name,map_value,array_value,boolean_type,double_type,long_type");
        // params.put("columns_types", "string#map<string,bigint>#array<string>#boolean#double#bigint");
        params.put("hive.serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        return params;
    }

    private void buildS3Params() {
        params.put("file_type", "3"); // FILE_S3
        params.put("s3.access_key", "AKIDpTwEF92zIMLC2iHbeQuGOJzQcJjsnUv6");
        params.put("s3.endpoint", "cos.ap-guangzhou.myqcloud.com");
        params.put("s3.region", "ap-guangzhou");
        params.put("s3.secret_key", "1h9LH0LRGvMZWFo9GAqGXYLwHQM5Z1Tn");
        params.put("s3.virtual.bucket", "zyk-gz-1316291683");
        params.put("s3.virtual.key", "path/person3.avro");
    }

    private void buildHDFSParams() {
        params.put("file_type", "4"); // FILE_HDFS
        params.put("uri", "hdfs://127.0.0.1:9000/input/person.avro");
        // params.put("uri", "hdfs://127.0.0.1:9000/input/person_complex.avro");
    }

}
