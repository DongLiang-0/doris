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

public class AvroJNIScannerTest {

    @Test
    public void testGetNext() throws IOException {
        OffHeap.setTesting();
        AvroJNIScanner scanner = new AvroJNIScanner(10, buildParamsKafka());
        scanner.open();
        long metaAddress = 0;
        do {
            metaAddress = scanner.getNextBatchMeta();
            if (metaAddress != 0) {
                long rows = OffHeap.getLong(null, metaAddress);
                VectorTable restoreTable = VectorTable.createReadableTable(scanner.getTable().getColumnTypes(),
                        scanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows));
            }
            scanner.resetTable();
        } while (metaAddress != 0);
        scanner.releaseTable();
        scanner.close();
    }

    private Map<String, String> buildParamsKafka() {
        Map<String, String> requiredParams = new HashMap<>();
        requiredParams.put("is_get_table_schema", "false");
        requiredParams.put("file_type", "6");
        requiredParams.put("topic", "avro-complex7");

        // requiredParams.put("partition", "4");
        // requiredParams.put("start.offset", "100");
        // requiredParams.put("max.rows", "100");

        requiredParams.put("split_size", "4");
        requiredParams.put("split_start_offset", "100");
        requiredParams.put("split_file_size", "10");

        requiredParams.put("broker_list", "10.16.10.6:9092");
        requiredParams.put("group.id", "doris-consumer-group");
        // requiredParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // requiredParams.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        requiredParams.put("schema.registry.url", "http://10.16.10.6:8082");

        requiredParams.put("columns_types", "varchar(65533)#boolean#string#string#string#map<string,bigint>#array<bigint>#double#bigint");
        requiredParams.put("required_fields", "name,boolean_type,favorite_number,favorite_color,enum_value,map_value,array_value,double_type,long_type");
        requiredParams.put("hive.serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        return requiredParams;
    }

}
