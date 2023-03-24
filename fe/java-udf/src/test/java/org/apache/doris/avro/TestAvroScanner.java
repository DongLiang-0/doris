package org.apache.doris.avro;

import org.apache.doris.jni.utils.OffHeap;
import org.apache.doris.jni.vec.VectorTable;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestAvroScanner {

    private Map<String, String> createScanTestParams() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "name,favorite_number,favorite_color,boolean_type,double_type,long_type");
        params.put("columns_types", "string#int#string#boolean#double#bigint");
//        params.put("uri", "hdfs://127.0.0.1:9000/input/users.avro");
        params.put("uri", "hdfs://127.0.0.1:9000/input/person.avro");
        params.put("fs.defaultFS", "hdfs://127.0.0.1:9000");
        params.put("serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");

        return params;
    }

    @Test
    public void testAvroScanner() throws IOException {
        OffHeap.setTesting();
        AvroScanner avroScanner = new AvroScanner(1, createScanTestParams());
        avroScanner.open();

        long metaAddress = 0;
        do{
            metaAddress = avroScanner.getNextBatchMeta();
            if (metaAddress !=0){
                long rows = OffHeap.getLong(null, metaAddress);
                VectorTable restoreTable = new VectorTable(avroScanner.getTable().getColumnTypes(), avroScanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows));
            }
            avroScanner.resetTable();
        }while (metaAddress !=0);
        avroScanner.releaseTable();
        avroScanner.close();
    }

}
