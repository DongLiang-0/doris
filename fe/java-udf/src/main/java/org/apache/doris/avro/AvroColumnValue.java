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

import org.apache.doris.jni.vec.ColumnValue;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class AvroColumnValue implements ColumnValue {

//    private final byte[] valueBytes;
//    private final ColumnType columnType;

    private final Object fieldData;
    private final ObjectInspector fieldInspector;

    public AvroColumnValue(ObjectInspector fieldInspector, Object fieldData) {
//        this.valueBytes = valueBytes;
//        this.columnType = columnType;
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
    }

    //    private byte[] extendByteArray(byte[] input) {
//        int length = input.length;
//        int newLength = Math.max(length, 4);
//        byte[] output = new byte[newLength];
//        for (int i = 0; i < newLength - length; i++) {
//            output[i] = 0;
//        }
//        System.arraycopy(input, 0, output, newLength - length, length);
//        return output;
//    }

    //    private Object inspectObject() {
//        byte[] bytes = extendByteArray(valueBytes);
//        ByteBuffer byteBuffer = ByteBuffer.wrap(TypeNativeBytes.convertByteOrder(Arrays.copyOf(bytes, bytes.length)));
//        switch (columnType.getType()) {
//            case BOOLEAN:
//                return byteBuffer.get() == 1;
//            case TINYINT:
//                return byteBuffer.get();
//            case SMALLINT:
//                return byteBuffer.getShort();
//            case INT:
//                return byteBuffer.getInt();
//            case BIGINT:
//                return byteBuffer.getLong();
//            case FLOAT:
//                return byteBuffer.getFloat();
//            case DOUBLE:
//                return byteBuffer.getDouble();
//            case DECIMALV2:
//            case DECIMAL32:
//            case DECIMAL64:
//            case DECIMAL128:
//                return TypeNativeBytes.getDecimal(Arrays.copyOf(valueBytes, valueBytes.length), columnType.getScale());
//            case CHAR:
//            case VARCHAR:
//            case STRING:
//                return new String(valueBytes, StandardCharsets.UTF_8);
//            case BINARY:
//                return valueBytes;
//            default:
//                return new Object();
//        }
//    }

    private Object inspectObject() {
        return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
    }

    @Override
    public boolean getBoolean() {
        return (boolean) inspectObject();
    }

    @Override
    public byte getByte() {
        return (byte) inspectObject();
    }

    @Override
    public short getShort() {
        return (short) inspectObject();
    }

    @Override
    public int getInt() {
        return (int) inspectObject();
    }

    @Override
    public float getFloat() {
        return (float) inspectObject();
    }

    @Override
    public long getLong() {
        return (long) inspectObject();
    }

    @Override
    public double getDouble() {
        return (double) inspectObject();
    }

    @Override
    public BigDecimal getDecimal() {
        return (BigDecimal) inspectObject();
    }

    @Override
    public String getString() {
        return inspectObject().toString();
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.now();
    }

    @Override
    public LocalDateTime getDateTime() {
        return LocalDateTime.now();
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) inspectObject();
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {

    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {

    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {

    }
}
