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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;

public class AvroHDFSFileReader implements AvroReader {
    private final Path filePath;
    private final String url;
    private final String[] requiredFields;
    private final String[] columnTypes;
    private final String defaultFs;
    private DataFileStream<GenericRecord> reader;
    private BufferedInputStream inputStream;
    private Schema schema;

    public AvroHDFSFileReader(String url, String[] requiredFields, String[] columnTypes, String defaultFs) {
        this.url = url;
        this.filePath = new Path(url);
        this.requiredFields = requiredFields;
        this.columnTypes = columnTypes;
        this.defaultFs = defaultFs;
    }

    @Override
    public void open(Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(URI.create(url), conf);
            inputStream = new BufferedInputStream(fs.open(filePath));
            reader = new DataFileStream<>(inputStream, new GenericDatumReader<>());
            schema = reader.getSchema();
        } catch (IOException e) {
            // todo
            throw new RuntimeException(e);
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public Object getNext() throws IOException {
        return reader.next();
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        reader.close();
    }
}
