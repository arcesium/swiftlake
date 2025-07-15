/*
 * Copyright (c) 2025, Arcesium LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arcesium.swiftlake.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetTypeVisitor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

/** Utility class for working with Parquet files and schemas. */
public class ParquetUtil {

  /**
   * Creates a ParquetFileReader for the given InputFile.
   *
   * @param inputFile The InputFile to read from
   * @return A ParquetFileReader instance
   * @throws IOException If an I/O error occurs
   */
  public static ParquetFileReader getParquetFileReader(InputFile inputFile) throws IOException {
    return ParquetFileReader.open(file(inputFile));
  }

  /**
   * Checks if the given Parquet schema contains any null IDs.
   *
   * @param parquetSchema The Parquet schema to check
   * @return true if the schema contains any null IDs, false otherwise
   */
  public static boolean hasNullIds(MessageType parquetSchema) {
    return ParquetTypeVisitor.visit(parquetSchema, new HasNullIds());
  }

  /**
   * Converts an Iceberg InputFile to a Parquet InputFile.
   *
   * @param file The Iceberg InputFile to convert
   * @return A Parquet InputFile
   */
  private static org.apache.parquet.io.InputFile file(org.apache.iceberg.io.InputFile file) {
    if (file instanceof HadoopInputFile hadoopInputFile) {
      try {
        return org.apache.parquet.hadoop.util.HadoopInputFile.fromStatus(
            hadoopInputFile.getStat(), hadoopInputFile.getConf());
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create Parquet input file for %s", file);
      }
    }
    return new ParquetInputFile(file);
  }

  /** Private inner class for checking null IDs in Parquet schemas. */
  private static class HasNullIds extends ParquetTypeVisitor<Boolean> {
    @Override
    public Boolean message(MessageType message, List<Boolean> fields) {
      return hasNullIds(fields);
    }

    @Override
    public Boolean struct(GroupType struct, List<Boolean> hasNullIds) {
      return struct.getId() == null || hasNullIds(hasNullIds);
    }

    private Boolean hasNullIds(List<Boolean> hasNullIds) {
      for (Boolean hasNullId : hasNullIds) {
        if (hasNullId) {
          return true;
        }
      }
      return false;
    }

    @Override
    public Boolean list(GroupType array, Boolean hasNullId) {
      return hasNullId || array.getId() == null;
    }

    @Override
    public Boolean map(GroupType map, Boolean keyHasNullId, Boolean valueHasNullId) {
      return keyHasNullId || valueHasNullId || map.getId() == null;
    }

    @Override
    public Boolean primitive(PrimitiveType primitive) {
      return primitive.getId() == null;
    }
  }

  /** Private inner class for adapting Iceberg InputFile to Parquet InputFile. */
  private static class ParquetInputFile implements org.apache.parquet.io.InputFile {
    private final org.apache.iceberg.io.InputFile file;

    private ParquetInputFile(org.apache.iceberg.io.InputFile file) {
      this.file = file;
    }

    @Override
    public long getLength() throws IOException {
      return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      org.apache.iceberg.io.SeekableInputStream stream = file.newStream();
      if (stream instanceof DelegatingInputStream delegatingInputStream) {
        InputStream wrapped = delegatingInputStream.getDelegate();
        if (wrapped instanceof FSDataInputStream fsDataInputStream) {
          return HadoopStreams.wrap(fsDataInputStream);
        }
      }
      return new ParquetInputStreamAdapter(stream);
    }
  }

  /**
   * Private inner class for adapting Iceberg SeekableInputStream to Parquet SeekableInputStream.
   */
  private static class ParquetInputStreamAdapter extends DelegatingSeekableInputStream {
    private final org.apache.iceberg.io.SeekableInputStream delegate;

    private ParquetInputStreamAdapter(org.apache.iceberg.io.SeekableInputStream delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }
  }
}
