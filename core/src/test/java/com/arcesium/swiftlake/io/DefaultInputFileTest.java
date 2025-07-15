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
package com.arcesium.swiftlake.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DefaultInputFileTest {

  @ParameterizedTest
  @MethodSource("provideInputFileParameters")
  void testParameterizedConstructorAndGetters(
      String localFileLocation, String location, long length) {
    DefaultInputFile inputFile = new DefaultInputFile(localFileLocation, location, length);

    assertThat(inputFile.getLocalFileLocation()).isEqualTo(localFileLocation);
    assertThat(inputFile.getLocation()).isEqualTo(location);
    assertThat(inputFile.getLength()).isEqualTo(length);
  }

  private static Stream<Arguments> provideInputFileParameters() {
    return Stream.of(
        Arguments.of("/tmp/file1.txt", "s3://bucket/file1.txt", 1024L),
        Arguments.of(null, "s3://bucket/file2.txt", 0L),
        Arguments.of("/tmp/file3.txt", null, 2048L),
        Arguments.of("/tmp/empty.txt", "s3://bucket/empty.txt", 0L));
  }
}
