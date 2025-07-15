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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MultipleInputFilesTest {

  @Mock private InputFiles inputFiles1;

  @Mock private InputFiles inputFiles2;

  @Mock private InputFile inputFile1;

  @Mock private InputFile inputFile2;

  @Mock private InputFile inputFile3;

  @Test
  void testConstructorAndGetInputFiles() {
    when(inputFiles1.getInputFiles()).thenReturn(Arrays.asList(inputFile1, inputFile2));
    when(inputFiles2.getInputFiles()).thenReturn(Collections.singletonList(inputFile3));

    MultipleInputFiles multipleInputFiles =
        new MultipleInputFiles(Arrays.asList(inputFiles1, inputFiles2));

    List<InputFile> result = multipleInputFiles.getInputFiles();

    assertThat(result).hasSize(3).containsExactly(inputFile1, inputFile2, inputFile3);
  }

  @Test
  void testConstructorWithEmptyList() {
    MultipleInputFiles multipleInputFiles = new MultipleInputFiles(Collections.emptyList());

    List<InputFile> result = multipleInputFiles.getInputFiles();

    assertThat(result).isEmpty();
  }

  @Test
  void testConstructorWithNullList() {
    assertThatThrownBy(() -> new MultipleInputFiles(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void testClose() throws Exception {
    MultipleInputFiles multipleInputFiles =
        new MultipleInputFiles(Arrays.asList(inputFiles1, inputFiles2));

    multipleInputFiles.close();

    verify(inputFiles1, times(1)).close();
    verify(inputFiles2, times(1)).close();
  }

  @Test
  void testCloseWithException() throws Exception {
    doThrow(new Exception("Test exception")).when(inputFiles1).close();

    MultipleInputFiles multipleInputFiles =
        new MultipleInputFiles(Arrays.asList(inputFiles1, inputFiles2));

    assertThatThrownBy(() -> multipleInputFiles.close())
        .isInstanceOf(SwiftLakeException.class)
        .hasMessageContaining("An error occurred while closing resource.")
        .hasCauseInstanceOf(Exception.class);

    verify(inputFiles1, times(1)).close();
    verify(inputFiles2, never()).close();
  }

  @Test
  void testConstructorWithDuplicateInputFiles() {
    when(inputFiles1.getInputFiles()).thenReturn(Arrays.asList(inputFile1, inputFile2));
    when(inputFiles2.getInputFiles()).thenReturn(Arrays.asList(inputFile2, inputFile3));

    MultipleInputFiles multipleInputFiles =
        new MultipleInputFiles(Arrays.asList(inputFiles1, inputFiles2));

    List<InputFile> result = multipleInputFiles.getInputFiles();

    assertThat(result).hasSize(4).containsExactly(inputFile1, inputFile2, inputFile2, inputFile3);
  }
}
