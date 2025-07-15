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

import java.util.Arrays;
import org.apache.iceberg.StructLike;

/** A class that implements the StructLike interface using an array to store values. */
public class ArrayStructLike implements StructLike {
  private final Object[] values;

  /**
   * Constructs an ArrayStructLike with the given array of values.
   *
   * @param values The array of values to store.
   */
  public ArrayStructLike(Object[] values) {
    this.values = values;
  }

  /**
   * Constructs an ArrayStructLike from another StructLike object.
   *
   * @param input The StructLike object to copy values from.
   */
  public ArrayStructLike(StructLike input) {
    Object[] inputValues = new Object[input.size()];
    for (int i = 0; i < input.size(); i++) {
      inputValues[i] = input.get(i, Object.class);
    }
    this.values = inputValues;
  }

  /**
   * Returns the number of elements in this struct.
   *
   * @return The number of elements.
   */
  @Override
  public int size() {
    return values.length;
  }

  /**
   * Gets the value at the specified position and casts it to the given Java class.
   *
   * @param pos The position of the value to retrieve.
   * @param javaClass The class to cast the value to.
   * @return The value at the specified position, cast to the given class.
   */
  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(values[pos]);
  }

  /**
   * Sets the value at the specified position.
   *
   * @param pos The position to set the value at.
   * @param value The value to set.
   */
  @Override
  public <T> void set(int pos, T value) {
    values[pos] = value;
  }

  /**
   * Checks if this ArrayStructLike is equal to another object.
   *
   * @param o The object to compare with.
   * @return true if the objects are equal, false otherwise.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof ArrayStructLike)) {
      return false;
    }

    ArrayStructLike that = (ArrayStructLike) o;
    return Arrays.equals(values, that.values);
  }

  /**
   * Computes the hash code for this ArrayStructLike.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }
}
