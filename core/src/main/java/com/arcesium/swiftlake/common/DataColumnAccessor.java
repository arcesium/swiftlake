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

import java.util.function.Function;
import org.apache.iceberg.types.Type;

/**
 * A class that provides access to data columns with a specific name, type, and value accessor.
 *
 * @param <T> The type of object from which the column value is accessed.
 */
public class DataColumnAccessor<T> {
  private final String name;
  private final Type type;
  private final Function<T, Object> valueAccessor;

  /**
   * Private constructor to create a DataColumnAccessor.
   *
   * @param name The name of the data column.
   * @param type The type of the data column.
   * @param valueAccessor The function to access the column value from an object.
   */
  private DataColumnAccessor(String name, Type type, Function<T, Object> valueAccessor) {
    this.name = name;
    this.type = type;
    this.valueAccessor = valueAccessor;
  }

  /**
   * Creates a new DataColumnAccessor.
   *
   * @param name The name of the data column.
   * @param type The type of the data column.
   * @param valueFunction The function to access the column value from an object.
   * @param <T> The type of object from which the column value is accessed.
   * @return A new DataColumnAccessor instance.
   */
  public static <T> DataColumnAccessor<T> of(
      String name, Type type, Function<T, Object> valueFunction) {
    return new DataColumnAccessor<>(name, type, valueFunction);
  }

  /**
   * Gets the name of the data column.
   *
   * @return The name of the data column.
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the type of the data column.
   *
   * @return The Type of the data column.
   */
  public Type getType() {
    return type;
  }

  /**
   * Gets the function used to access the column value from an object.
   *
   * @return The value accessor function.
   */
  public Function<T, Object> getValueAccessor() {
    return valueAccessor;
  }

  /**
   * Retrieves the column value from the given object.
   *
   * @param object The object from which to retrieve the column value.
   * @return The column value.
   */
  public Object getValue(T object) {
    return valueAccessor.apply(object);
  }
}
