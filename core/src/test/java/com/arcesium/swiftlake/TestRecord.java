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
package com.arcesium.swiftlake;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

class TestRecord {
  private Long id;
  private String name;
  private String category;
  private LocalDate date;
  private Integer intValue;
  private Long longValue;
  private Float floatValue;
  private Double doubleValue;
  private BigDecimal decimalValue;
  private Boolean booleanValue;
  private LocalDateTime timestampValue;
  private OffsetDateTime offsetDateTimeValue;
  private LocalTime localTimeValue;
  private NestedStruct structValue;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public LocalDate getDate() {
    return date;
  }

  public void setDate(LocalDate date) {
    this.date = date;
  }

  public Integer getIntValue() {
    return intValue;
  }

  public void setIntValue(Integer intValue) {
    this.intValue = intValue;
  }

  public Long getLongValue() {
    return longValue;
  }

  public void setLongValue(Long longValue) {
    this.longValue = longValue;
  }

  public Float getFloatValue() {
    return floatValue;
  }

  public void setFloatValue(Float floatValue) {
    this.floatValue = floatValue;
  }

  public Double getDoubleValue() {
    return doubleValue;
  }

  public void setDoubleValue(Double doubleValue) {
    this.doubleValue = doubleValue;
  }

  public BigDecimal getDecimalValue() {
    return decimalValue;
  }

  public void setDecimalValue(BigDecimal decimalValue) {
    this.decimalValue = decimalValue;
  }

  public Boolean getBooleanValue() {
    return booleanValue;
  }

  public void setBooleanValue(Boolean booleanValue) {
    this.booleanValue = booleanValue;
  }

  public LocalDateTime getTimestampValue() {
    return timestampValue;
  }

  public void setTimestampValue(LocalDateTime timestampValue) {
    this.timestampValue = timestampValue;
  }

  public OffsetDateTime getOffsetDateTimeValue() {
    return offsetDateTimeValue;
  }

  public void setOffsetDateTimeValue(OffsetDateTime offsetDateTimeValue) {
    this.offsetDateTimeValue = offsetDateTimeValue;
  }

  public LocalTime getLocalTimeValue() {
    return localTimeValue;
  }

  public void setLocalTimeValue(LocalTime localTimeValue) {
    this.localTimeValue = localTimeValue;
  }

  public NestedStruct getStructValue() {
    return structValue;
  }

  public void setStructValue(NestedStruct structValue) {
    this.structValue = structValue;
  }

  public static class NestedStruct {
    private Integer intValue;
    private String stringValue;

    public Integer getIntValue() {
      return intValue;
    }

    public void setIntValue(Integer intValue) {
      this.intValue = intValue;
    }

    public String getStringValue() {
      return stringValue;
    }

    public void setStringValue(String stringValue) {
      this.stringValue = stringValue;
    }
  }
}
