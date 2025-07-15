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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility class for file operations. */
public class FileUtil {
  private static final Pattern DATA_SIZE_PATTERN =
      Pattern.compile("^([.0-9]+)(\\s)?([a-zA-Z]+)$", Pattern.CASE_INSENSITIVE);
  public static final long KB_FACTOR = 1000;
  public static final long KIB_FACTOR = 1024;
  public static final long MB_FACTOR = 1000 * KB_FACTOR;
  public static final long MIB_FACTOR = 1024 * KIB_FACTOR;
  public static final long GB_FACTOR = 1000 * MB_FACTOR;
  public static final long GIB_FACTOR = 1024 * MIB_FACTOR;
  public static final long TB_FACTOR = 1000 * GB_FACTOR;
  public static final long TIB_FACTOR = 1024 * GIB_FACTOR;

  /**
   * Checks if a given folder is empty.
   *
   * @param folder The path to the folder to check.
   * @return true if the folder is empty, false otherwise.
   * @throws SwiftLakeException if an error occurs while checking the folder.
   */
  public static boolean isEmptyFolder(String folder) {
    Path path = Path.of(folder);
    if (Files.isDirectory(path)) {
      try (Stream<Path> entries = Files.list(path)) {
        return !entries.findFirst().isPresent();
      } catch (IOException e) {
        throw new SwiftLakeException(
            e, "An error occurred while checking if a folder is empty or not.");
      }
    }

    return false;
  }

  /**
   * Gets a list of file paths in a given folder.
   *
   * @param folder The path to the folder to list.
   * @return A List of String containing absolute paths of files in the folder.
   * @throws SwiftLakeException if an error occurs while listing the folder.
   */
  public static List<String> getFilePathsInFolder(String folder) {
    Path path = Path.of(folder);
    if (Files.isDirectory(path)) {
      try (Stream<Path> entries = Files.list(path)) {
        return entries.map(p -> p.toAbsolutePath().toString()).collect(Collectors.toList());
      } catch (IOException e) {
        throw new SwiftLakeException(e, "An error occurred while listing the folder %s", folder);
      }
    }
    return new ArrayList<>();
  }

  /**
   * Gets a list of File objects in a given folder.
   *
   * @param folder The path to the folder to list.
   * @return A List of File objects in the folder.
   */
  public static List<File> getFilesInFolder(String folder) {
    return getFilesInFolder(folder, 1);
  }

  /**
   * Recursive helper method to get files in a folder up to a certain depth.
   *
   * @param folder The path to the folder to list.
   * @param level The current depth level.
   * @return A List of File objects in the folder and its subfolders.
   * @throws SwiftLakeException if an error occurs while listing the folder.
   */
  private static List<File> getFilesInFolder(String folder, int level) {
    Path path = Path.of(folder);
    List<File> files = new ArrayList<>();
    if (Files.isDirectory(path)) {
      try (Stream<Path> entries = Files.list(path)) {
        entries.forEach(
            e -> {
              if (Files.isDirectory(e)) {
                if (level <= 10) {
                  files.addAll(getFilesInFolder(e.toAbsolutePath().toString(), level + 1));
                }
              } else {
                files.add(e.toFile());
              }
            });
      } catch (IOException e) {
        throw new SwiftLakeException(e, "An error occurred while listing the folder %s", folder);
      }
    }
    return files;
  }

  /**
   * Parses a data size string and converts it to bytes.
   *
   * @param dataSize The data size string to parse (e.g., "5MB", "2.5GiB").
   * @return The size in bytes.
   * @throws ValidationException if the data size string is invalid or cannot be parsed.
   */
  public static long parseDataSizeString(String dataSize) {
    Matcher matcher = DATA_SIZE_PATTERN.matcher(dataSize);
    if (!matcher.matches()) {
      throw new ValidationException("Could not parse data size string: %s", dataSize);
    }
    double value = Double.parseDouble(matcher.group(1).trim());
    String unit = matcher.group(3).trim();

    return switch (unit) {
      case "byte", "bytes" -> (long) value;
      case "KB" -> (long) (value * KB_FACTOR);
      case "KiB" -> (long) (value * KIB_FACTOR);
      case "MB" -> (long) (value * MB_FACTOR);
      case "MiB" -> (long) (value * MIB_FACTOR);
      case "GB" -> (long) (value * GB_FACTOR);
      case "GiB" -> (long) (value * GIB_FACTOR);
      case "TB" -> (long) (value * TB_FACTOR);
      case "TiB" -> (long) (value * TIB_FACTOR);
      default -> throw new ValidationException("Invalid size unit %s", unit);
    };
  }

  /**
   * Removes the trailing slash from a path if present.
   *
   * @param path The path to process.
   * @return The path without a trailing slash.
   */
  public static String stripTrailingSlash(String path) {
    return normalizePath(path, false, true);
  }

  /**
   * Normalizes a path by removing leading and trailing slashes.
   *
   * @param path The path to normalize.
   * @return The normalized path.
   */
  public static String normalizePath(String path) {
    return normalizePath(path, true, true);
  }

  /**
   * Normalizes a path by optionally removing leading and/or trailing slashes.
   *
   * @param path The path to normalize.
   * @param beginning Whether to remove leading slash.
   * @param ending Whether to remove trailing slash.
   * @return The normalized path.
   */
  public static String normalizePath(String path, boolean beginning, boolean ending) {
    String normalized = path.trim();
    if (beginning && normalized.startsWith("/")) normalized = normalized.substring(1);
    if (ending && normalized.endsWith("/"))
      normalized = normalized.substring(0, normalized.length() - 1);
    return normalized;
  }

  /**
   * Extracts the scheme from a given path.
   *
   * @param path The path to extract the scheme from.
   * @return The scheme if present, null otherwise.
   */
  public static String getScheme(String path) {
    int colon = path.indexOf(':');
    int slash = path.indexOf('/');
    if ((colon != -1) && ((slash == -1) || (colon < slash))) {
      return path.substring(0, colon);
    }
    return null;
  }

  /**
   * Concatenates multiple path components into a single path string.
   *
   * @param basePath The base path to start with.
   * @param otherParts Variable number of additional path components to be concatenated.
   * @return A string representing the concatenated path.
   *     <p>This method normalizes each path component and joins them with "/" separators. It skips
   *     null or blank path components. If no additional parts are provided, it returns the
   *     normalized base path.
   */
  public static String concatPaths(final String basePath, final String... otherParts) {
    if (otherParts == null || otherParts.length == 0) return basePath;
    StringJoiner fullPath = new StringJoiner("/");
    String normalizedBase = normalizePath(basePath, false, true);
    if (!normalizedBase.isBlank()) fullPath.add(normalizedBase);

    for (final String part : otherParts) {
      if (part == null || part.isBlank()) continue;

      String normalizedPart = normalizePath(part);
      if (!normalizedPart.isBlank()) fullPath.add(normalizedPart);
    }
    return fullPath.toString();
  }
}
