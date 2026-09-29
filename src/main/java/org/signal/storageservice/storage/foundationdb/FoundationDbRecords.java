/*
 * Copyright 2026 BRS Holding (SWARM) - FoundationDB storage backend
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.storage.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/// SWARM: how one record (what is one Bigtable row) is laid out in FoundationDB. See docs/SWARM-CHANGES.md,
/// section 3.4.
///
/// A record is every key under `subspace.pack(prefix)`. It has named columns, each a tuple suffix after the prefix:
///
/// - a *plain* column is one key, `prefix + column`, whose value is the column's bytes (a version number as decimal
///   text; always far below the value size limit);
/// - a *chunked* column is the keys `prefix + column + (0)`, `prefix + column + (1)`, ..., whose values are
///   consecutive pieces of at most [#MAX_VALUE_BYTES] bytes of the column's bytes. An empty value is one empty chunk.
///
/// Plain columns end with a string element, chunked columns with the integer chunk index, so a key can always be
/// told apart. Storage items use one chunked column with an empty name: `(uuid, item key, chunk)`.
public final class FoundationDbRecords {

  /// FoundationDB refuses values larger than 100,000 bytes.
  public static final int MAX_VALUE_BYTES = 100_000;

  private FoundationDbRecords() {
  }

  /// The columns of one record, keyed by their tuple suffix.
  public record Columns(Map<Tuple, byte[]> plain, Map<Tuple, byte[]> chunked) {

    public static Columns empty() {
      return new Columns(Collections.emptyMap(), Collections.emptyMap());
    }

    public boolean isEmpty() {
      return plain.isEmpty() && chunked.isEmpty();
    }

    public byte[] plain(final String column) {
      return plain.get(Tuple.from(column));
    }

    public byte[] chunked(final String column) {
      return chunked.get(Tuple.from(column));
    }

    /// @return whether both records hold exactly the same columns with exactly the same bytes
    public boolean sameContentAs(final Columns other) {
      return sameMaps(plain, other.plain) && sameMaps(chunked, other.chunked);
    }

    private static boolean sameMaps(final Map<Tuple, byte[]> a, final Map<Tuple, byte[]> b) {
      if (!a.keySet().equals(b.keySet())) {
        return false;
      }
      for (final Map.Entry<Tuple, byte[]> entry : a.entrySet()) {
        if (!Arrays.equals(entry.getValue(), b.get(entry.getKey()))) {
          return false;
        }
      }
      return true;
    }
  }

  /// A builder for the columns of a record that is about to be written.
  public static final class ColumnsBuilder {

    private final Map<Tuple, byte[]> plain = new TreeMap<>();
    private final Map<Tuple, byte[]> chunked = new TreeMap<>();

    public ColumnsBuilder plain(final String column, final byte[] value) {
      plain.put(Tuple.from(column), value);
      return this;
    }

    public ColumnsBuilder chunked(final String column, final byte[] value) {
      chunked.put(Tuple.from(column), value);
      return this;
    }

    public ColumnsBuilder chunked(final Tuple column, final byte[] value) {
      chunked.put(column, value);
      return this;
    }

    public Columns build() {
      return new Columns(Collections.unmodifiableMap(plain), Collections.unmodifiableMap(chunked));
    }
  }

  public static ColumnsBuilder columns() {
    return new ColumnsBuilder();
  }

  /// Replaces the record under `prefix`: clears every key of the record, then writes the given columns.
  public static void write(final Transaction transaction, final Subspace subspace, final Tuple prefix,
      final Columns columns) {

    transaction.clear(subspace.range(prefix));

    columns.plain().forEach((column, value) -> transaction.set(subspace.pack(prefix.addAll(column)), value));
    columns.chunked().forEach((column, value) -> {
      final Tuple chunkPrefix = prefix.addAll(column);
      int chunk = 0;
      int offset = 0;
      do {
        final int length = Math.min(MAX_VALUE_BYTES, value.length - offset);
        transaction.set(subspace.pack(chunkPrefix.add(chunk)), Arrays.copyOfRange(value, offset, offset + length));
        chunk++;
        offset += length;
      } while (offset < value.length);
    });
  }

  /// Deletes every key of the record under `prefix`.
  public static void clear(final Transaction transaction, final Subspace subspace, final Tuple prefix) {
    transaction.clear(subspace.range(prefix));
  }

  /// Reads the record under `prefix` in one range read.
  public static CompletableFuture<Columns> read(final ReadTransaction transaction, final Subspace subspace,
      final Tuple prefix) {

    return transaction.getRange(subspace.range(prefix), ReadTransaction.ROW_LIMIT_UNLIMITED, false,
            StreamingMode.WANT_ALL)
        .asList()
        .thenApply(keyValues -> parse(subspace, prefix.size(), keyValues));
  }

  /// Reads only the first chunk of a chunked column: enough to tell whether the column holds any bytes.
  ///
  /// @return `true` if the column exists and is not empty (an empty column is one empty chunk)
  public static CompletableFuture<Boolean> hasNonEmptyChunkedColumn(final ReadTransaction transaction,
      final Subspace subspace, final Tuple prefix, final String column) {

    return transaction.getRange(subspace.range(prefix.add(column)), 1).asList()
        .thenApply(keyValues -> !keyValues.isEmpty() && keyValues.getFirst().getValue().length > 0);
  }

  /// Turns the key-values of one record, in key order, into its columns.
  ///
  /// @param prefixSize the number of tuple elements of the record's prefix
  public static Columns parse(final Subspace subspace, final int prefixSize, final List<KeyValue> keyValues) {
    if (keyValues.isEmpty()) {
      return Columns.empty();
    }

    final Map<Tuple, byte[]> plain = new TreeMap<>();
    final Map<Tuple, ByteArrayOutputStream> chunked = new TreeMap<>();
    final Map<Tuple, Long> nextChunk = new TreeMap<>();

    for (final KeyValue keyValue : keyValues) {
      final List<Object> items = subspace.unpack(keyValue.getKey()).getItems();
      final List<Object> suffix = items.subList(Math.min(prefixSize, items.size()), items.size());
      addKeyValue(plain, chunked, nextChunk, suffix, keyValue);
    }

    final Map<Tuple, byte[]> chunkedBytes = new TreeMap<>();
    chunked.forEach((column, bytes) -> chunkedBytes.put(column, bytes.toByteArray()));

    return new Columns(Collections.unmodifiableMap(plain), Collections.unmodifiableMap(chunkedBytes));
  }

  private static void addKeyValue(final Map<Tuple, byte[]> plain, final Map<Tuple, ByteArrayOutputStream> chunked,
      final Map<Tuple, Long> nextChunk, final List<Object> suffix, final KeyValue keyValue) {

    if (suffix.isEmpty()) {
      throw new IllegalStateException("unexpected key at a record prefix");
    }

    final Object last = suffix.getLast();
    if (last instanceof Long chunk) {
      final Tuple column = Tuple.fromList(suffix.subList(0, suffix.size() - 1));
      final long expected = nextChunk.getOrDefault(column, 0L);
      if (chunk != expected) {
        throw new IllegalStateException("chunk " + chunk + " of column " + column + " follows chunk " + (expected - 1));
      }
      nextChunk.put(column, expected + 1);
      chunked.computeIfAbsent(column, ignored -> new ByteArrayOutputStream()).writeBytes(keyValue.getValue());
    } else {
      plain.put(Tuple.fromList(suffix), keyValue.getValue());
    }
  }
}
