part of dart_cassandra_cql.types;

/// A simple typed wrapper over a standard list
/// so we can distinguish between tuples and iterables
/// during serialization
class Tuple extends _collection.DelegatingList<Object?> {
  Tuple.fromIterable(Iterable<Object?> iterable)
      : super(iterable as List<Object?>);
}
