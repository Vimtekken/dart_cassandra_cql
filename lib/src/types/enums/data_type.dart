part of dart_cassandra_cql.types;

enum DataType {
  custom,
  ascii,
  bigint,
  blob,
  boolean,
  counter,
  decimal,
  double,
  float,
  int,
  text,
  timestamp,
  uuid,
  varchar,
  varint,
  timeuuid,
  inet,
  tinyint,
  list,
  map,
  set,
  udt,
  tuple,
  smallint,
}

extension ByteValuesForDataType on DataType {
  static final RegExp _UUID_REGEX = RegExp(
      r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
      caseSensitive: false);

  static final Map<DataType, int> _byteMap = <DataType, int>{
    DataType.custom: 0x00,
    DataType.ascii: 0x01,
    DataType.bigint: 0x02,
    DataType.blob: 0x03,
    DataType.boolean: 0x04,
    DataType.counter: 0x05,
    DataType.decimal: 0x06,
    DataType.double: 0x07,
    DataType.float: 0x08,
    DataType.int: 0x09,
    DataType.text: 0x0a,
    DataType.timestamp: 0x0b,
    DataType.uuid: 0x0c,
    DataType.varchar: 0x0d,
    DataType.varint: 0x0e,
    DataType.timeuuid: 0x0f,
    DataType.inet: 0x10,
    DataType.tinyint: 0x14,
    DataType.list: 0x20,
    DataType.map: 0x21,
    DataType.set: 0x22,
    DataType.udt: 0x30,
    DataType.tuple: 0x31,
  };

  static final Map<int, DataType> _inverseMap =
      _byteMap.map((key, value) => MapEntry<int, DataType>(value, key));

  bool get isCollection {
    switch (this) {
      case DataType.list:
      case DataType.set:
      case DataType.map:
        return true;
      default:
        return false;
    }
  }

  int toByteValue() {
    if (_byteMap.containsKey(this) && _byteMap[this] != null) {
      return _byteMap[this]!;
    } else {
      throw ArgumentError('Invalid datatype $this');
    }
  }

  // @note This is just to be backwards compatible with old call. May replace with call
  // to toByteValue() instead of .value in the future. Or may prefer access through
  // .value. TBD.
  int get value => toByteValue();

  static DataType fromByteValue(int value) {
    if (_inverseMap.containsKey(value) && _inverseMap[value] != null) {
      return _inverseMap[value]!;
    } else {
      throw ArgumentError('Invalid datatype from bytes $value');
    }
  }

  /// Attempt to guess the correct [DataType] for the given. Returns
  /// the guessed [DataType] or null if type cannot be guessed
  static DataType? guessForValue(Object value) {
    if (value is bool) {
      return DataType.boolean;
    } else if (value is BigInt) {
      return DataType.varint;
    } else if (value is int) {
      int v = value;
      return v.bitLength <= 32
          ? DataType.int
          : v.bitLength <= 64
              ? DataType.bigint
              : DataType.varint;
    } else if (value is num) {
      return DataType.double;
    } else if (value is Uuid ||
        (value is String && _UUID_REGEX.hasMatch(value))) {
      return DataType.uuid;
    } else if (value is String) {
      return DataType.varchar;
    } else if (value is ByteData || value is TypedData) {
      return DataType.blob;
    } else if (value is DateTime) {
      return DataType.timestamp;
    } else if (value is InternetAddress) {
      return DataType.inet;
    } else if (value is Tuple) {
      return DataType.tuple;
    } else if (value is Set) {
      return DataType.set;
    } else if (value is List) {
      return DataType.list;
    } else if (value is Map) {
      return DataType.map;
    }

    return null;
  }
}
