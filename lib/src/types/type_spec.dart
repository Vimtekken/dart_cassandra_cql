part of dart_cassandra_cql.types;

class TypeSpec {
  DataType valueType;
  TypeSpec? keySubType;
  TypeSpec? valueSubType;

  // Custom type
  String? customTypeClass;

  // V3 protocol: UDT
  String? keyspace;
  String? udtName;
  late Map<String, TypeSpec> udtFields;

  // V3 protocol: TUPLE
  late List<TypeSpec> tupleFields;

  TypeSpec(DataType this.valueType,
      {TypeSpec? this.keySubType, TypeSpec? this.valueSubType}) {
    if (valueType == DataType.list &&
        (valueSubType == null || valueSubType is! TypeSpec)) {
      throw ArgumentError(
          "LIST type should specify a TypeSpec instance for its values");
    } else if (valueType == DataType.set &&
        (valueSubType == null || valueSubType is! TypeSpec)) {
      throw ArgumentError(
          "SET type should specify a TypeSpec instance for its values");
    } else if (valueType == DataType.map &&
        (keySubType == null ||
            keySubType is! TypeSpec ||
            valueSubType == null ||
            valueSubType is! TypeSpec)) {
      throw ArgumentError(
          "MAP type should specify TypeSpec instances for both its keys and values");
    } else if (valueType == DataType.udt) {
      udtFields = LinkedHashMap<String, TypeSpec>();
    } else if (valueType == DataType.tuple) {
      tupleFields = [];
    }
  }

  String toString() {
    switch (valueType) {
      case DataType.custom:
        return "CustomType<${customTypeClass}>";
      case DataType.map:
        return "Map<${keySubType}, ${valueSubType}>";
      case DataType.list:
        return "List<${valueSubType}>";
      case DataType.set:
        return "Set<${valueSubType}>";
      case DataType.udt:
        return "{${keyspace}.${udtName}: ${udtFields}}";
      case DataType.tuple:
        return "(${tupleFields})";
      default:
        return valueType.name;
    }
  }
}
