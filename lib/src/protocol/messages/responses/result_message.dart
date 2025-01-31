part of dart_cassandra_cql.protocol;

abstract class ResultMessage extends Message {
  ResultMetadata? metadata;
  List<Map<String, Object?>> rows = <Map<String, Object?>>[];

  ResultMessage() : super(Opcode.RESULT);

  factory ResultMessage.parse(TypeDecoder decoder) {
    // Read message type
    ResultType type = ResultType.valueOf(decoder.readUInt());
    switch (type) {
      case ResultType.VOID:
        return VoidResultMessage();
      case ResultType.ROWS:
        //decoder.dumpToFile("frame-response-rows.dump");
        return RowsResultMessage.parse(decoder);
      case ResultType.SET_KEYSPACE:
        return SetKeyspaceResultMessage.parse(decoder);
      case ResultType.PREPARED:
        //decoder.dumpToFile("frame-response-prepared.dump");
        return PreparedResultMessage.parse(decoder);
      case ResultType.SCHEMA_CHANGE:
        return SchemaChangeResultMessage.parse(decoder);
    }
    throw new UnimplementedError('Unknown type: $type');
  }

  ResultMetadata _parseMetadata(TypeDecoder decoder) {
    ResultMetadata metadata = ResultMetadata();

    int flags = metadata.flags = decoder.readUInt();
    bool globalTableSpec = (flags & RowResultFlag.GLOBAL_TABLE_SPEC.value) ==
        RowResultFlag.GLOBAL_TABLE_SPEC.value;
    bool hasMorePages = (flags & RowResultFlag.HAS_MORE_PAGES.value) ==
        RowResultFlag.HAS_MORE_PAGES.value;
    //bool noMetadata = (flags & RowResultFlag.NO_METADATA.value) == RowResultFlag.NO_METADATA.value;
    int colCount = decoder.readUInt();

    // Parse paging state
    if (hasMorePages) {
      metadata.pagingState = decoder.readBytes(SizeType.LONG);
    }

    // Skip over global table spec (<keyspace><table name>)
    if (globalTableSpec) {
      decoder.skipString(SizeType.SHORT);
      decoder.skipString(SizeType.SHORT);
    }

    // Parse column specs
    metadata.colSpec = LinkedHashMap<String, TypeSpec>();
    for (int colIndex = colCount; colIndex > 0; colIndex--) {
      // Skip over col-specific table spec (<keyspace><table name>)
      if (!globalTableSpec) {
        decoder.skipString(SizeType.SHORT);
        decoder.skipString(SizeType.SHORT);
      }

      // Parse column name and type
      final String? columnName = decoder.readString(SizeType.SHORT);
      if (columnName != null) {
        metadata.colSpec![columnName] = decoder.readTypeOption();
      }
    }

    return metadata;
  }
}
