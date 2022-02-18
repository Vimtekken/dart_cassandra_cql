library dart_cassandra_cql.tests.type_guess;

import "dart:typed_data";
import "dart:io";
import "package:test/test.dart";

import 'package:dart_cassandra_cql/src/types.dart';
import 'mocks/mocks.dart' as mock;
import 'mocks/custom.dart' as custom;

main({bool enableLogger: false}) {
  if (enableLogger) {
    mock.initLogger();
  }

  group("Collection type:", () {
    test("isCollection(LIST)", () {
      expect(DataType.set.isCollection, isTrue);
    });
    test("isCollection(SET)", () {
      expect(DataType.list.isCollection, isTrue);
    });
    test("isCollection(MAP)", () {
      expect(DataType.map.isCollection, isTrue);
    });
    test("isCollection(TUPLE)", () {
      expect(DataType.tuple.isCollection, isFalse);
    });
  });

  group("TypeSpec.toString():", () {
    test("ASCII", () {
      TypeSpec ts = new TypeSpec(DataType.ascii);
      expect(ts.toString(), equals("ascii"));
    });

    test("CUSTOM", () {
      custom.CustomJson customJson = new custom.CustomJson({});
      TypeSpec ts = new TypeSpec(DataType.custom)
        ..customTypeClass = customJson.customTypeClass;
      expect(
          ts.toString(), equals("CustomType<${customJson.customTypeClass}>"));
    });

    test("LIST", () {
      TypeSpec ts = new TypeSpec(DataType.list,
          valueSubType: new TypeSpec(DataType.inet));
      expect(ts.toString(), equals("List<inet>"));
    });

    test("SET", () {
      TypeSpec ts = new TypeSpec(DataType.set,
          valueSubType: new TypeSpec(DataType.timestamp));
      expect(ts.toString(), equals("Set<timestamp>"));
    });

    test("MAP", () {
      TypeSpec ts = new TypeSpec(DataType.map,
          keySubType: new TypeSpec(DataType.timestamp),
          valueSubType: new TypeSpec(DataType.int));
      expect(ts.toString(), equals("Map<timestamp, int>"));
    });

    test("UDT", () {
      TypeSpec ts = new TypeSpec(DataType.udt)
        ..keyspace = "test"
        ..udtName = "phone"
        ..udtFields = {
          "tags": new TypeSpec(DataType.list,
              valueSubType: new TypeSpec(DataType.ascii))
        };
      expect(ts.toString(), equals('{test.phone: {tags: List<ascii>}}'));
    });

    test("TUPLE", () {
      TypeSpec ts = new TypeSpec(DataType.tuple)
        ..tupleFields = [
          new TypeSpec(DataType.int),
          new TypeSpec(DataType.ascii),
          new TypeSpec(DataType.timestamp)
        ];
      expect(ts.toString(), equals('([int, ascii, timestamp])'));
    });
  });

  group("Type guess:", () {
    test("BOOL", () {
      expect(ByteValuesForDataType.guessForValue(true), equals(DataType.boolean));
      expect(ByteValuesForDataType.guessForValue(false), equals(DataType.boolean));
    });

    test("DOUBLE", () {
      expect(ByteValuesForDataType.guessForValue(3.145), equals(DataType.double));
    });

    test("INT", () {
      expect(ByteValuesForDataType.guessForValue(3), equals(DataType.int));
    });

    test("BIGINT", () {
      expect(
          ByteValuesForDataType.guessForValue(9223372036854775807), equals(DataType.bigint));
    });

    test("VARINT", () {
      expect(ByteValuesForDataType.guessForValue(BigInt.parse('9223372036854775807000000')),
          equals(DataType.varint));
    });

    test("VARCHAR", () {
      expect(ByteValuesForDataType.guessForValue("test123 123"), equals(DataType.varchar));
    });

    test("UUID", () {
      expect(ByteValuesForDataType.guessForValue(new Uuid.simple()), equals(DataType.uuid));

      expect(
          ByteValuesForDataType.guessForValue(new Uuid.timeBased()), equals(DataType.uuid));

      expect(ByteValuesForDataType.guessForValue(new Uuid.timeBased().toString()),
          equals(DataType.uuid));
    });

    test("BLOB", () {
      expect(ByteValuesForDataType.guessForValue(new Uint8List.fromList([0xff])),
          equals(DataType.blob));
    });

    test("TIMESTAMP", () {
      expect(ByteValuesForDataType.guessForValue(new DateTime.now()),
          equals(DataType.timestamp));
    });

    test("INET", () {
      expect(ByteValuesForDataType.guessForValue(new InternetAddress("127.0.0.1")),
          equals(DataType.inet));
    });

    test("LIST", () {
      expect(ByteValuesForDataType.guessForValue(["test123 123", 1, 2, 3.14]),
          equals(DataType.list));
    });

    test("SET", () {
      expect(ByteValuesForDataType.guessForValue(new Set.from(["a", "a", "b"])),
          equals(DataType.set));
    });

    test("MAP", () {
      expect(ByteValuesForDataType.guessForValue({"foo": "bar"}), equals(DataType.map));
    });

    test("TUPLE", () {
      expect(ByteValuesForDataType.guessForValue(new Tuple.fromIterable([1, 2, 3])),
          equals(DataType.tuple));
    });

    test("No guess", () {
      expect(ByteValuesForDataType.guessForValue(new SocketException("foo")), isNull);
    });
  });
}
