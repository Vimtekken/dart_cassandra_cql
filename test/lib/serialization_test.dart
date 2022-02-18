library dart_cassandra_cql.tests.serialization;

import "dart:typed_data";
import "dart:io";
import "dart:math";
import 'package:dart_cassandra_cql/src/types/ints.dart';
import "package:test/test.dart";

import 'package:dart_cassandra_cql/src/stream.dart';
import 'package:dart_cassandra_cql/src/types.dart';
import 'mocks/mocks.dart' as mock;
import 'mocks/custom.dart' as custom;

main({bool enableLogger: false}) {
  if (enableLogger) {
    mock.initLogger();
  }

  final custom.CustomJson customJsonInstance = new custom.CustomJson({});

  group("Serialization", () {
    late TypeEncoder encoder;
    late SizeType size;

    group("Exceptions:", () {
      setUp(() {
        encoder = new TypeEncoder(ProtocolVersion.V3);
        size = SizeType.LONG;
      });

      tearDown(() {
        unregisterCodec(customJsonInstance.customTypeClass);
      });

      group("TypeSpec:", () {
        test("Missing key/valueSubTYpe", () {
          expect(
              () => new TypeSpec(DataType.map),
              throwsA(predicate((dynamic e) =>
                  e is ArgumentError &&
                  e.message ==
                      "MAP type should specify TypeSpec instances for both its keys and values")));

          expect(
              () => new TypeSpec(DataType.map,
                  keySubType: new TypeSpec(DataType.ascii)),
              throwsA(predicate((dynamic e) =>
                  e is ArgumentError &&
                  e.message ==
                      "MAP type should specify TypeSpec instances for both its keys and values")));

          expect(
              () => new TypeSpec(DataType.map,
                  valueSubType: new TypeSpec(DataType.ascii)),
              throwsA(predicate((dynamic e) =>
                  e is ArgumentError &&
                  e.message ==
                      "MAP type should specify TypeSpec instances for both its keys and values")));
        });

        test("Missing valueSubType", () {
          expect(
              () => new TypeSpec(DataType.list),
              throwsA(predicate((dynamic e) =>
                  e is ArgumentError &&
                  e.message ==
                      "LIST type should specify a TypeSpec instance for its values")));

          expect(
              () => new TypeSpec(DataType.set),
              throwsA(predicate((dynamic e) =>
                  e is ArgumentError &&
                  e.message ==
                      "SET type should specify a TypeSpec instance for its values")));
        });
      });

      test("Not instance of DateTime", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.timestamp);
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type timestamp to be an instance of DateTime")));
      });

      test("Not instance of Uint8List or CustomType", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.custom);
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type custom to be an instance of Uint8List OR an instance of CustomType with a registered type handler")));

        expect(
            () => encoder.writeTypedValue('test', new Uint16List.fromList([]),
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type custom to be an instance of Uint8List OR an instance of CustomType with a registered type handler")));

        unregisterCodec(customJsonInstance.customTypeClass);
        expect(
            () => encoder.writeTypedValue('test', customJsonInstance,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "No custom type handler codec registered for custom type: ${customJsonInstance.customTypeClass}")));

        type = new TypeSpec(DataType.blob);
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type blob to be an instance of Uint8List")));
      });

      test("Not instance of InternetAddress", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.inet);
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type inet to be an instance of InternetAddress")));
      });

      test("Not instance of Iterable", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.list,
            valueSubType: new TypeSpec(DataType.ascii));
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type list to implement Iterable")));
        type = new TypeSpec(DataType.set,
            valueSubType: new TypeSpec(DataType.ascii));
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type set to implement Iterable")));
      });

      test("Not instance of Map", () {
        Object input = "foo";
        TypeSpec type = new TypeSpec(DataType.map,
            keySubType: new TypeSpec(DataType.ascii),
            valueSubType: new TypeSpec(DataType.ascii));
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type map to implement Map")));

        type = new TypeSpec(DataType.udt);
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type udt to implement Map")));
      });

      test("Not instance of Tuple", () {
        Object input = ["foo"];
        TypeSpec type = new TypeSpec(DataType.tuple);
        expect(
            () => encoder.writeTypedValue('test', input,
                typeSpec: type, size: size),
            throwsA(predicate((dynamic e) =>
                e is ArgumentError &&
                e.message ==
                    "Expected value for field 'test' of type tuple to be an instance of Tuple")));
      });
    });

    group("Internal types:", () {
      setUp(() {
        encoder = new TypeEncoder(ProtocolVersion.V2);
        size = SizeType.SHORT;
      });

      test("Consistency", () {
        Consistency input = Consistency.LOCAL_ONE;
        encoder.writer!.addLast(new Uint8List.fromList([0x00, input.value]));
        Object output = mock.createDecoder(encoder).readConsistency();

        expect(output, equals(input));
      });

      test("String list", () {
        List<String> input = ["a", "foo", "f33dfAce"];
        encoder.writeStringList(input, size);
        Object output = mock.createDecoder(encoder).readStringList(size);
        expect(output, equals(input));
      });

      test("String map", () {
        final input = <String, String>{"foo": "bar", "baz00ka": "f33df4ce"};
        encoder.writeStringMap(input, size);
        Object output = mock.createDecoder(encoder).readStringMap(size);
        expect(output, equals(input));
      });

      test("String multimap", () {
        final input = <String, List<String>>{
          "foo": ["bar", "baz"],
          "baz00ka": ["f33df4ce"]
        };
        encoder.writeStringMultiMap(input, size);
        Object output = mock.createDecoder(encoder).readStringMultiMap(size);
        expect(output, equals(input));
      });
    });

    group("(protocol V2):", () {
      setUp(() {
        encoder = new TypeEncoder(ProtocolVersion.V2);
        size = SizeType.SHORT;
      });

      test("UTF-8 STRING", () {
        Object input = "Test 123 AbC !@#ΤΕΣΤ";
        TypeSpec type = new TypeSpec(DataType.text);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("ASCII STRING", () {
        Object input = "Test 123 AbC";
        TypeSpec type = new TypeSpec(DataType.ascii);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("UUID", () {
        Object input = new Uuid.simple();
        TypeSpec type = new TypeSpec(DataType.uuid);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("TIMEUUID", () {
        Object input = new Uuid.timeBased();
        TypeSpec type = new TypeSpec(DataType.timeuuid);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      group("CUSTOM:", () {
        test("without type handler", () {
          Object input = new Uint8List.fromList(
              new List<int>.generate(10, (int index) => index * 2));
          TypeSpec type = new TypeSpec(DataType.custom)
            ..customTypeClass = customJsonInstance.customTypeClass;
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("with type handler", () {
          // Register custom type handler
          registerCodec('com.achilleasa.cassandra.cqltypes.Json',
              new custom.CustomJsonCodec());

          customJsonInstance.payload = {
            "foo": {"bar": "baz"}
          };

          TypeSpec type = new TypeSpec(DataType.custom)
            ..customTypeClass = customJsonInstance.customTypeClass;

          encoder.writeTypedValue('test', customJsonInstance,
              typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);
          expect(output, new isInstanceOf<custom.CustomJson>());
          expect((output as custom.CustomJson).payload,
              equals(customJsonInstance.payload));
        });
      });

      test("BLOB", () {
        Object input = new Uint8List.fromList(
            new List<int>.generate(10, (int index) => index * 2));
        TypeSpec type = new TypeSpec(DataType.blob);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      group("COUNTER", () {
        test("(positive)", () {
          Object input = 9223372036854775807;
          TypeSpec type = new TypeSpec(DataType.counter);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("(negative)", () {
          Object input = -1;
          TypeSpec type = new TypeSpec(DataType.counter);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      test("TIMESTAMP", () {
        Object input = new DateTime.fromMillisecondsSinceEpoch(1455746327000);
        TypeSpec type = new TypeSpec(DataType.timestamp);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      group("BOOLEAN", () {
        test("(true)", () {
          Object input = true;
          TypeSpec type = new TypeSpec(DataType.boolean);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("(false)", () {
          Object input = false;
          TypeSpec type = new TypeSpec(DataType.boolean);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      group("BOOLEAN", () {
        test("(true)", () {
          Object input = true;
          TypeSpec type = new TypeSpec(DataType.boolean);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("(false)", () {
          Object input = false;
          TypeSpec type = new TypeSpec(DataType.boolean);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      group("INET:", () {
        test("(ipv4)", () {
          Object input = new InternetAddress("192.168.169.101");
          TypeSpec type = new TypeSpec(DataType.inet);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("(ipv6)", () {
          Object input =
              new InternetAddress("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
          TypeSpec type = new TypeSpec(DataType.inet);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      group("NUMBERS:", () {
        group("INT", () {
          test("(positive)", () {
            Object input = 2147483647;
            TypeSpec type = new TypeSpec(DataType.int);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input = -21474836;
            TypeSpec type = new TypeSpec(DataType.int);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });
        });

        group("SMALLINT", () {
          test("(positive)", () {
            SmallInt input = SmallInt(32768);
            TypeSpec type = new TypeSpec(DataType.smallint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            SmallInt input = SmallInt(-32768);
            TypeSpec type = new TypeSpec(DataType.smallint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });
        });

        group("TINYINT", () {
          test("(positive)", () {
            TinyInt input = TinyInt(128);
            TypeSpec type = new TypeSpec(DataType.tinyint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            TinyInt input = TinyInt(-128);
            TypeSpec type = new TypeSpec(DataType.tinyint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });
        });

        group("BIGINT", () {
          test("(positive)", () {
            Object input = 9223372036854775807;
            TypeSpec type = new TypeSpec(DataType.bigint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input = -922036854775807;
            TypeSpec type = new TypeSpec(DataType.bigint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });
        });

        group("FLOAT", () {
          test("(positive)", () {
            Object input = 3.141516;
            TypeSpec type = new TypeSpec(DataType.float);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, closeTo(input as num, 0.000001));
          });

          test("(negative)", () {
            Object input = -3.12345;
            TypeSpec type = new TypeSpec(DataType.float);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, closeTo(input as num, 0.000001));
          });
        });

        group("DOUBLE", () {
          test("(positive)", () {
            Object input = 3.141516;
            TypeSpec type = new TypeSpec(DataType.double);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, closeTo(input as num, 0.000001));
          });

          test("(negative)", () {
            Object input = -3.12345;
            TypeSpec type = new TypeSpec(DataType.double);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, closeTo(input as num, 0.000001));
          });
        });

        group("DECIMAL [fraction digits = ${DECIMAL_FRACTION_DIGITS}]", () {
          test("(positive)", () {
            Object input = 3.123451234512345;
            TypeSpec type = new TypeSpec(DataType.decimal);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output,
                closeTo(input as num, pow(10, -DECIMAL_FRACTION_DIGITS)));
          });

          test("(negative)", () {
            Object input = -3.123451234512345;
            TypeSpec type = new TypeSpec(DataType.decimal);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output,
                closeTo(input as num, pow(10, -DECIMAL_FRACTION_DIGITS)));
          });
        });

        group("VARINT", () {
          test("(positive)", () {
            final input = BigInt.parse('12345678901234567890123');
            TypeSpec type = new TypeSpec(DataType.varint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            final input = BigInt.parse('-987677654324167384628746291873912873');
            TypeSpec type = new TypeSpec(DataType.varint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });
        });
      });

      group("COLLECTIONS:", () {
        test("SET", () {
          Object input = new Set.from([-2, -1, 0, 1, 2]);
          TypeSpec type = new TypeSpec(DataType.set,
              valueSubType: new TypeSpec(DataType.int));
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("LIST", () {
          Object input = [
            new DateTime.fromMillisecondsSinceEpoch(1455746327000),
            new DateTime.fromMillisecondsSinceEpoch(1455746327000)
          ];
          TypeSpec type = new TypeSpec(DataType.list,
              valueSubType: new TypeSpec(DataType.timestamp));
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("MAP", () {
          Object input = {
            "foo": new DateTime.fromMillisecondsSinceEpoch(1455746327000),
            "bar": new DateTime.fromMillisecondsSinceEpoch(1455746327000)
          };
          TypeSpec type = new TypeSpec(DataType.map,
              keySubType: new TypeSpec(DataType.text),
              valueSubType: new TypeSpec(DataType.timestamp));
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });
    });

    group("(protocol V3):", () {
      setUp(() {
        encoder = new TypeEncoder(ProtocolVersion.V3);
        size = SizeType.LONG;
      });
      test("UTF-8 STRING", () {
        Object input = "Test 123 AbC !@#ΤΕΣΤ";
        TypeSpec type = new TypeSpec(DataType.text);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("ASCII STRING", () {
        Object input = "Test 123 AbC";
        TypeSpec type = new TypeSpec(DataType.ascii);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("UUID", () {
        Object input = new Uuid.simple();
        TypeSpec type = new TypeSpec(DataType.uuid);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("TIMEUUID", () {
        Object input = new Uuid.timeBased();
        TypeSpec type = new TypeSpec(DataType.timeuuid);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("CUSTOM", () {
        Object input = new Uint8List.fromList(
            List<int>.generate(10, (int index) => index * 2));
        TypeSpec type = new TypeSpec(DataType.custom);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("BLOB", () {
        Object input = new Uint8List.fromList(
            List<int>.generate(10, (int index) => index * 2));
        TypeSpec type = new TypeSpec(DataType.blob);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      group("COUNTER", () {
        test("(positive)", () {
          Object input = 9223372036854775807;
          TypeSpec type = new TypeSpec(DataType.counter);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("(negative)", () {
          Object input = -1;
          TypeSpec type = new TypeSpec(DataType.counter);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      test("TIMESTAMP", () {
        Object input = new DateTime.fromMillisecondsSinceEpoch(1455746327000);
        TypeSpec type = new TypeSpec(DataType.timestamp);
        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      group("BOOLEAN", () {
        test("(true)", () {
          Object input = true;
          TypeSpec type = new TypeSpec(DataType.boolean);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("(false)", () {
          Object input = false;
          TypeSpec type = new TypeSpec(DataType.boolean);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      group("BOOLEAN", () {
        test("(true)", () {
          Object input = true;
          TypeSpec type = new TypeSpec(DataType.boolean);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("(false)", () {
          Object input = false;
          TypeSpec type = new TypeSpec(DataType.boolean);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      group("INET:", () {
        test("(ipv4)", () {
          Object input = new InternetAddress("192.168.169.101");
          TypeSpec type = new TypeSpec(DataType.inet);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("(ipv6)", () {
          Object input =
              new InternetAddress("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
          TypeSpec type = new TypeSpec(DataType.inet);
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      group("NUMBERS:", () {
        group("INT", () {
          test("(positive)", () {
            Object input = 2147483647;
            TypeSpec type = new TypeSpec(DataType.int);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input = -21474836;
            TypeSpec type = new TypeSpec(DataType.int);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });
        });

        group("BIGINT", () {
          test("(positive)", () {
            Object input = 9223372036854775807;
            TypeSpec type = new TypeSpec(DataType.bigint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input = -922036854775807;
            TypeSpec type = new TypeSpec(DataType.bigint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });
        });

        group("FLOAT", () {
          test("(positive)", () {
            Object input = 3.141516;
            TypeSpec type = new TypeSpec(DataType.float);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, closeTo(input as num, 0.000001));
          });

          test("(negative)", () {
            Object input = -3.12345;
            TypeSpec type = new TypeSpec(DataType.float);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, closeTo(input as num, 0.000001));
          });
        });

        group("DOUBLE", () {
          test("(positive)", () {
            Object input = 3.141516;
            TypeSpec type = new TypeSpec(DataType.double);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, closeTo(input as num, 0.000001));
          });

          test("(negative)", () {
            Object input = -3.12345;
            TypeSpec type = new TypeSpec(DataType.double);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, closeTo(input as num, 0.000001));
          });
        });

        group("DECIMAL [fraction digits = ${DECIMAL_FRACTION_DIGITS}]", () {
          test("(positive)", () {
            Object input = 3.123451234512345;
            TypeSpec type = new TypeSpec(DataType.decimal);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output,
                closeTo(input as num, pow(10, -DECIMAL_FRACTION_DIGITS)));
          });

          test("(negative)", () {
            Object input = -3.123451234512345;
            TypeSpec type = new TypeSpec(DataType.decimal);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output,
                closeTo(input as num, pow(10, -DECIMAL_FRACTION_DIGITS)));
          });
        });

        group("VARINT", () {
          test("(positive)", () {
            Object input = BigInt.parse('12345678901234567890123');
            TypeSpec type = new TypeSpec(DataType.varint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });

          test("(negative)", () {
            Object input =
                BigInt.parse('-987677654324167384628746291873912873');
            TypeSpec type = new TypeSpec(DataType.varint);
            encoder.writeTypedValue('test', input, typeSpec: type, size: size);
            Object? output =
                mock.createDecoder(encoder).readTypedValue(type, size: size);

            expect(output, equals(input));
          });
        });
      });

      group("COLLECTIONS:", () {
        test("SET", () {
          Object input = new Set.from([-2, -1, 0, 1, 2]);
          TypeSpec type = new TypeSpec(DataType.set,
              valueSubType: new TypeSpec(DataType.int));
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("LIST", () {
          Object input = [
            new DateTime.fromMillisecondsSinceEpoch(1455746327000),
            new DateTime.fromMillisecondsSinceEpoch(1455746327000)
          ];
          TypeSpec type = new TypeSpec(DataType.list,
              valueSubType: new TypeSpec(DataType.timestamp));
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });

        test("MAP", () {
          Object input = {
            "foo": new DateTime.fromMillisecondsSinceEpoch(1455746327000),
            "bar": new DateTime.fromMillisecondsSinceEpoch(1455746327000)
          };
          TypeSpec type = new TypeSpec(DataType.map,
              keySubType: new TypeSpec(DataType.text),
              valueSubType: new TypeSpec(DataType.timestamp));
          encoder.writeTypedValue('test', input, typeSpec: type, size: size);
          Object? output =
              mock.createDecoder(encoder).readTypedValue(type, size: size);

          expect(output, equals(input));
        });
      });

      test("UDT (nested)", () {
        Object input = {
          "address": "Elm street",
          "phones": [
            {"prefix": 30, "phone": "123456789"},
            {"prefix": 1, "phone": "800180023"}
          ],
          "tags": {
            "home": {
              "when": new DateTime.fromMillisecondsSinceEpoch(1455746327000),
              "labels": ["red", "green", "blue"]
            }
          }
        };
        TypeSpec intType = new TypeSpec(DataType.int);
        TypeSpec dateType = new TypeSpec(DataType.timestamp);
        TypeSpec stringType = new TypeSpec(DataType.text);
        TypeSpec phoneType = new TypeSpec(DataType.udt)
          ..udtFields["prefix"] = intType
          ..udtFields["phone"] = stringType;
        TypeSpec tagType = new TypeSpec(DataType.udt)
          ..udtFields["when"] = dateType
          ..udtFields["labels"] =
              new TypeSpec(DataType.list, valueSubType: stringType);

        TypeSpec type = new TypeSpec(DataType.udt)
          ..udtFields["address"] = stringType
          ..udtFields["phones"] =
              new TypeSpec(DataType.list, valueSubType: phoneType)
          ..udtFields["tags"] = new TypeSpec(DataType.map,
              keySubType: stringType, valueSubType: tagType);

        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });

      test("TUPLE", () {
        Object input = new Tuple.fromIterable([
          "Test",
          3.14,
          new DateTime.fromMillisecondsSinceEpoch(1455746327000)
        ]);
        TypeSpec type = new TypeSpec(DataType.tuple)
          ..tupleFields.add(new TypeSpec(DataType.text))
          ..tupleFields.add(new TypeSpec(DataType.double))
          ..tupleFields.add(new TypeSpec(DataType.timestamp));

        encoder.writeTypedValue('test', input, typeSpec: type, size: size);
        Object? output =
            mock.createDecoder(encoder).readTypedValue(type, size: size);

        expect(output, equals(input));
      });
    });
  });
}
