library dart_cassandra_cql.tests.chunked_input_reader;

import 'dart:typed_data';

import "package:test/test.dart";

import 'package:dart_cassandra_cql/src/stream.dart';

main({bool enableLogger: false}) {
  group("Chunked input reader:", () {
    test("add chunks", () {
      ChunkedInputReader reader = new ChunkedInputReader();
      reader.add(Uint8List.fromList([1, 2, 3]));
      reader.add(Uint8List.fromList([4]));
      expect(reader.length, equals(4));
    });

    test("peek next byte", () {
      ChunkedInputReader reader = new ChunkedInputReader();
      reader.add(Uint8List.fromList([1, 2, 3]));
      reader.add(Uint8List.fromList([4]));
      expect(reader.peekNextByte(), equals(1));
    });

    test("clear", () {
      ChunkedInputReader reader = new ChunkedInputReader();
      reader.add(Uint8List.fromList([1, 2, 3]));
      reader.add(Uint8List.fromList([4]));
      expect(reader.length, equals(4));
      reader.clear();
      expect(reader.length, equals(0));
    });

    test("read", () {
      ChunkedInputReader reader = new ChunkedInputReader();
      reader.add(Uint8List.fromList([1, 2, 3]));
      reader.add(Uint8List.fromList([4]));

      List<int?> buffer = List.filled(3, null);
      expect(reader.length, equals(4));
      reader.read(buffer, 3);
      expect(reader.length, equals(1));
      expect(buffer, equals([1, 2, 3]));

      reader.read(buffer, 1);
      expect(reader.length, equals(0));
      expect(buffer, equals([4, 2, 3]));
    });

    test("skip", () {
      ChunkedInputReader reader = new ChunkedInputReader();
      reader.add(Uint8List.fromList([1, 2, 3]));
      reader.add(Uint8List.fromList([4, 5]));

      expect(reader.length, equals(5));
      reader.skip(1);
      expect(reader.length, equals(4));

      reader.skip(2);
      expect(reader.length, equals(2));

      List<int?> buffer = List.filled(2, null);
      reader.read(buffer, 2);
      expect(reader.length, equals(0));
      expect(buffer, equals([4, 5]));
    });
  });
}
