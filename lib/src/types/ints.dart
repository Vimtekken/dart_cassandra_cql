import 'dart:typed_data';

class TinyInt {
  ByteData _data = ByteData(1);

  TinyInt([int value = 0]) {
    data = value;
  }

  int get data => _data.getInt8(0);

  set data(int newValue) => _data.setInt8(0, newValue);

  @override
  bool operator ==(Object other) {
    // Note that the type info is encoded to base64 and does not need to be compared separately
    return other is TinyInt && other.data == data;
  }

  @override
  int get hashCode {
    return data.hashCode;
  }
}

class SmallInt {
  ByteData _data = ByteData(2);

  SmallInt([int value = 0]) {
    data = value;
  }

  int get data => _data.getInt16(0);

  set data(int newValue) => _data.setInt16(0, newValue);

  @override
  bool operator ==(Object other) {
    // Note that the type info is encoded to base64 and does not need to be compared separately
    return other is SmallInt && other.data == data;
  }

  @override
  int get hashCode {
    return data.hashCode;
  }
}
