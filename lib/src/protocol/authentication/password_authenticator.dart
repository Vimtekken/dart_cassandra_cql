part of dart_cassandra_cql.protocol;

class PasswordAuthenticator implements Authenticator {
  String? _userName;
  String? _password;

  /// Create a new [BasicAuthenticator] with the specified [userName] and [password]
  PasswordAuthenticator(String userName, String password) {
    _userName = userName;
    _password = password;

    if (_userName == null || _userName!.isEmpty) {
      throw ArgumentError("Username cannot be empty");
    }
    if (_password == null || _password!.isEmpty) {
      throw ArgumentError("Password cannot be empty");
    }
  }

  /// Get the class of this authenticator
  String get authenticatorClass =>
      "org.apache.cassandra.auth.PasswordAuthenticator";

  /// Process the [challenge] sent by the server and return a [Uint8List] response
  Uint8List answerChallenge(Uint8List? challenge) {
    ChunkedOutputWriter writer = ChunkedOutputWriter();

    Uint8List separator = Uint8List.fromList(List<int>.from([0]));

    // Write user and password separated by a NULL byte
    writer.addLast(separator);
    writer.addLast(utf8.encode(_userName!) as Uint8List?);
    writer.addLast(separator);
    writer.addLast(utf8.encode(_password!) as Uint8List?);

    return writer.joinChunks();
  }
}
