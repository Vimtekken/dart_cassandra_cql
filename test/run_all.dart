library dart_cassandra_cql.tests;

import "lib/enum_test.dart" as enums;
import "lib/query_test.dart" as query;
import "lib/chunked_input_reader_test.dart" as chunkedInputReader;
import "lib/serialization_test.dart" as serialization;
import "lib/connection_test.dart" as connection;
import "lib/type_test.dart" as typeTest;
import "lib/pool_config_test.dart" as poolConfig;
import "lib/client_test.dart" as client;

void main() {
  // Check if we need to disable our loggers
  bool enableLogger = false;

  enums.main(enableLogger: enableLogger);
  chunkedInputReader.main(enableLogger: enableLogger);
  serialization.main(enableLogger: enableLogger);
  connection.main(enableLogger: enableLogger);
  typeTest.main(enableLogger: enableLogger);
  poolConfig.main(enableLogger: enableLogger);
  query.main(enableLogger: enableLogger);
  client.main(enableLogger: enableLogger);
}
