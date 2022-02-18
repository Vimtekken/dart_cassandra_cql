#!/bin/bash

# Build test coverage reports. To use this script you need to:
# 1) Install lcov package
# 2) Run `pub global activate coverage`

# Cleanup
rm -f test/coverage/coverage.json
rm -f test/coverage/coverage.lcov
rm -f test/coverage/filtered.lcov

# Run tests in checked mode and start observatory; block when tests complete so we can collect coverage data
echo "Begin Testing"
dart test -j 1 --coverage test/coverage

# Convert data to LCOV format
echo "Converting to LCOV format..."
pub global run coverage:format_coverage --packages=.packages -i test/coverage/ --out test/coverage/coverage.lcov --lcov

# Remove LCOV blocks that do not belong to our project lib/ folder
echo "Filtering unrelated files from the LCOV data..."
ePROJECT_ROOT=`pwd`
sed -n '\:^SF.*'"$PROJECT_ROOT"'/lib:,\:end_of_record:p' test/coverage/coverage.lcov > test/coverage/filtered.lcov

# Format LCOV data to HTML
echo "Rendering HTML coverage report to: test/coverage/html"
genhtml test/coverage/filtered.lcov --output-directory test/coverage/html --ignore-errors source --quiet

echo "The generated coverage data is available here: "`pwd`"/test/coverage/html/index.html"
