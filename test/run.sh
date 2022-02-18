#!/bin/bash

# Set the thread count to 1 (-j 1) or else errors will arise and multiple mock servers 
# Try to bind to the same port
dart test -j 1 --chain-stack-traces
