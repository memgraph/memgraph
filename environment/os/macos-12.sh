#/bin/bash

brew install bash
brew install cmake
brew install clisp sbcl
brew install boost gflags fmt jemalloc openssl
brew install openssl@1.1

brew install cyrus-sasl
# cyrus-sasl is keg-only, which means it was not symlinked into /opt/homebrew,
# because macOS already provides this software and installing another version in
# parallel can cause all kinds of trouble.
# If you need to have cyrus-sasl first in your PATH, run:
#   echo 'export PATH="/opt/homebrew/opt/cyrus-sasl/sbin:$PATH"' >> ~/.zshrc
# For compilers to find cyrus-sasl you may need to set:
#   export LDFLAGS="-L/opt/homebrew/opt/cyrus-sasl/lib"
#   export CPPFLAGS="-I/opt/homebrew/opt/cyrus-sasl/include"

# TODO(gitbuda): memgraph::utils::SpinLock
# TODO(gitbuda): memgraph::utils::AsyncTimer
# TODO(gitbuda): memgraph::utils::RWLock
# TODO(gitbuda): memgraph::utils::ThreadSetName
# TODO(gitbuda): RocksDB 7.7.2 compiles fine
