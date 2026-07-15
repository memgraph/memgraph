# complete code
#!/bin/bash

# Clean the build directory
rm -rf build

# Clean the conan cache
conan remove --force --all

echo 'Build directory and conan cache cleaned.'