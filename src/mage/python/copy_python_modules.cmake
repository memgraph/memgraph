# Copies the MAGE Python query modules into the dist directory, skipping
# development-only files. Run as: cmake -DSRC=<dir> -DDST=<dir> -P <this>.
#
# Keep the exclude list in sync with the install(DIRECTORY ...) rule in
# CMakeLists.txt next to this script.
file(COPY "${SRC}/" DESTINATION "${DST}"
     PATTERN "__pycache__"       EXCLUDE
     PATTERN "tests"             EXCLUDE
     PATTERN "htmlcov"           EXCLUDE
     PATTERN "pytest.ini"        EXCLUDE
     PATTERN "requirements.txt"  EXCLUDE
     PATTERN "CMakeLists.txt"    EXCLUDE
     PATTERN "copy_python_modules.cmake" EXCLUDE
     REGEX "/\\.[^/]*$"          EXCLUDE)
