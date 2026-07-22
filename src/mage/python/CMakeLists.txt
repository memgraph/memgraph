# MAGE Python query modules: no compilation, just copy into the dist and
# install the same file set.
#
# requirements.txt is a dev/packaging input, not a shipped module, so it is
# excluded; requirements-gpu.txt has always shipped with the modules and
# still does. The old `setup build --no-ml` flavour merely dropped tgn.py,
# link_prediction.py and node_classification.py from the shipped set —
# that's a packaging-flavour concern, to be handled when the mage packages
# move to CPack.
#
# NOTE: query_modules/CMakeLists.txt independently copies a small subset of
# these files (mage/date, top-level *.py) into the memgraph package; see the
# comment there. Dedup is deferred to the packaging phase.

file(GLOB_RECURSE MG_MAGE_PY_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.py)
mg_add_stamped_custom_target(mage_python_modules ALL
    STAMP_DIR ${MG_MAGE_DIST_DIR}
    COMMANDS
        COMMAND ${CMAKE_COMMAND}
            -DSRC=${CMAKE_CURRENT_SOURCE_DIR}
            -DDST=${MG_MAGE_DIST_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/copy_python_modules.cmake
    DEPENDS ${MG_MAGE_PY_SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/copy_python_modules.cmake
    COMMENT "Copying MAGE Python modules into dist")

# Keep the exclude list in sync with copy_python_modules.cmake.
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
        DESTINATION ${MG_MAGE_INSTALL_DIR} COMPONENT mage
        PATTERN "__pycache__"       EXCLUDE
        PATTERN "tests"             EXCLUDE
        PATTERN "htmlcov"           EXCLUDE
        PATTERN "pytest.ini"        EXCLUDE
        PATTERN "requirements.txt"  EXCLUDE
        PATTERN "CMakeLists.txt"    EXCLUDE
        PATTERN "copy_python_modules.cmake" EXCLUDE
        REGEX "/\\.[^/]*$"          EXCLUDE)
