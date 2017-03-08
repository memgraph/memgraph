#pragma once

#include <dlfcn.h>
#include <experimental/filesystem>
#include <stdexcept>
#include <string>
#include "utils/exceptions/dynamic_lib_exception.hpp"
namespace fs = std::experimental::filesystem;

#include "logging/loggable.hpp"

/**
 * DynamicLib is a wrapper aroung dynamic object returned by dlopen.
 *
 * Dynamic object must have extern C functions which names should be
 * "produce" and "destruct" (that's by convention).
 *
 * The functions prototypes can be defined with template parameter
 * type trait (T).
 *
 * DynamicLib isn't implemented for concurrent access.
 *
 * @tparam T type trait which defines the prototypes of extern C functions
 *         of undelying dynamic object.
 */
template <typename T>
class DynamicLib : public Loggable {
 public:
  /**
   * Initializes dynamic library (loads lib, produce and
   * destruct functions)
   *
   * @param lib_path file system path to dynamic library
   */
  DynamicLib(const fs::path &lib_path)
      : Loggable("DynamicLib"), lib_path(lib_path), lib_object(nullptr) {
    // load dynamic lib
    dynamic_lib = dlopen(lib_path.c_str(), RTLD_NOW);
    if (!dynamic_lib) throw DynamicLibException(dlerror());
    dlerror(); /* Clear any existing error */
    logger.trace("dynamic lib at ADDRESS({}) was opened", dynamic_lib);

    // load produce method
    this->produce_method =
        (typename T::ProducePrototype)dlsym(dynamic_lib, "produce");
    const char *dlsym_produce_error = dlerror();
    if (dlsym_produce_error) throw DynamicLibException(dlsym_produce_error);

    // load destruct method
    this->destruct_method =
        (typename T::DestructPrototype)dlsym(dynamic_lib, "destruct");
    const char *dlsym_destruct_error = dlerror();
    if (dlsym_destruct_error) throw DynamicLibException(dlsym_destruct_error);
  }

  // becuase we are dealing with pointers
  // and conceptualy there is no need to copy the instance of this class
  // the copy constructor and copy assignment operator are deleted
  // the same applies to move methods
  DynamicLib(const DynamicLib &other) = delete;
  DynamicLib &operator=(const DynamicLib &other) = delete;
  DynamicLib(DynamicLib &&other) = delete;
  DynamicLib &operator=(DynamicLib &&other) = delete;

  /**
   * Singleton method. Returns the same instance of underlying class
   * for every call. The instance of underlying class is returned by
   * extern C produce function.
   *
   * @return T the instance of lib class
   */
  typename T::ObjectPrototype *instance() {
    if (lib_object == nullptr) lib_object = this->produce_method();
    return lib_object;
  }

  /**
   * Clean underlying object and close the lib
   */
  ~DynamicLib() {
    // first destroy underlying object
    if (lib_object != nullptr) {
      logger.trace("shared object at ADDRESS({}) will be destroyed",
                   (void *)lib_object);
      this->destruct_method(lib_object);
    }

    // then destroy dynamic lib
    logger.trace("unloading lib {}", lib_path.c_str());
    if (dynamic_lib != nullptr) {
      logger.trace("closing dynamic lib ADDRESS({})", (void *)dynamic_lib);
      // // IMPORTANT: weird problem the program SEGFAULT on dlclose
      // // TODO: FIX somehow
      // // maybe something similar is:
      // //
      // http://stackoverflow.com/questions/6450828/segmentation-fault-when-using-dlclose-on-android-platform
      // // for now it is not crucial so I've created a task for that
      // // ! 0 is success
      int closing_status = dlclose(dynamic_lib);
      if (closing_status != 0) throw DynamicLibException(dlerror());
    } else {
      logger.trace("unload lib was called but lib ptr is null");
    }
  }

 private:
  std::string lib_path;
  void *dynamic_lib;
  typename T::ObjectPrototype *lib_object;
  typename T::ProducePrototype produce_method;
  typename T::DestructPrototype destruct_method;
};
