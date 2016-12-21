#pragma once

#include <utility>

/*  @brief Calls a cleanup function on scope exit
 *
 *  consider this example:
 *
 *  void hard_worker()
 *  {
 *      resource.enable();
 *      do_stuff();          // throws exception
 *      resource.disable();
 *  }
 *
 *  if do_stuff throws an exception, resource.disable is never called
 *  and the app is left in an inconsistent state. ideally, you would like
 *  to call resource.disable regardles of the exception being thrown.
 *  OnScopeExit makes this possible and very convenient via a 'Auto' macro
 *
 *  void hard_worker()
 *  {
 *      resource.enable();
 *      Auto(resource.disable());
 *      do_stuff();          // throws exception
 *  }
 *
 *  now, resource.disable will be called every time it goes out of scope
 *  regardless of the exception
 *
 *  @tparam F Lambda which holds a wrapper function around the cleanup code
 */
template <class F>
class OnScopeExit
{
public:
   OnScopeExit(F& f) : f(f) {}

   ~OnScopeExit()
   {
       // calls the cleanup function
       f();
   }

private:
   F& f;
};

#define TOKEN_PASTEx(x, y) x##y
#define TOKEN_PASTE(x, y) TOKEN_PASTEx(x, y)

#define Auto_INTERNAL(Destructor, counter) \
    auto TOKEN_PASTE(auto_func_, counter) = [&]() { Destructor; }; \
        OnScopeExit<decltype(TOKEN_PASTE(auto_func_, counter))> \
            TOKEN_PASTE(auto_, counter)(TOKEN_PASTE(auto_func_, counter));

#define Auto(Destructor) Auto_INTERNAL(Destructor, __COUNTER__)

// -- example:
// Auto(f());
// -- is expended to:
// auto auto_func_1 = [&]() { f(); };
// OnScopeExit<decltype(auto_func_1)> auto_1(auto_func_1);
// -- f() is called at the end of a scope
