//
// Copyright 2017 Memgraph
// Created by Marko Budiselic
//
// SOURCES:
// http://arne-mertz.de/2016/10/passkey-idiom/
// http://stackoverflow.com/questions/3217390/clean-c-granular-friend-equivalent-answer-attorney-client-idiom


#pragma once

template<typename T>
class PassKey {
  friend T;

private:
  // default constructor has to be manually defined otherwise = default
  // would allow aggregate initialization to bypass default constructor
  // both, default and copy constructors have to be user-defined
  // otherwise are public by default
  PassKey() {}
};
