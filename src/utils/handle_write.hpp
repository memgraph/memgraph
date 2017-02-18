#pragma once

/*
 * Source object is going to be traversed by Writer and Writer will
 * write that data into the Destination object.
 *
 * Writer object defines write format.
 */
template <typename Destination, typename Writer, typename Source>
Destination handle_write(const Source& source) {
  Destination destination;
  Writer writter(destination);
  source.handle(writter);
  return destination;
}
