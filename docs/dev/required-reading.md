# Required Reading

This chapter lists a few books that should be read by everyone working on
Memgraph. Since Memgraph is developed primarily with C++, Python and Common
Lisp, books are oriented around those languages. Of course, there are plenty
of general books which will help you improve your technical skills (such as
"The Pragmatic Programmer", "The Mythical Man-Month", etc.), but they are not
listed here. This way the list should be kept short and the *required* part in
"Required Reading" more easily honored.

Some of these books you may find in our office, so feel free to pick them up.
If any are missing and you would like a physical copy, don't be afraid to
request the book for our office shelves.

Besides reading, don't get stuck in a rut and be a
[Blub Programmer](http://www.paulgraham.com/avg.html).

## Effective C++ by Scott Meyers

Required for C++ developers.

The book is a must-read as it explains most common gotchas of using C++. After
reading this book, you are good to write competent C++ which will pass code
reviews easily.

## Effective Modern C++ by Scott Meyers

Required for C++ developers.

This is a continuation of the previous book, it covers updates to C++ which
came with C++11 and later. The book isn't as imperative as the previous one,
but it will make you aware of modern features we are using in our codebase.

## Practical Common Lisp by Peter Siebel

Required for Common Lisp developers.

Free: http://www.gigamonkeys.com/book/

We use Common Lisp to generate C++ code and make our lives easier.
Unfortunately, not many developers are familiar with the language. This book
will make you familiar very quickly as it has tons of very practical
exercises. E.g. implementing unit testing library, serialization library and
bundling all that to create a mp3 music server.

## Effective Python by Brett Slatkin

(Almost) required reading for Python developers.

Why the "almost"? Well, Python is relatively easy to pick up and you will
probably learn all the gotchas during code review from someone more
experienced. This makes the book less necessary for a newcomer to Memgraph,
but the book is not advanced enough to delegate it to
[Advanced Reading](#advanced-reading). The book is written in similar vein as
the "Effective C++" ones and will make you familiar with nifty Python features
that make everyone's lives easier.

# Advanced Reading

The books listed below are not required reading, but you may want to read them
at some point when you feel comfortable enough.

## Design Patterns by Gamma et. al.

Recommended for C++ developers.

This book is highly divisive because it introduced a culture centered around
design patterns. The main issues is overuse of patterns which complicates the
code. This has made many Java programs to serve as examples of highly
complicated, "enterprise" code.

Unfortunately, design patterns are pretty much missing
language features. This is most evident in dynamic languages such as Python
and Lisp, as demonstrated by
[Peter Norvig](http://www.norvig.com/design-patterns/).

Or as [Paul Graham](http://www.paulgraham.com/icad.html) put it:

```
This practice is not only common, but institutionalized. For example, in the
OO world you hear a good deal about "patterns". I wonder if these patterns are
not sometimes evidence of case (c), the human compiler, at work. When I see
patterns in my programs, I consider it a sign of trouble. The shape of a
program should reflect only the problem it needs to solve. Any other
regularity in the code is a sign, to me at least, that I'm using abstractions
that aren't powerful enough-- often that I'm generating by hand the expansions
of some macro that I need to write
```

After presenting the book so negatively, why you should even read it then?
Well, it is good to be aware of those design patterns and use them when
appropriate. They can improve modularity and reuse of the code. You will also
find examples of such patterns in our code, primarily Strategy and Visitor
patterns. The book is also a good stepping stone to more advanced reading
about software design.

## Modern C++ Design by Andrei Alexandrescu

Recommended for C++ developers.

This book can be treated as a continuation of the previous "Design Patterns"
book. It introduced "dark arts of template meta-programming" to the world.
Many of the patterns are converted to use C++ templates which makes them even
better for reuse. But, like the previous book, there are downsides if used too
much. You should approach it with a critical eye and it will help you
understand ideas that are used in some parts of our codebase.

## Large Scale C++ Software Design by John Lakos

Recommended for C++ developers.

An old book, but well worth the read. Lakos presents a very pragmatic view of
writing modular software and how it affects both development time as well as
program runtime. Some things are outdated or controversial, but it will help
you understand how the whole C++ process of working in a large team, compiling
and linking affects development.

## On Lisp by Paul Graham

Recommended for Common Lisp developers.

Free: http://www.paulgraham.com/onlisp.html

An excellent continuation to "Practical Common Lisp". It starts of slow, as if
introducing the language, but very quickly picks up speed. The main meat of
the book are macros and their uses. From using macros to define cooperative
concurrency to including Prolog as if it's part of Common Lisp. The book will
help you understand more advanced macros that are occasionally used in our
Lisp C++ Preprocessor (LCP).
