@0xb107d3d6b4b1600b;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("query::capnp");

using Dis = import "/distributed/serialization.capnp";
using Storage = import "/storage/serialization.capnp";
using Symbols = import "/query/frontend/semantic/symbol.capnp";

struct Tree {
  uid @0 :Int64;

  union {
    expression @1 :Expression;
    where @2 :Where;
    namedExpression @3 :NamedExpression;
    patternAtom @4 :PatternAtom;
    pattern @5 :Pattern;
    clause @6 :Clause;
    singleQuery @7 :SingleQuery;
    cypherUnion @8 :CypherUnion;
    query @9 :Query;
  }
}

struct Expression {
  union {
    binaryOperator @0 :BinaryOperator;
    unaryOperator @1 :UnaryOperator;
    baseLiteral @2 :BaseLiteral;
    listSlicingOperator @3 :ListSlicingOperator;
    ifOperator @4 :IfOperator;
    identifier @5 :Identifier;
    propertyLookup @6 :PropertyLookup;
    labelsTest @7 :LabelsTest;
    function @8 :Function;
    reduce @9 :Reduce;
    all @10 :All;
    single @11 :Single;
    parameterLookup @12 :ParameterLookup;
  }
}

struct Where {
  expression @0 :Tree;
}

struct NamedExpression {
  name @0 :Text;
  expression @1 :Tree;
  tokenPosition @2 :Int32;
}

struct PatternAtom {
  union {
    nodeAtom @0 :NodeAtom;
    edgeAtom @1 :EdgeAtom;
  }
  identifier @2 :Tree;
}

struct Pair(First, Second) {
  first @0 :First;
  second @1 :Second;
}

struct NodeAtom {
  properties @0 :List(Entry);
  struct Entry {
    key @0 :Pair(Text, Storage.Common);
    value @1 :Tree;
  }

  labels @1 :List(Storage.Common);
}

struct EdgeAtom {
  enum Type {
    single @0;
    depthFirst @1;
    breadthFirst @2;
    weightedShortestPath @3;
  }
  type @0 :Type;

  enum Direction {
    in @0;
    out @1;
    both @2;
  }
  direction @1 :Direction;

  properties @2 :List(Entry);
  struct Entry {
    key @0 :Pair(Text, Storage.Common);
    value @1 :Tree;
  }

  lowerBound @3 :Tree;
  upperBound @4 :Tree;

  filterLambda @5 :Lambda;
  weightLambda @6 :Lambda;
  struct Lambda {
    innerEdge @0 :Tree;
    innerNode @1 :Tree;
    expression @2 :Tree;
  }

  totalWeight @7 :Tree;
  edgeTypes @8 :List(Storage.Common);
}

struct Pattern {
  identifier @0 :Tree;
  atoms @1 :List(Tree);
}

struct Clause {
  union {
    create @0 :Create;
    match @1 :Match;
    return @2 :Return;
    with @3 :With;
    delete @4 :Delete;
    setProperty @5 :SetProperty;
    setProperties @6 :SetProperties;
    setLabels @7 :SetLabels;
    removeProperty @8 :RemoveProperty;
    removeLabels @9 :RemoveLabels;
    merge @10 :Merge;
    unwind @11 :Unwind;
    createIndex @12 :CreateIndex;
    modifyUser @13 :ModifyUser;
    dropUser @14 :DropUser;
  }
}

struct SingleQuery {
  clauses @0 :List(Tree);
}

struct CypherUnion {
  singleQuery @0 :Tree;
  distinct @1 :Bool;
  unionSymbols @2 :List(Symbols.Symbol);
}

struct Query {
  singleQuery @0 :Tree;
  cypherUnions @1 :List(Tree);
}

struct BinaryOperator {
  union {
    orOperator @0 :OrOperator;
    xorOperator @1 :XorOperator;
    andOperator @2 :AndOperator;
    additionOperator @3 :AdditionOperator;
    subtractionOperator @4 :SubtractionOperator;
    multiplicationOperator @5 :MultiplicationOperator;
    divisionOperator @6 :DivisionOperator;
    modOperator @7 :ModOperator;
    notEqualOperator @8 :NotEqualOperator;
    equalOperator @9 :EqualOperator;
    lessOperator @10 :LessOperator;
    greaterOperator @11 :GreaterOperator;
    lessEqualOperator @12 :LessEqualOperator;
    greaterEqualOperator @13 :GreaterEqualOperator;
    inListOperator @14 :InListOperator;
    listMapIndexingOperator @15 :ListMapIndexingOperator;
    aggregation @16 :Aggregation;
  }
  expression1 @17 :Tree;
  expression2 @18 :Tree;
}

struct OrOperator {}
struct XorOperator {}
struct AndOperator {}
struct AdditionOperator {}
struct SubtractionOperator {}
struct MultiplicationOperator {}
struct DivisionOperator {}
struct ModOperator {}
struct NotEqualOperator {}
struct EqualOperator {}
struct LessOperator {}
struct GreaterOperator {}
struct LessEqualOperator {}
struct GreaterEqualOperator {}
struct InListOperator {}
struct ListMapIndexingOperator {}
struct Aggregation {
  enum Op {
    count @0;
    min @1;
    max @2;
    sum @3;
    avg @4 ;
    collectList @5;
    collectMap @6;
  }
  op @0 :Op;
}

struct UnaryOperator {
  union {
    notOperator @0 :NotOperator;
    unaryPlusOperator @1 :UnaryPlusOperator;
    unaryMinusOperator @2 :UnaryMinusOperator;
    isNullOperator @3 :IsNullOperator;
  }
  expression @4 :Tree;
}

struct NotOperator {}
struct UnaryPlusOperator {}
struct UnaryMinusOperator {}
struct IsNullOperator {}

struct BaseLiteral {
  union {
    primitiveLiteral @0 :PrimitiveLiteral;
    listLiteral @1 :ListLiteral;
    mapLiteral @2 :MapLiteral;
  }
}

struct PrimitiveLiteral {
  tokenPosition @0 :Int32;
  value @1 :Dis.TypedValue;
}

struct ListLiteral {
  elements @0 :List(Tree);
}

struct MapLiteral {
  elements @0 :List(Entry);
  struct Entry {
    key @0 :Pair(Text, Storage.Common);
    value @1 :Tree;
  }
}

struct ListSlicingOperator {
  list @0 :Tree;
  lowerBound @1 :Tree;
  upperBound @2 :Tree;
}

struct IfOperator {
  condition @0 :Tree;
  thenExpression @1 :Tree;
  elseExpression @2 :Tree;
}

struct Identifier {
  name @0 :Text;
  userDeclared @1 :Bool;
}

struct PropertyLookup {
  expression @0 :Tree;
  propertyName @1 :Text;
  property @2 :Storage.Common;
}

struct LabelsTest {
  expression @0 :Tree;
  labels @1 :List(Storage.Common);
}

struct Function {
  functionName @0 :Text;
  arguments @1 :List(Tree);
}

struct Reduce {
  accumulator @0 :Tree;
  initializer @1 :Tree;
  identifier @2 :Tree;
  list @3 :Tree;
  expression @4 :Tree;
}

struct All {
  identifier @0 :Tree;
  listExpression @1 :Tree;
  where @2 :Tree;
}

struct Single {
  identifier @0 :Tree;
  listExpression @1 :Tree;
  where @2 :Tree;
}

struct ParameterLookup {
  tokenPosition @0 :Int32;
}

struct Create {
  patterns @0 :List(Tree);
}

struct Match {
  patterns @0 :List(Tree);
  where @1 :Tree;
  optional @2 :Bool;
}

enum Ordering {
  asc @0;
  desc @1;
}

struct ReturnBody {
  distinct @0 :Bool;
  allIdentifiers @1 :Bool;
  namedExpressions @2 :List(Tree);
  orderBy @3 :List(Pair);

  struct Pair {
    ordering @0 :Ordering;
    expression @1 :Tree;
  }

  skip @4 :Tree;
  limit @5 :Tree;
}

struct Return {
  returnBody @0 :ReturnBody;
}

struct With {
  returnBody @0 :ReturnBody;
  where @1 :Tree;
}

struct Delete {
  detach @0 :Bool;
  expressions @1 :List(Tree);
}

struct SetProperty {
  propertyLookup @0 :Tree;
  expression @1 :Tree;
}

struct SetProperties {
  identifier @0 :Tree;
  expression @1 :Tree;
  update @2 :Bool;
}

struct SetLabels {
  identifier @0 :Tree;
  labels @1 :List(Storage.Common);
}

struct RemoveProperty {
  propertyLookup @0 :Tree;
}

struct RemoveLabels {
  identifier @0 :Tree;
  labels @1 :List(Storage.Common);
}

struct Merge {
  pattern @0 :Tree;
  onMatch @1 :List(Tree);
  onCreate @2 :List(Tree);
}

struct Unwind {
  namedExpression @0 :Tree;
}

struct CreateIndex {
  label @0 :Storage.Common;
  property @1 :Storage.Common;
}

struct ModifyUser {
  username @0 :Text;
  password @1 :Tree;
  isCreate @2 :Bool;
}

struct DropUser {
  usernames @0 :List(Text);
}
