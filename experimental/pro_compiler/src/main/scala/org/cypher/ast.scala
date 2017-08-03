package org.cypher



import scala.collection._
import scala.reflect.ClassTag
import scala.util.parsing.combinator._



// Parsing


trait CharacterStream {
  def asString: String
}


object CharacterStream {
  case class Default(asString: String) extends CharacterStream
}


// Query trees


trait Indexed {
  private var rawIndex: Int = -1

  def index: Int = rawIndex

  def index_=(i: Int) = rawIndex = i
}


sealed abstract class Tree extends Indexed


case class Query(
  val clauses: Seq[Clause]
) extends Tree


sealed abstract class Clause extends Tree


case class Match(
  optional: Boolean,
  patterns: Seq[Pattern],
  where: Where
) extends Clause


case class Where(
  expr: Expr
) extends Tree


sealed abstract class Expr extends Tree


case class Literal(value: Value) extends Expr


case class Ident(name: Name) extends Expr


case class Equals(left: Expr, right: Expr) extends Expr


case class PatternExpr(pattern: Pattern) extends Expr


sealed trait Value


case class Bool(x: Boolean) extends Value


case class Return(expr: Expr) extends Clause


case class Pattern(name: Name, elems: Seq[PatternElement]) extends Tree


sealed abstract class PatternElement extends Tree


case class NodePatternElement(
  name: Name,
  labels: Seq[Name],
  properties: Properties
) extends PatternElement


case class EdgePatternElement(
  left: Boolean,
  right: Boolean,
  name: Name,
  labels: Seq[Name],
  range: RangeSpec,
  properties: Properties
) extends PatternElement


case class RangeSpec(
  left: Option[Int],
  right: Option[Int]
) extends Tree


case class Properties(contents: Map[Name, Expr]) extends Tree


case class Name private[cypher] (private val raw: String) {
  def asString = raw
  override def toString = s"Name($raw)"
}


// Semantic analysis


class Symbol(val name: Name, val tpe: Type) {
  override def toString = s"<$name.asString: $tpe>"
}


trait Type


case class NodeType()


case class EdgeType()


class Table[T <: AnyRef: ClassTag] {
  private var array = new Array[T](100)

  private def ensureSize(sz: Int) {
    if (array.length < sz) {
      val narray = new Array[T](sz)
      System.arraycopy(array, 0, narray, 0, array.length)
      array = narray
    }
  }

  def apply(idx: Indexed): T = {
    array(idx.index)
  }

  def update(idx: Indexed, v: T) = {
    ensureSize(idx.index + 1)
    array(idx.index) = v
  }
}


class SymbolTable() {
  private val rawTable = mutable.Map[Name, Symbol]()
  def apply(name: Name): Symbol = rawTable(name)
  def update(name: Name, sym: Symbol) = {
    assert(!rawTable.contains(name))
    rawTable(name) = sym
  }
  def getOrCreate(name: Name): Symbol = rawTable.get(name) match {
    case Some(sym) =>
      sym
    case None =>
      val sym = new Symbol(name, null)
      rawTable(name) = sym
      sym
  }
}


case class TypecheckedTree(
  tree: Tree,
  symbols: SymbolTable,
  types: Table[Type]
)


class Namer {
  private var count = 0
  def freshName(): Name = {
    count += 1
    return Name(s"anon-$count")
  }
}


// Data model


trait Node


trait Edge


trait Index


trait Database {
  def nodeIndices(label: Name): Seq[Index]
  def edgeIndices(label: Name): Seq[Index]
}


object Database {
  case class Default() extends Database {
    def nodeIndices(label: Name) = Nil
    def edgeIndices(label: Name) = Nil
  }
}


// Logical plan


sealed trait LogicalPlan {
  def outputs: Seq[Symbol]

  def children: Seq[LogicalPlan]

  def operatorName = s"${getClass.getSimpleName}"

  def prettySelf: String = s"${operatorName}"

  def prettyVars: String = outputs.map(_.name.asString).mkString(", ")

  def pretty: String = {
    def titleWidth(plan: LogicalPlan): Int = {
      var w = 1 + plan.operatorName.length
      for (child <- plan.children) {
        val cw = titleWidth(child) + (plan.children.length - 1) * 2
        if (w < cw) w = cw
      }
      w
    }
    val w = math.max(12, titleWidth(this))
    var s = ""
    s += " Operator " + " " * (w - 7) + "| Variables \n"
    s += "-" * (w + 3) + "+" + "-" * 30 + "\n"
    def print(indent: Int, plan: LogicalPlan): Unit = {
      val leftspace = "| " * indent
      val title = plan.prettySelf
      val rightspace = " " * (w - title.length - 2 * indent)
      val rightpadspace = " " * (w - 2 * indent)
      val vars = plan.prettyVars
      s += s""" $leftspace+$title$rightspace | $vars\n"""
      if (plan.children.nonEmpty) {
        s += s""" $leftspace|$rightpadspace | \n"""
      }
      for ((child, idx) <- plan.children.zipWithIndex.reverse) {
        print(idx, child)
      }
    }
    print(0, this)
    s
  }
}


object LogicalPlan {
  sealed trait Source extends LogicalPlan {
  }

  case class ScanAll(outputs: Seq[Symbol], patternElem: NodePatternElement)
  extends Source {
    def children = Nil

    // def evaluate(db: Database): Stream = {
    //   db.iterator().filter { n =>
    //     patternElem match {
    //       case NodePatternElement(name, labels, properties) =>
    //         if (labels.subset(n.labels) && properties.subset(n.properties)) {
    //           Some(Seq(n))
    //         } else {
    //           None
    //         }
    //     }
    //   }
    // }
  }


  case class SeekByNodeLabelIndex(
    index: Index, name: Name, outputs: Seq[Symbol], patternElem: NodePatternElement
  ) extends Source {
    def children = Nil
  }


  case class ExpandAll(input: LogicalPlan, edgePattern: EdgePatternElement, nodePattern: NodePatternElement) extends LogicalPlan {
    def outputs = ???
    def children = ???
  }


  case class ExpandInto(input: LogicalPlan) extends LogicalPlan {
    def outputs = ???
    def children = ???
  }

  case class Filter(input: LogicalPlan, expr: Expr)
  extends LogicalPlan {
    def outputs = input.outputs
    def children = Seq(input)
  }


  sealed trait Sink extends LogicalPlan


  case class Produce(input: LogicalPlan, outputs: Seq[Symbol], expr: Expr)
  extends Sink {
    def children = Seq(input)
  }
}


trait Emitter {
  def emit[T](symbol: Symbol, value: T): Unit
}


// Physical plan


case class PhysicalPlan(val logicalPlan: LogicalPlan) {
  def execute(db: Database): Stream = {
    println(logicalPlan.pretty)
    ???
  }
}


case class Cost()


trait Stream


trait Target


// Phases


trait Phase[In, Out] {
  def apply(input: In): Out
}


case class Parser(ctx: Context) extends Phase[CharacterStream, Tree] {
  object CypherParser extends RegexParsers {
    def query: Parser[Query] = rep(clause) ^^ {
      case clauses => Query(clauses)
    }
    def clause: Parser[Clause] = `match` | `return` ^^ {
      case clause => clause
    }
    def `match`: Parser[Match] = opt("optional") ~ "match" ~ patterns ~ opt(where) ^^ {
      case optional ~ _ ~ p ~ Some(w) =>
        Match(optional.nonEmpty, p, w)
      case optional ~ _ ~ p ~ None =>
        Match(optional.nonEmpty, p, Where(Literal(Bool(true))))
    }
    def patterns: Parser[Seq[Pattern]] = nodePattern ~ rep(edgeAndNodePattern) ^^ {
      case node ~ edgeNodes =>
        val ps = node +: edgeNodes.map({ case (edge, node) => Seq(edge, node) }).flatten
        Seq(Pattern(ctx.namer.freshName(), ps))
    }
    def nodePattern: Parser[NodePatternElement] = "(" ~ ident ~ ")" ^^ {
      case _ ~ ident ~ _ => NodePatternElement(ident.name, Nil, Properties(Map()))
    }
    def edgeAndNodePattern: Parser[(EdgePatternElement, NodePatternElement)] =
      edgePattern ~ nodePattern ^^ {
        case edge ~ node => (edge, node)
      }
    def edgePattern: Parser[EdgePatternElement] =
      opt("<") ~ "--" ~ opt(">") ^^ {
        case left ~ _ ~ right => EdgePatternElement(
          left.nonEmpty,
          right.nonEmpty,
          ctx.namer.freshName(),
          Nil,
          RangeSpec(None, None),
          Properties(Map())
        )
      }
    def where: Parser[Where] = "where" ~ expr ^^ {
      case _ ~ expr => Where(expr)
    }
    def `return`: Parser[Return] = "return" ~ expr ^^ {
      case _ ~ expr => Return(expr)
    }
    def expr: Parser[Expr] = literal | ident | binary
    def binary: Parser[Expr] = equals
    def equals: Parser[Expr] = expr ~ "=" ~ expr ^^ {
      case left ~ _ ~ right => Equals(left, right)
    }
    def literal: Parser[Literal] = boolean ^^ {
      case x => x
    }
    def boolean: Parser[Literal] = ("true" | "false") ^^ {
      case "true" => Literal(Bool(true))
      case "false" => Literal(Bool(false))
    }
    def ident: Parser[Ident] = "[a-z]+".r ^^ {
      case s => Ident(Name(s))
    }
  }

  def apply(tokens: CharacterStream): Tree = {
    CypherParser.parseAll(CypherParser.query, tokens.asString) match {
      case CypherParser.Success(tree, _) => tree
      case failure: CypherParser.NoSuccess => sys.error(failure.msg)
    }
  }
}


case class Typechecker() extends Phase[Tree, TypecheckedTree] {
  private class Instance(val symbols: SymbolTable, val types: Table[Type]) {
    def traverse(tree: Tree): Unit = tree match {
      case Query(clauses) =>
        for (clause <- clauses) traverse(clause)
      case Match(opt, patterns, where) =>
        for (pattern <- patterns) traverse(pattern)

        // Ignore where for now.
      case Pattern(name, elems) =>
        for (elem <- elems) traverse(elem)
      case NodePatternElement(name, _, _) =>
        symbols.getOrCreate(name)
      case _ =>
        // Ignore for now.
    }
    def typecheck(tree: Tree) = {
      traverse(tree)
      TypecheckedTree(tree, symbols, types)
    }
  }
  def apply(tree: Tree): TypecheckedTree = {
    val symbols = new SymbolTable()
    val types = new Table[Type]
    val instance = new Instance(symbols, types)
    instance.typecheck(tree)
  }
}


case class LogicalPlanner(val ctx: Context)
extends Phase[TypecheckedTree, LogicalPlan] {
  def apply(tree: TypecheckedTree): LogicalPlan = {
    ctx.config.logicalPlanGenerator.generate(tree, ctx).next()
  }
}


case class PhysicalPlanner(val ctx: Context)
extends Phase[LogicalPlan, PhysicalPlan] {
  def apply(plan: LogicalPlan): PhysicalPlan = {
    ctx.config.physicalPlanGenerator.generate(plan).next()
  }
}


trait LogicalPlanGenerator {
  def generate(typedTree: TypecheckedTree, ctx: Context): Iterator[LogicalPlan]
}


object LogicalPlanGenerator {
  case class Default() extends LogicalPlanGenerator {
    class Instance(val typedTree: TypecheckedTree, val ctx: Context) {
      private def findOutputs(pat: NodePatternElement): Seq[Symbol] = {
        val sym = typedTree.symbols(pat.name)
        Seq(sym)
      }

      private def findOutputs(expr: Expr): Seq[Symbol] = {
        Seq()
      }

      private def genSource(pat: NodePatternElement): LogicalPlan = {
        pat.labels.find(label => ctx.database.nodeIndices(label).nonEmpty) match {
          case Some(label) =>
            val index = ctx.database.nodeIndices(label).head
            val outputs = findOutputs(pat)
            LogicalPlan.SeekByNodeLabelIndex(index, label, outputs, pat)
          case None =>
            val outputs = findOutputs(pat)
            LogicalPlan.ScanAll(outputs, pat)
        }
      }

      private def genPattern(elems: Seq[PatternElement]): LogicalPlan = {
        assert(elems.size == 1)
        val source = genSource(elems.head.asInstanceOf[NodePatternElement])
        source
      }

      private def genSourceClause(clause: Clause): LogicalPlan = clause match {
        case Match(opt, patterns, where) =>
          // Create source.
          assert(patterns.length == 1)
          val Pattern(_, elements) = patterns.head
          val plan = genPattern(elements)

          // Add a filter.
          new LogicalPlan.Filter(plan, where.expr)
        case tree =>
          sys.error(s"Unsupported source clause: $tree.")
      }

      def genReturn(input: LogicalPlan, ret: Return): LogicalPlan.Produce = {
        val outputs = findOutputs(ret.expr)
        LogicalPlan.Produce(input, outputs, ret.expr)
      }

      def genQueryPlan(tree: Tree): LogicalPlan = tree match {
        case Query(clauses) =>
          var plan = genSourceClause(clauses.head)
          for (clause <- clauses.tail) clause match {
            case ret @ Return(_) =>
              plan = genReturn(plan, ret)
            case clause =>
              sys.error(s"Unsupported clause: $tree.")
          }
          plan
        case tree =>
          sys.error(s"Not a valid query: $tree.")
      }
    }

    def generate(typedTree: TypecheckedTree, ctx: Context) = {
      val instance = new Instance(typedTree, ctx)
      val plan = instance.genQueryPlan(typedTree.tree)
      Iterator(plan)
    }
  }
}


trait PhysicalPlanGenerator {
  def generate(tree: LogicalPlan): Iterator[PhysicalPlan]
}


object PhysicalPlanGenerator {
  case class Default() extends PhysicalPlanGenerator {
    def generate(plan: LogicalPlan) = {
      Iterator(PhysicalPlan(plan))
    }
  }
}


case class Configuration(
  logicalPlanGenerator: LogicalPlanGenerator,
  physicalPlanGenerator: PhysicalPlanGenerator,
  estimator: PhysicalPlan => Cost
)


case class Context(
  config: Configuration,
  namer: Namer,
  database: Database
)


object Configuration {
  val defaultLogicalPlanGenerator = LogicalPlanGenerator.Default()
  val defaultPhysicalPlanGenerator = PhysicalPlanGenerator.Default()
  val defaultEstimator = (plan: PhysicalPlan) => {
    Cost()
  }
  def default() = Configuration(
    defaultLogicalPlanGenerator,
    defaultPhysicalPlanGenerator,
    defaultEstimator
  )
}


trait Interpreter {
  def interpret(query: CharacterStream): Stream
}


class DefaultInterpreter(ctx: Context) extends Interpreter {
  def interpret(query: CharacterStream): Stream = {
    val tree = Parser(ctx).apply(query)
    val typedTree = Typechecker().apply(tree)
    val logicalPlan = LogicalPlanner(ctx).apply(typedTree)
    val physicalPlan = PhysicalPlanner(ctx).apply(logicalPlan)
    physicalPlan.execute(ctx.database)
  }
}


trait Compiler {
  def compile(query: CharacterStream): Target
}


object Main {
  def main(args: Array[String]) {
    val db = Database.Default()
    val config = Configuration.default()
    val namer = new Namer
    val ctx = Context(config, namer, db)
    val interpreter = new DefaultInterpreter(ctx)
    val query = CharacterStream.Default("""
    match (a)
    return a
    """)
    interpreter.interpret(query)
  }
}
