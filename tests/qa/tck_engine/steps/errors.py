from behave import *

# TODO check for exact error?


def handle_error(context):
    """
    Function checks if exception exists in context.
    Exception exists if error occured in executing query

    @param context:
        behave.runner.Context, context of behave.
    """
    assert(context.exception is not None)


@then('an error should be raised')
def error(context):
    handle_error(context)


@then('a SyntaxError should be raised at compile time: NestedAggregation')
def syntax_error(context):
    handle_error(context)


@then('TypeError should be raised at compile time: IncomparableValues')
def type_error(context):
    handle_error(context)


@then(u'a TypeError should be raised at compile time: IncomparableValues')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: RequiresDirectedRelationship')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidRelationshipPattern')
def syntax_error(context):
    handle_error(context)


@then(u'a TypeError should be raised at runtime: MapElementAccessByNonString')
def type_error(context):
    handle_error(context)


@then(u'a ConstraintVerificationFailed should be raised at runtime: DeleteConnectedNode')
def step(context):
    handle_error(context)


@then(u'a TypeError should be raised at runtime: ListElementAccessByNonInteger')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidArgumentType')
def step(context):
    handle_error(context)


@then(u'a TypeError should be raised at runtime: InvalidElementAccess')
def step(context):
    handle_error(context)


@then(u'a ArgumentError should be raised at runtime: NumberOutOfRange')
def step(context):
    handle_error(context)


@then(u'a TypeError should be raised at runtime: InvalidArgumentValue')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: VariableAlreadyBound')
def step(context):
    handle_error(context)


@then(u'a TypeError should be raised at runtime: IncomparableValues')
def step(context):
    handle_error(context)


@then(u'a TypeError should be raised at runtime: PropertyAccessOnNonMap')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidUnicodeLiteral')
def step(context):
    handle_error(context)


@then(u'a SemanticError should be raised at compile time: MergeReadOwnWrites')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidAggregation')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: NoExpressionAlias')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: UndefinedVariable')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: VariableTypeConflict')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: DifferentColumnsInUnion')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidClauseComposition')
def step(context):
    handle_error(context)


@then(u'a TypeError should be raised at compile time: InvalidPropertyType')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: ColumnNameConflict')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: NoVariablesInScope')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidDelete')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: NegativeIntegerArgument')
def step(context):
    handle_error(context)


@then(u'a EntityNotFound should be raised at runtime: DeletedEntityAccess')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: RelationshipUniquenessViolation')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: CreatingVarLength')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidParameterUse')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: FloatingPointOverflow')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time InvalidArgumentExpression')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time InvalidUnicodeCharacter')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: NonConstantExpression')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: NoSingleRelationshipType')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: UnknownFunction')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidNumberLiteral')
def step_impl(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidArgumentExpression')
def step(context):
    handle_error(context)


@then(u'a SyntaxError should be raised at compile time: InvalidUnicodeCharacter')
def step(context):
    handle_error(context)
