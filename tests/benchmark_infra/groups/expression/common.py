def generate(expressions, repetitions):
    idx = 0
    def get_alias():
        nonlocal idx
        idx += 1
        return "a" + str(idx)

    query = []
    for i in range(repetitions):
        for expression in expressions:
            query.append(expression + " as " + get_alias())

    return "RETURN " + ", ".join(query)
