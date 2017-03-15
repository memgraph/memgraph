Feature: Test01

	Scenario: Create empty node without clearing database
		When executing query:
			"""
			CREATE (n)
			"""
		Then the result should be empty
	
	Scenario: Create node with label without clearing database
		When executing query:
			"""
			CREATE (n:L:K)
			"""
		Then the result should be empty

	Scenario: Create node with int property without clearing database
		When executing query:
			"""
			CREATE (n{a: 1})
			"""
		Then the result should be empty
	
	Scenario: Create node with float property without clearing database
		When executing query:
			"""
			CREATE (n{a: 1.0})
			"""
		Then the result should be empty

	Scenario: Create node with string property without clearing database
		When executing query:
			"""
			CREATE (n{a: 'string'})
			"""
		Then the result should be empty

	Scenario: Create node with bool properties without clearing database
		When executing query:
			"""
			CREATE (n{a: True, b: false})
			"""
		Then the result should be empty

	Scenario: Create node with null property without clearing database
		When executing query:
			"""
			CREATE (n{a: NULL})
			"""
		Then the result should be empty
	
	Scenario: Create node with properties without clearing database
		When executing query:
			"""
			CREATE (n{a: 1.0, b: false, c: 1, d: 'neki"string"', e: NULL})
			"""
		Then the result should be empty
	
	Scenario: Create node with properties without clearing database
		When executing query:
			"""
			CREATE (n:L:K:T {a: 1.0, b: false, c: 1, d: 'neki"string"', e: NULL})
			"""
		Then the result should be empty
	


	

	Scenario: Create empty node without clearing database:
		When executing query:
			"""
			CREATE (n) RETURN n
			"""
		Then the result should be:
		    | n |
			|( )|
	
	Scenario: Create node with labels without clearing database and return it
		When executing query:
			"""
			CREATE (n:A:B) RETURN n
			"""
		Then the result should be:
		    |   n  |
			|(:A:B)|

	Scenario: Create node with properties without clearing database and return it
		When executing query:
			"""
			CREATE (n{a: 1.0, b: FALSE, c: 1, d: 'neki"string"', e: NULL}) RETURN n
			"""
		Then the result should be:
			|                 n								|
			|({a: 1.0, b: false, c: 1, d: 'neki"string"'})  |

	Scenario: Create node with labels and properties without clearing database and return it
		When executing query:
			"""
			CREATE (n:A:B{a: 1.0, b: False, c: 1, d: 'neki"string"', e: NULL}) RETURN n
			"""
		Then the result should be:
			|                 n								    |
			| (:A:B{a: 1.0, b: false, c: 1, d: 'neki"string"'}) |

	Scenario: Create node with properties and labels without clearing database and return its properties
		When executing query:
			"""
			CREATE (n:A:B{a: 1.0, b: false, c: 1, d: 'neki"string"', e: NULL}) RETURN n.a, n.b, n.c, n.d, n.e
			"""
		Then the result should be:
			| n.a | n.b   | n.c | n.d           | n.e  |
			| 1.0 | false | 1   | 'neki"string"'| null |



	Scenario: Create and match with label
		Given graph "graph_01"
		When executing query:
			"""
			MATCH (n:Person) RETURN n
			"""
		Then the result should be:
			|              n                |
			|     (:Person {age: 20})       |
			|  (:Person :Student {age: 20}) |
			|     (:Person {age: 21})       |

	Scenario: Create and match with label
		Given graph "graph_01"
		When executing query:
			"""
			MATCH (n:Student) RETURN n
			"""
		Then the result should be:
			|              n                |
			|  (:Person :Student {age: 20}) |
			|      (:Student {age: 21})     |

	Scenario: Create, match with label and property
		Given graph "graph_01"
		When executing query:
			"""
			MATCH (n:Person {age: 20}) RETURN n
			"""
		Then the result should be:
			|              n                |
			|     (:Person {age: 20})       |
			|  (:Person :Student {age: 20}) |

	Scenario: Create, match with label and filter property using WHERE
		Given graph "graph_01"
		When executing query:
			"""
			MATCH (n:Person) WHERE n.age = 20 RETURN n
			"""
		Then the result should be:
			|              n                |
			|     (:Person {age: 20})       |
			|  (:Person :Student {age: 20}) |

	Scenario: Create and match with property
		Given graph "graph_01"
		When executing query:
			"""
			MATCH (n {age: 20}) RETURN n
			"""
		Then the result should be:
			|              n                |
			|     (:Person {age: 20})       |
			|  (:Person :Student {age: 20}) |

