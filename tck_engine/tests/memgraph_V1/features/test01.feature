Feature: Test01
	
	
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

	
