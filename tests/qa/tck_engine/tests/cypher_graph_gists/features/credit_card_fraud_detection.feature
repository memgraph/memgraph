Feature: Credit Card Fraud Detection

    Scenario: Match all disputed transactions
        Given graph "credit_card_fraud_detection"
        When executing query:
            """
            MATCH (victim:Person)-[r:HAS_BOUGHT_AT]->(merchant) WHERE r.status = "Disputed" RETURN victim.name AS `Customer Name`, merchant.name AS `Store Name`, r.amount AS Amount, r.time AS `Transaction Time` ORDER BY `Transaction Time` DESC
            """
        Then the result should be:
            | Customer Name | Store Name         | Amount   | Transaction Time |
            | 'Olivia'      | 'Urban Outfitters' | '1152'   | '8/10/2014'      |
            | 'Olivia'      | 'RadioShack'       | '1884'   | '8/1/2014'       |
            | 'Paul'        | 'Apple Store'      | '1021'   | '7/18/2014'      |
            | 'Marc'        | 'Apple Store'      | '1914'   | '7/18/2014'      |
            | 'Olivia'      | 'Apple Store'      | '1149'   | '7/18/2014'      |
            | 'Madison'     | 'Apple Store'      | '1925'   | '7/18/2014'      |
            | 'Madison'     | 'Urban Outfitters' | '1374'   | '7/10/2014'      |
            | 'Madison'     | 'RadioShack'       | '1368'   | '7/1/2014'       |
            | 'Paul'        | 'Urban Outfitters' | '1732'   | '5/10/2014'      |
            | 'Marc'        | 'Urban Outfitters' | '1424'   | '5/10/2014'      |
            | 'Paul'        | 'RadioShack'       | '1415'   | '4/1/2014'       |
            | 'Marc'        | 'RadioShack'       | '1721'   | '4/1/2014'       |
            | 'Paul'        | 'Macys'            | '1849'   | '12/20/2014'     |
            | 'Marc'        | 'Macys'            | '1003'   | '12/20/2014'     |
            | 'Olivia'      | 'Macys'            | '1790'   | '12/20/2014'     |
            | 'Madison'     | 'Macys'            | '1816'   | '12/20/2014'     |

    Scenario: Identify the Point of Origin of the Fraud
        Given graph "credit_card_fraud_detection"
        When executing query:
            """
            MATCH (victim:Person)-[r:HAS_BOUGHT_AT]->(merchant) WHERE r.status = "Disputed" MATCH (victim)-[t:HAS_BOUGHT_AT]->(othermerchants) WHERE t.status = "Undisputed" AND t.time < r.time WITH victim, othermerchants, t ORDER BY t.time DESC RETURN victim.name AS `Customer Name`, othermerchants.name AS `Store Name`, t.amount AS Amount, t.time AS `Transaction Time` ORDER BY `Transaction Time` DESC
            """
        Then the result should be:
            | Customer Name | Store Name            | Amount   | Transaction Time |
            | 'Olivia'      | 'Wallmart'            | '231'    | '7/12/2014'      |
            | 'Olivia'      | 'Wallmart'            | '231'    | '7/12/2014'      |
            | 'Olivia'      | 'Wallmart'            | '231'    | '7/12/2014'      |
            | 'Madison'     | 'Wallmart'            | '91'     | '6/29/2014'      |
            | 'Madison'     | 'Wallmart'            | '91'     | '6/29/2014'      |
            | 'Madison'     | 'Wallmart'            | '91'     | '6/29/2014'      |
            | 'Paul'        | 'Starbucks'           | '239'    | '5/15/2014'      |
            | 'Marc'        | 'American Apparel'    | '336'    | '4/3/2014'       |
            | 'Marc'        | 'American Apparel'    | '336'    | '4/3/2014'       |
            | 'Paul'        | 'Just Brew It'        | '986'    | '4/17/2014'      |
            | 'Paul'        | 'Just Brew It'        | '986'    | '4/17/2014'      |
            | 'Marc'        | 'Amazon'              | '134'    | '4/14/2014'      |
            | 'Marc'        | 'Amazon'              | '134'    | '4/14/2014'      |
            | 'Paul'        | 'Sears'               | '475'    | '3/28/2014'      |
            | 'Paul'        | 'Sears'               | '475'    | '3/28/2014'      |
            | 'Paul'        | 'Sears'               | '475'    | '3/28/2014'      |
            | 'Marc'        | 'Wallmart'            | '964'    | '3/22/2014'      |
            | 'Marc'        | 'Wallmart'            | '964'    | '3/22/2014'      |
            | 'Marc'        | 'Wallmart'            | '964'    | '3/22/2014'      |
            | 'Paul'        | 'Wallmart'            | '654'    | '3/20/2014'      |
            | 'Paul'        | 'Wallmart'            | '654'    | '3/20/2014'      |
            | 'Paul'        | 'Wallmart'            | '654'    | '3/20/2014'      |
            | 'Madison'     | 'Subway'              | '352'    | '12/16/2014'     |
            | 'Madison'     | 'Subway'              | '352'    | '12/16/2014'     |
            | 'Madison'     | 'Subway'              | '352'    | '12/16/2014'     |
            | 'Madison'     | 'Subway'              | '352'    | '12/16/2014'     |
            | 'Madison'     | 'MacDonalds'          | '630'    | '10/6/2014'      |
            | 'Madison'     | 'MacDonalds'          | '630'    | '10/6/2014'      |
            | 'Madison'     | 'MacDonalds'          | '630'    | '10/6/2014'      |
            | 'Madison'     | 'MacDonalds'          | '630'    | '10/6/2014'      |
            | 'Olivia'      | 'Soccer for the City' | '924'    | '10/4/2014'      |
            | 'Olivia'      | 'Soccer for the City' | '924'    | '10/4/2014'      |
            | 'Olivia'      | 'Soccer for the City' | '924'    | '10/4/2014'      |
            | 'Olivia'      | 'Soccer for the City' | '924'    | '10/4/2014'      |

    # doesnt' work because of count(DISTINCT ...) and collect(DISTINCT ...)
    Scenario: Zero in on the criminal
        Given graph "credit_card_fraud_detection"
        When executing query:
            """
            MATCH (victim:Person)-[r:HAS_BOUGHT_AT]->(merchant) WHERE r.status = "Disputed" MATCH (victim)-[t:HAS_BOUGHT_AT]->(othermerchants) WHERE t.status = "Undisputed" AND t.time < r.time WITH victim, othermerchants, t ORDER BY t.time DESC RETURN DISTINCT othermerchants.name AS `Suspicious Store`, count(DISTINCT t) AS CountT, collect(DISTINCT victim.name) AS Victims ORDER BY CountT DESC
            """
        Then the result should be:
            | Suspicious Store | Count | Victims                               |
            | 'Wallmart'       | 4     | ['Olivia', 'Madison', 'Marc', 'Paul'] |
