/* Memgraph specific Cypher reserved words used for enterprise features. */

/*
 * When changing this grammar make sure to update constants in
 * src/query/frontend/stripped_lexer_constants.hpp (kKeywords, kSpecialTokens
 * and bitsets) if needed.
 */

lexer grammar MemgraphCypherLexer ;

import CypherLexer ;

AFTER          : A F T E R ;
ALTER          : A L T E R ;
ASYNC          : A S Y N C ;
AUTH           : A U T H ;
BAD            : B A D ;
BEFORE         : B E F O R E ;
CLEAR          : C L E A R ;
COMMIT         : C O M M I T ;
CSV            : C S V ;
DATA           : D A T A ;
DELIMITER      : D E L I M I T E R ;
DATABASE       : D A T A B A S E ;
DENY           : D E N Y ;
DIRECTORY      : D I R E C T O R Y ;
DROP           : D R O P ;
DUMP           : D U M P ;
EACH           : E A C H ;
EXECUTE        : E X E C U T E ;
FOR            : F O R ;
FREE           : F R E E ;
FROM           : F R O M ;
GRANT          : G R A N T ;
GRANTS         : G R A N T S ;
HEADER         : H E A D E R ;
IDENTIFIED     : I D E N T I F I E D ;
IGNORE         : I G N O R E ;
LOAD           : L O A D ;
LOCK           : L O C K ;
MAIN           : M A I N ;
MODE           : M O D E ;
NO             : N O ;
PASSWORD       : P A S S W O R D ;
PORT           : P O R T ;
PRIVILEGES     : P R I V I L E G E S ;
REGISTER       : R E G I S T E R ;
REPLICA        : R E P L I C A ;
REPLICAS       : R E P L I C A S ;
REPLICATION    : R E P L I C A T I O N ;
REVOKE         : R E V O K E ;
ROLE           : R O L E ;
ROLES          : R O L E S ;
QUOTE          : Q U O T E ;
STATS          : S T A T S ;
SYNC           : S Y N C ;
TIMEOUT        : T I M E O U T ;
TO             : T O ;
TRIGGER        : T R I G G E R ;
TRIGGERS       : T R I G G E R S ;
UNLOCK         : U N L O C K ;
UPDATE         : U P D A T E ;
USER           : U S E R ;
USERS          : U S E R S ;
