/* Memgraph specific Cypher reserved words used for enterprise features. */

/*
 * When changing this grammar make sure to update constants in
 * src/query/frontend/stripped_lexer_constants.hpp (kKeywords, kSpecialTokens
 * and bitsets) if needed.
 */

lexer grammar MemgraphCypherLexer ;

import CypherLexer ;

ALTER          : A L T E R ;
AUTH           : A U T H ;
CLEAR          : C L E A R ;
DATABASE       : D A T A B A S E ;
DENY           : D E N Y ;
DROP           : D R O P ;
DUMP           : D U M P ;
FOR            : F O R ;
FROM           : F R O M ;
GRANT          : G R A N T ;
GRANTS         : G R A N T S ;
IDENTIFIED     : I D E N T I F I E D ;
PASSWORD       : P A S S W O R D ;
PRIVILEGES     : P R I V I L E G E S ;
REVOKE         : R E V O K E ;
ROLE           : R O L E ;
ROLES          : R O L E S ;
STATS          : S T A T S ;
TO             : T O ;
USER           : U S E R ;
USERS          : U S E R S ;
