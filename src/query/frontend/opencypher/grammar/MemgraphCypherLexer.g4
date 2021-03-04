/* Memgraph specific Cypher reserved words used for enterprise features. */

/*
 * When changing this grammar make sure to update constants in
 * src/query/frontend/stripped_lexer_constants.hpp (kKeywords, kSpecialTokens
 * and bitsets) if needed.
 */

lexer grammar MemgraphCypherLexer ;

import CypherLexer ;

ALTER          : A L T E R ;
ASYNC          : A S Y N C ;
AUTH           : A U T H ;
CLEAR          : C L E A R ;
DATA           : D A T A ;
DATABASE       : D A T A B A S E ;
DENY           : D E N Y ;
DIRECTORY      : D I R E C T O R Y ;
DROP           : D R O P ;
DUMP           : D U M P ;
FOR            : F O R ;
FREE           : F R E E ;
FROM           : F R O M ;
GRANT          : G R A N T ;
GRANTS         : G R A N T S ;
IDENTIFIED     : I D E N T I F I E D ;
LOCK           : L O C K ;
MAIN           : M A I N ;
MODE           : M O D E ;
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
STATS          : S T A T S ;
SYNC           : S Y N C ;
TIMEOUT        : T I M E O U T ;
TO             : T O ;
UNLOCK         : U N L O C K ;
USER           : U S E R ;
USERS          : U S E R S ;
