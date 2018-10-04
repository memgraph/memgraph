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
BATCH          : B A T C H ;
BATCHES        : B A T C H E S ;
CLEAR          : C L E A R ;
DATA           : D A T A ;
DENY           : D E N Y ;
DROP           : D R O P ;
FOR            : F O R ;
FROM           : F R O M ;
GRANT          : G R A N T ;
GRANTS         : G R A N T S ;
IDENTIFIED     : I D E N T I F I E D ;
INTERVAL       : I N T E R V A L ;
K_TEST         : T E S T ;
KAFKA          : K A F K A ;
LOAD           : L O A D ;
PASSWORD       : P A S S W O R D ;
PRIVILEGES     : P R I V I L E G E S ;
REVOKE         : R E V O K E ;
ROLE           : R O L E ;
ROLES          : R O L E S ;
SIZE           : S I Z E ;
START          : S T A R T ;
STOP           : S T O P ;
STREAM         : S T R E A M ;
STREAMS        : S T R E A M S ;
TO             : T O ;
TOPIC          : T O P I C ;
TRANSFORM      : T R A N S F O R M ;
USER           : U S E R ;
USERS          : U S E R S ;
