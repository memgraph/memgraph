/* Memgraph specific Cypher reserved words used for enterprise features. */

/*
 * When changing this grammar make sure to update constants in
 * src/query/frontend/stripped_lexer_constants.hpp (kKeywords, kSpecialTokens
 * and bitsets) if needed.
 */

lexer grammar MemgraphCypherLexer ;

import CypherLexer ;

UNDERSCORE : '_' ;

AFTER          : A F T E R ;
ALTER          : A L T E R ;
ASYNC          : A S Y N C ;
AUTH           : A U T H ;
BAD            : B A D ;
BATCHES        : B A T C H E S ;
BATCH_INTERVAL : B A T C H UNDERSCORE I N T E R V A L ;
BATCH_SIZE     : B A T C H UNDERSCORE S I Z E ;
BEFORE         : B E F O R E ;
CLEAR          : C L E A R ;
COMMIT         : C O M M I T ;
COMMITTED      : C O M M I T T E D ;
CONFIG         : C O N F I G ;
CONSUMER_GROUP : C O N S U M E R UNDERSCORE G R O U P ;
CSV            : C S V ;
DATA           : D A T A ;
DELIMITER      : D E L I M I T E R ;
DATABASE       : D A T A B A S E ;
DENY           : D E N Y ;
DIRECTORY      : D I R E C T O R Y ;
DROP           : D R O P ;
DUMP           : D U M P ;
EXECUTE        : E X E C U T E ;
FOR            : F O R ;
FREE           : F R E E ;
FREE_MEMORY    : F R E E UNDERSCORE M E M O R Y ;
FROM           : F R O M ;
GLOBAL         : G L O B A L ;
GRANT          : G R A N T ;
GRANTS         : G R A N T S ;
HEADER         : H E A D E R ;
IDENTIFIED     : I D E N T I F I E D ;
IGNORE         : I G N O R E ;
ISOLATION      : I S O L A T I O N ;
LEVEL          : L E V E L ;
LOAD           : L O A D ;
LOCK           : L O C K ;
LOCK_PATH      : L O C K UNDERSCORE P A T H ;
MAIN           : M A I N ;
MODE           : M O D E ;
NEXT           : N E X T ;
NO             : N O ;
PASSWORD       : P A S S W O R D ;
PORT           : P O R T ;
PRIVILEGES     : P R I V I L E G E S ;
READ           : R E A D ;
READ_FILE      : R E A D UNDERSCORE F I L E ;
REGISTER       : R E G I S T E R ;
REPLICA        : R E P L I C A ;
REPLICAS       : R E P L I C A S ;
REPLICATION    : R E P L I C A T I O N ;
REVOKE         : R E V O K E ;
ROLE           : R O L E ;
ROLES          : R O L E S ;
QUOTE          : Q U O T E ;
SESSION        : S E S S I O N ;
SNAPSHOT       : S N A P S H O T ;
START          : S T A R T ;
STATS          : S T A T S ;
STREAM         : S T R E A M ;
STREAMS        : S T R E A M S ;
SYNC           : S Y N C ;
TIMEOUT        : T I M E O U T ;
TO             : T O ;
TOPICS         : T O P I C S;
TRANSACTION    : T R A N S A C T I O N ;
TRANSFORM      : T R A N S F O R M ;
TRIGGER        : T R I G G E R ;
TRIGGERS       : T R I G G E R S ;
UNCOMMITTED    : U N C O M M I T T E D ;
UNLOCK         : U N L O C K ;
UPDATE         : U P D A T E ;
USER           : U S E R ;
USERS          : U S E R S ;
