/*
 * Copyright 2021 Memgraph Ltd.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
 * License, and you may not use this file except in compliance with the Business Source License.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

/* Memgraph specific Cypher reserved words used for enterprise features. */

/*
 * When changing this grammar make sure to update constants in
 * src/query/frontend/stripped_lexer_constants.hpp (kKeywords, kSpecialTokens
 * and bitsets) if needed.
 */

lexer grammar MemgraphCypherLexer ;

import CypherLexer ;

UNDERSCORE : '_' ;

AFTER               : A F T E R ;
ALTER               : A L T E R ;
ASYNC               : A S Y N C ;
AUTH                : A U T H ;
BAD                 : B A D ;
BATCH_INTERVAL      : B A T C H UNDERSCORE I N T E R V A L ;
BATCH_LIMIT         : B A T C H UNDERSCORE L I M I T ;
BATCH_SIZE          : B A T C H UNDERSCORE S I Z E ;
BEFORE              : B E F O R E ;
BOOL                : B O O L ;
BOOTSTRAP_SERVERS   : B O O T S T R A P UNDERSCORE S E R V E R S ;
CHECK               : C H E C K ;
CLEAR               : C L E A R ;
COMMIT              : C O M M I T ;
COMMITTED           : C O M M I T T E D ;
CONFIG              : C O N F I G ;
CONFIGS             : C O N F I G S;
CONSUMER_GROUP      : C O N S U M E R UNDERSCORE G R O U P ;
CREDENTIALS         : C R E D E N T I A L S ;
CSV                 : C S V ;
DATA                : D A T A ;
DELIMITER           : D E L I M I T E R ;
DATABASE            : D A T A B A S E ;
DATE                : D A T E ;
DENY                : D E N Y ;
DIRECTORY           : D I R E C T O R Y ;
DROP                : D R O P ;
DUMP                : D U M P ;
DURABILITY          : D U R A B I L I T Y ;
DURATION            : D U R A T I O N ;
EXECUTE             : E X E C U T E ;
FOR                 : F O R ;
FOREACH             : F O R E A C H;
FREE                : F R E E ;
FREE_MEMORY         : F R E E UNDERSCORE M E M O R Y ;
FROM                : F R O M ;
FLOAT               : F L O A T ;
GLOBAL              : G L O B A L ;
GRANT               : G R A N T ;
GRANTS              : G R A N T S ;
HEADER              : H E A D E R ;
IDENTIFIED          : I D E N T I F I E D ;
IGNORE              : I G N O R E ;
INTEGER             : I N T E G E R ;
ISOLATION           : I S O L A T I O N ;
KAFKA               : K A F K A ;
LEVEL               : L E V E L ;
LOAD                : L O A D ;
LIST                : L I S T ;
LOCALDATETIME       : L O C A L D A T E T I M E ;
LOCALTIME           : L O C A L T I M E ;
LOCK                : L O C K ;
MAIN                : M A I N ;
MAP                 : M A P ;
MODE                : M O D E ;
MODULE_READ         : M O D U L E UNDERSCORE R E A D ;
MODULE_WRITE        : M O D U L E UNDERSCORE W R I T E ;
NEXT                : N E X T ;
NO                  : N O ;
PASSWORD            : P A S S W O R D ;
PORT                : P O R T ;
PRIVILEGES          : P R I V I L E G E S ;
PULSAR              : P U L S A R ;
READ                : R E A D ;
READ_FILE           : R E A D UNDERSCORE F I L E ;
REGISTER            : R E G I S T E R ;
REPLICA             : R E P L I C A ;
REPLICAS            : R E P L I C A S ;
REPLICATION         : R E P L I C A T I O N ;
REVOKE              : R E V O K E ;
ROLE                : R O L E ;
ROLES               : R O L E S ;
QUOTE               : Q U O T E ;
SCHEMA              : S C H E M A ;
SCHEMAS             : S C H E M A S ;
SERVICE_URL         : S E R V I C E UNDERSCORE U R L ;
SESSION             : S E S S I O N ;
SETTING             : S E T T I N G ;
SETTINGS            : S E T T I N G S ;
SNAPSHOT            : S N A P S H O T ;
START               : S T A R T ;
STATS               : S T A T S ;
STOP                : S T O P ;
STREAM              : S T R E A M ;
STREAMS             : S T R E A M S ;
STRING              : S T R I N G ;
SYNC                : S Y N C ;
TIMEOUT             : T I M E O U T ;
TO                  : T O ;
TOPICS              : T O P I C S;
TRANSACTION         : T R A N S A C T I O N ;
TRANSFORM           : T R A N S F O R M ;
TRIGGER             : T R I G G E R ;
TRIGGERS            : T R I G G E R S ;
UNCOMMITTED         : U N C O M M I T T E D ;
UNLOCK              : U N L O C K ;
UPDATE              : U P D A T E ;
USER                : U S E R ;
USERS               : U S E R S ;
VERSION             : V E R S I O N ;
WEBSOCKET           : W E B S O C K E T ;
