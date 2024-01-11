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

ACTIVE                  : A C T I V E ;
AFTER                   : A F T E R ;
ALTER                   : A L T E R ;
ANALYZE                 : A N A L Y Z E ;
ASYNC                   : A S Y N C ;
AUTH                    : A U T H ;
BAD                     : B A D ;
BATCH_INTERVAL          : B A T C H UNDERSCORE I N T E R V A L ;
BATCH_LIMIT             : B A T C H UNDERSCORE L I M I T ;
BATCH_SIZE              : B A T C H UNDERSCORE S I Z E ;
BEFORE                  : B E F O R E ;
BUILD                   : B U I L D ;
BOOTSTRAP_SERVERS       : B O O T S T R A P UNDERSCORE S E R V E R S ;
CALL                    : C A L L ;
CHECK                   : C H E C K ;
CLEAR                   : C L E A R ;
CLUSTER                 : C L U S T E R ;
COMMIT                  : C O M M I T ;
COMMITTED               : C O M M I T T E D ;
CONFIG                  : C O N F I G ;
CONFIGS                 : C O N F I G S;
CONSUMER_GROUP          : C O N S U M E R UNDERSCORE G R O U P ;
COORDINATOR             : C O O R D I N A T O R ;
CREATE_DELETE           : C R E A T E UNDERSCORE D E L E T E ;
CREDENTIALS             : C R E D E N T I A L S ;
CSV                     : C S V ;
DATA                    : D A T A ;
DO                      : D O ;
DELIMITER               : D E L I M I T E R ;
DATABASE                : D A T A B A S E ;
DATABASES               : D A T A B A S E S ;
DENY                    : D E N Y ;
DIRECTORY               : D I R E C T O R Y ;
DROP                    : D R O P ;
DUMP                    : D U M P ;
DURABILITY              : D U R A B I L I T Y ;
EDGE                    : E D G E ;
EDGE_TYPES              : E D G E UNDERSCORE T Y P E S ;
EXECUTE                 : E X E C U T E ;
FAILOVER                : F A I L O V E R ;
FOR                     : F O R ;
FOREACH                 : F O R E A C H;
FREE                    : F R E E ;
FREE_MEMORY             : F R E E UNDERSCORE M E M O R Y ;
FROM                    : F R O M ;
GLOBAL                  : G L O B A L ;
GRANT                   : G R A N T ;
GRAPH                   : G R A P H ;
GRANTS                  : G R A N T S ;
HEADER                  : H E A D E R ;
IDENTIFIED              : I D E N T I F I E D ;
IGNORE                  : I G N O R E ;
IMPORT                  : I M P O R T ;
INACTIVE                : I N A C T I V E ;
IN_MEMORY_ANALYTICAL    : I N UNDERSCORE M E M O R Y UNDERSCORE A N A L Y T I C A L ;
IN_MEMORY_TRANSACTIONAL : I N UNDERSCORE M E M O R Y UNDERSCORE T R A N S A C T I O N A L ;
ISOLATION               : I S O L A T I O N ;
KAFKA                   : K A F K A ;
LABELS                  : L A B E L S ;
LEVEL                   : L E V E L ;
LOAD                    : L O A D ;
LOCK                    : L O C K ;
MAIN                    : M A I N ;
MODE                    : M O D E ;
MODULE_READ             : M O D U L E UNDERSCORE R E A D ;
MODULE_WRITE            : M O D U L E UNDERSCORE W R I T E ;
MULTI_DATABASE_EDIT     : M U L T I UNDERSCORE D A T A B A S E UNDERSCORE E D I T ;
MULTI_DATABASE_USE      : M U L T I UNDERSCORE D A T A B A S E UNDERSCORE U S E ;
NEXT                    : N E X T ;
NO                      : N O ;
NODE_LABELS             : N O D E UNDERSCORE L A B E L S ;
NOTHING                 : N O T H I N G ;
ON_DISK_TRANSACTIONAL   : O N UNDERSCORE D I S K UNDERSCORE T R A N S A C T I O N A L ;
NULLIF                  : N U L L I F ;
PASSWORD                : P A S S W O R D ;
PORT                    : P O R T ;
PRIVILEGES              : P R I V I L E G E S ;
PULSAR                  : P U L S A R ;
READ                    : R E A D ;
READ_FILE               : R E A D UNDERSCORE F I L E ;
REGISTER                : R E G I S T E R ;
REPLICA                 : R E P L I C A ;
REPLICAS                : R E P L I C A S ;
REPLICATION             : R E P L I C A T I O N ;
REVOKE                  : R E V O K E ;
ROLE                    : R O L E ;
ROLES                   : R O L E S ;
QUOTE                   : Q U O T E ;
SERVER                  : S E R V E R ;
SERVICE_URL             : S E R V I C E UNDERSCORE U R L ;
SESSION                 : S E S S I O N ;
SETTING                 : S E T T I N G ;
SETTINGS                : S E T T I N G S ;
SNAPSHOT                : S N A P S H O T ;
START                   : S T A R T ;
STATISTICS              : S T A T I S T I C S ;
STATS                   : S T A T S ;
STATUS                  : S T A T U S ;
STOP                    : S T O P ;
STORAGE                 : S T O R A G E;
STORAGE_MODE            : S T O R A G E UNDERSCORE MODE;
STREAM                  : S T R E A M ;
STREAMS                 : S T R E A M S ;
SYNC                    : S Y N C ;
TERMINATE               : T E R M I N A T E ;
TIMEOUT                 : T I M E O U T ;
TO                      : T O ;
TOPICS                  : T O P I C S;
TRANSACTION             : T R A N S A C T I O N ;
TRANSACTION_MANAGEMENT  : T R A N S A C T I O N UNDERSCORE M A N A G E M E N T ;
TRANSACTIONS            : T R A N S A C T I O N S ;
TRANSFORM               : T R A N S F O R M ;
TRIGGER                 : T R I G G E R ;
TRIGGERS                : T R I G G E R S ;
UNCOMMITTED             : U N C O M M I T T E D ;
UNLOCK                  : U N L O C K ;
UPDATE                  : U P D A T E ;
USE                     : U S E ;
USER                    : U S E R ;
USERS                   : U S E R S ;
USING                   : U S I N G ;
VERSION                 : V E R S I O N ;
WEBSOCKET               : W E B S O C K E T ;
