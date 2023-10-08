/*
 * When changing this grammar make sure to update constants in
 * src/query/frontend/stripped_lexer_constants.hpp (kKeywords, kSpecialTokens
 * and bitsets) if needed.
 */

lexer grammar CypherLexer ;

import UnicodeCategories ;

/* Skip whitespace and comments. */
Skipped              : ( Whitespace | Comment ) -> skip ;

fragment Whitespace  : '\u0020'
                     | [\u0009-\u000D]
                     | [\u001C-\u001F]
                     | '\u1680' | '\u180E'
                     | [\u2000-\u200A]
                     | '\u2028' | '\u2029'
                     | '\u205F'
                     | '\u3000'
                     | '\u00A0'
                     | '\u202F'
                     ;

fragment Comment     : '/*' .*? '*/'
                     | '//' ~[\r\n]*
                     ;

/* Special symbols. */
LPAREN    : '(' ;
RPAREN    : ')' ;
LBRACK    : '[' ;
RBRACK    : ']' ;
LBRACE    : '{' ;
RBRACE    : '}' ;

COMMA     : ',' ;
DOT       : '.' ;
DOTS      : '..' ;
COLON     : ':' ;
SEMICOLON : ';' ;
DOLLAR    : '$' ;
PIPE      : '|' ;

EQ        : '=' ;
LT        : '<' ;
GT        : '>' ;
LTE       : '<=' ;
GTE       : '>=' ;
NEQ1      : '<>' ;
NEQ2      : '!=' ;
SIM       : '=~' ;

PLUS      : '+' ;
MINUS     : '-' ;
ASTERISK  : '*' ;
SLASH     : '/' ;
PERCENT   : '%' ;
CARET     : '^' ;
PLUS_EQ   : '+=' ;

/* Some random unicode characters that can be used to draw arrows. */
LeftArrowHeadPart  : '⟨' | '〈' | '﹤' | '＜' ;
RightArrowHeadPart : '⟩' | '〉' | '﹥' | '＞' ;
DashPart           : '­' | '‐' | '‑' | '‒' | '–' | '—' | '―'
                   | '−' | '﹘' | '﹣' | '－'
                   ;

/* Cypher reserved words. */
ALL            : A L L ;
AND            : A N D ;
ANY            : A N Y ;
AS             : A S ;
ASC            : A S C ;
ASCENDING      : A S C E N D I N G ;
ASSERT         : A S S E R T ;
BFS            : B F S ;
BY             : B Y ;
CALL           : C A L L ;
CASE           : C A S E ;
COALESCE       : C O A L E S C E ;
CONSTRAINT     : C O N S T R A I N T ;
CONTAINS       : C O N T A I N S ;
COUNT          : C O U N T ;
CREATE         : C R E A T E ;
CYPHERNULL     : N U L L ;
DELETE         : D E L E T E ;
DESC           : D E S C ;
DESCENDING     : D E S C E N D I N G ;
DETACH         : D E T A C H ;
DISTINCT       : D I S T I N C T ;
DROP           : D R O P ;
ELSE           : E L S E ;
END            : E N D ;
ENDS           : E N D S ;
EXISTS         : E X I S T S ;
EXPLAIN        : E X P L A I N ;
EXTRACT        : E X T R A C T ;
FALSE          : F A L S E ;
FILTER         : F I L T E R ;
IN             : I N ;
INDEX          : I N D E X ;
INFO           : I N F O ;
IS             : I S ;
KB             : K B ;
KEY            : K E Y ;
LIMIT          : L I M I T ;
L_SKIP         : S K I P ;
MATCH          : M A T C H ;
MB             : M B ;
MEMORY         : M E M O R Y ;
MERGE          : M E R G E ;
NODE           : N O D E ;
NONE           : N O N E ;
NOT            : N O T ;
ON             : O N ;
OPTIONAL       : O P T I O N A L ;
OR             : O R ;
ORDER          : O R D E R ;
PROCEDURE      : P R O C E D U R E ;
PROFILE        : P R O F I L E ;
QUERY          : Q U E R Y ;
REDUCE         : R E D U C E ;
REMOVE         : R E M O V E ;
RETURN         : R E T U R N ;
SET            : S E T ;
SHOW           : S H O W ;
SINGLE         : S I N G L E ;
STARTS         : S T A R T S ;
STORAGE        : S T O R A G E ;
THEN           : T H E N ;
TRUE           : T R U E ;
UNION          : U N I O N ;
UNIQUE         : U N I Q U E ;
UNLIMITED      : U N L I M I T E D ;
UNWIND         : U N W I N D ;
WHEN           : W H E N ;
WHERE          : W H E R E ;
WITH           : W I T H ;
WSHORTEST      : W S H O R T E S T ;
ALLSHORTEST    : A L L S H O R T E S T ;
XOR            : X O R ;
YIELD          : Y I E L D ;

/* Double and single quoted string literals. */
StringLiteral : '"'  ( ~[\\"] | EscapeSequence )* '"'
              | '\'' ( ~[\\'] | EscapeSequence )* '\''
              ;

fragment EscapeSequence : '\\' ( B | F | N | R | T | '\\' | '\'' | '"' )
                        | '\\u' HexDigit HexDigit HexDigit HexDigit
                        | '\\U' HexDigit HexDigit HexDigit HexDigit
                                HexDigit HexDigit HexDigit HexDigit
                        ;

/* Number literals. */
DecimalLiteral     : '0' | NonZeroDigit ( DecDigit )* ;
OctalLiteral       : '0' ( OctDigit )+ ;
HexadecimalLiteral : '0x' ( HexDigit )+ ;
FloatingLiteral    : DecDigit* '.' DecDigit+ ( E '-'? DecDigit+ )?
                   | DecDigit+ ( '.' DecDigit* )? ( E '-'? DecDigit+ )
                   | DecDigit+ ( E '-'? DecDigit+ )
                   ;

fragment NonZeroDigit : [1-9] ;
fragment DecDigit     : [0-9] ;
fragment OctDigit     : [0-7] ;
fragment HexDigit     : [0-9] | [a-f] | [A-F] ;

/* Symbolic names. */
UnescapedSymbolicName : IdentifierStart ( IdentifierPart )* ;
EscapedSymbolicName   : ( '`' ~[`]* '`' )+ ;

/**
 * Based on the unicode identifier and pattern syntax
 * (http://www.unicode.org/reports/tr31/)
 * and extended with a few characters.
 */
IdentifierStart : ID_Start    | Pc ;
IdentifierPart  : ID_Continue | Sc ;
UNDERSCORE : '_' ;

/* Hack for case-insensitive reserved words */
fragment A : 'A' | 'a' ;
fragment B : 'B' | 'b' ;
fragment C : 'C' | 'c' ;
fragment D : 'D' | 'd' ;
fragment E : 'E' | 'e' ;
fragment F : 'F' | 'f' ;
fragment G : 'G' | 'g' ;
fragment H : 'H' | 'h' ;
fragment I : 'I' | 'i' ;
fragment J : 'J' | 'j' ;
fragment K : 'K' | 'k' ;
fragment L : 'L' | 'l' ;
fragment M : 'M' | 'm' ;
fragment N : 'N' | 'n' ;
fragment O : 'O' | 'o' ;
fragment P : 'P' | 'p' ;
fragment Q : 'Q' | 'q' ;
fragment R : 'R' | 'r' ;
fragment S : 'S' | 's' ;
fragment T : 'T' | 't' ;
fragment U : 'U' | 'u' ;
fragment V : 'V' | 'v' ;
fragment W : 'W' | 'w' ;
fragment X : 'X' | 'x' ;
fragment Y : 'Y' | 'y' ;
fragment Z : 'Z' | 'z' ;
