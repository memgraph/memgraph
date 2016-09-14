#pragma once

// FROM: A Malloc Tutorial, Marwan Burelle, 2009

// align address x to 4 bytes
#define align_4(x) (((((x) - 1)>>2)<<2)+4)

// align address x to 8 bytes
#define align_8(x) (((((x) - 1)>>3)<<3)+8)
