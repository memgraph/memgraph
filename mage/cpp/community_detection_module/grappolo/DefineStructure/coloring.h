#ifndef __coloring__
#define __coloring__

#include "basic_util.h"
#include "coloringUtils.h"

// In coloringDistanceOne.cpp
int algoDistanceOneVertexColoringOpt(graph *G, int *vtxColor, int nThreads, double *totTime);
int algoDistanceOneVertexColoring(graph *G, int *vtxColor, int nThreads, double *totTime);

// In ColoringMultiHasMaxMin.cpp
int algoColoringMultiHashMaxMin(graph *G, int *vtxColor, int nThreads, double *totTime, int nHash, int nItrs);

// In vBase.cpp
int vBaseRedistribution(graph* G, int* vtxColor, int ncolors, int type);

// In equitableColoringDistanceOne.cpp
void buildColorSize(long NVer, int *vtxColor, int numColors, long *colorSize);
void computeVariance(long NVer, int numColors, long *colorSize);

void equitableDistanceOneColorBased(graph *G, mgp_graph *mg_graph, int *vtxColor, int numColors, long *colorSize,
				    int nThreads, double *totTime, int type);

#endif
