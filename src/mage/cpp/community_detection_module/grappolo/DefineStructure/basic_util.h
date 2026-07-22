#ifndef __UTILITY__
#define __UTILITY__

// Define in buildNextPhase.cpp
long renumberClustersContiguously(long *C, long size);
double buildNextLevelGraphOpt(graph *Gin, mgp_graph *mg_graph, graph *Gout, long *C, long numUniqueClusters, int nThreads);
void buildNextLevelGraph(graph *Gin, mgp_graph *mg_graph, graph *Gout, long *C, long numUniqueClusters);
long buildCommunityBasedOnVoltages(graph *G, long *Volts, long *C, long *Cvolts);
void segregateEdgesBasedOnVoltages(graph *G, long *Volts);
inline void Visit(long v, long myCommunity, short *Visited, long *Volts,
				  long* vtxPtr, edge* vtxInd, long *C);

// Define in vertexFollowing.cpp
long vertexFollowing(graph *G, long *C);
double buildNewGraphVF(graph *Gin, graph *Gout, long *C, long numUniqueClusters);

// Define in utilityFunctions.cpp
double computeGiniCoefficient(long *colorSize, int numColors);
void generateRandomNumbers(double *RandVec, long size);
void displayGraph(graph *G);
void duplicateGivenGraph(graph *Gin, graph *Gout);
void displayGraphEdgeList(graph *G);
void writeEdgeListToFile(graph *G, FILE* out);
void displayGraphCharacteristics(graph *G);


#endif
