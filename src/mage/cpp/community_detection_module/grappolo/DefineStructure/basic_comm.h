#ifndef __BASIC__COMM__
#define __BASIC__COMM__

// Define in louvainMultiPhaseRun.cpp
void runMultiPhaseBasic(graph *G, mgp_graph *mg_graph, long *C_orig, int basicOpt, long minGraphSize,
			double threshold, double C_threshold, int numThreads, int threadsOpt);

// same as above, but runs exactly one phase
void runMultiPhaseBasicOnce(graph *G, mgp_graph *mg_graph, long *C_orig, int basicOpt, long minGraphSize,
			double threshold, double C_threshold, int numThreads, int threadsOpt);

// uses Granell, Arenas, et al. Fast track resistance
void runMultiPhaseBasicFastTrackResistance(graph *G, mgp_graph *mg_graph, long *C_orig, int basicOpt, long minGraphSize,
			double threshold, double C_threshold, int numThreads, int threadsOpt);


void runMultiPhaseBasicApprox(graph *G, mgp_graph *mg_graph, long *C_orig, int basicOpt, long minGraphSize,
			double threshold, double C_threshold, int numThreads, int threadsOpt, int percentage);

// Define in parallelLouvianMethod.cpp
double parallelLouvianMethod(graph *G, mgp_graph *mg_graph, long *C, int nThreads, double Lower,
				double thresh, double *totTime, int *numItr);

// Define in parallelLouvianMethodApprox.cpp
double parallelLouvianMethodApprox(graph *G, mgp_graph *mg_graph, long *C, int nThreads, double Lower,
				double thresh, double *totTime, int *numItr, int percentage);

double parallelLouvianMethodNoMap(graph *G, long *C, int nThreads, double Lower,
				double thresh, double *totTime, int *numItr);

double parallelLouvianMethodScale(graph *G, mgp_graph *mg_graph, long *C, int nThreads, double Lower,
				double thresh, double *totTime, int *numItr);

// implements Granell, Arenas, et al. Fast track resistance
// Granell, Clara, Sergio Gomez, and Alex Arenas. "Hierarchical multiresolution method to
// overcome the resolution limit in complex networks." International Journal of Bifurcation
// and Chaos 22, no. 07 (2012): 1250171.

// Define in parallelLouvianMethodFastTrackResistance.cpp
double parallelLouvianMethodFastTrackResistance(graph *G, mgp_graph *mg_graph, long *C, int nThreads, double Lower,
        double thresh, double *totTime, int *numItr, int phase, double* rmin, double* finMod);

// Define in parallelLouvianMethodNoMapFastTrackResistance.cpp
double parallelLouvianMethodNoMapFastTrackResistance(graph *G, long *C, int nThreads, double Lower,
        double thresh, double *totTime, int *numItr, int phase, double* rmin, double* finMod);

// Define in parallelLouvianMethodScaleFastTrackResistance.cpp
double parallelLouvianMethodScaleFastTrackResistance(graph *G, mgp_graph *mg_graph, long *C, int nThreads, double Lower,
        double thresh, double *totTime, int *numItr, int phase, double* rmin, double* finMod);

#endif
