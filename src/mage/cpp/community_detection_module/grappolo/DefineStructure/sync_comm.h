#ifndef __sync_comm__
#define __sync_comm__

#include <mg_procedure.h>
#include "basic_util.h"
#include "utilityClusteringFunctions.h"

void runMultiPhaseSyncType(graph *G, mgp_graph *mg_graph, long *C_orig, int syncType, long minGraphSize,
			double threshold, double C_threshold, int numThreads, int threadsOpt);

double parallelLouvainMethodFullSyncEarly(graph *G, long *C, int nThreads, double Lower,
				double thresh, double *totTime, int *numItr,int ytype, int freedom);

double parallelLouvainMethodFullSync(graph *G, long *C, int nThreads, double Lower,
				double thresh, double *totTime, int *numItr,int ytype, int freedom);

double parallelLouvianMethodEarlyTerminate(graph *G, long *C, int nThreads, double Lower,
				double thresh, double *totTime, int *numItr);

// Define in fullSyncUtility.cpp
double buildAndLockLocalMapCounter(long v, mapElement* clusterLocalMap, long* vtxPtr, edge* vtxInd,
                               long* currCommAss, long &numUniqueClusters, omp_lock_t* vlocks, omp_lock_t* clocks, int ytype, double& eix, int freedom);

void maxAndFree(long v, mapElement* clusterLocalMap, long* vtxPtr, edge* vtxInd, double selfLoop, Comm* cInfo, long* CA,
							double constant, long numUniqueClusters, omp_lock_t* vlocks, omp_lock_t* clocks, int ytype, double eix, double* vDegree);

#endif
