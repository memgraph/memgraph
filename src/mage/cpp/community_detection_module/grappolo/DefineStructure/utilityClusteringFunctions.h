// ***********************************************************************
//
//            Grappolo: A C++ library for graph clustering
//               Mahantesh Halappanavar (hala@pnnl.gov)
//               Pacific Northwest National Laboratory
//
// ***********************************************************************
//
//       Copyright (2014) Battelle Memorial Institute
//                      All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
// FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
// COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
// ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// ************************************************************************

#ifndef __CLUSTERING__FUNCTIONS__
#define __CLUSTERING__FUNCTIONS__

#include "defs.h"

using namespace std;

void sumVertexDegree(edge* vtxInd, long* vtxPtr, double* vDegree, long NV, Comm* cInfo);

double calConstantForSecondTerm(double* vDegree, long NV);

void initCommAss(long* pastCommAss, long* currCommAss, long NV);

void initCommAssOpt(long* pastCommAss, long* currCommAss, long NV,
		    mapElement* clusterLocalMap, long* vtxPtr, edge* vtxInd,
		    Comm* cInfo, double constant, double* vDegree);

double buildLocalMapCounter(long adj1, long adj2, map<long, long> &clusterLocalMap,
						  vector<double> &Counter, edge* vtxInd, long* currCommAss, long me);

double buildLocalMapCounterNoMap(long v, mapElement* clusterLocalMap, long* vtxPtr, edge* vtxInd,
                               long* currCommAss, long &numUniqueClusters);

long max(map<long, long> &clusterLocalMap, vector<double> &Counter,
		 double selfLoop, Comm* cInfo, double degree, long sc, double constant ) ;

long maxNoMap(long v, mapElement* clusterLocalMap, long* vtxPtr, double selfLoop, Comm* cInfo, double degree,
              long sc, double constant, long numUniqueClusters );

void computeCommunityComparisons(vector<long>& C1, long N1, vector<long>& C2, long N2);

double computeGiniCoefficient(long *colorSize, int numColors);
double computeMerkinMetric(long* C1, long N1, long* C2, long N2);
double computeVanDongenMetric(long* C1, long N1, long* C2, long N2);

//Sorting functions:
void merge(long* arr, long l, long m, long r);
void mergeSort(long* arr, long l, long r);
void SortNeighborListUsingInsertionAndMergeSort(graph *G);
long removeEdges(long NV, long NE, edge *edgeList);
void SortEdgesUndirected(long NV, long NE, edge *list1, edge *list2, long *ptrs);
void SortNodeEdgesByIndex(long NV, edge *list1, edge *list2, long *ptrs);

double* computeEdgeSimilarityMetrics(graph *G);
graph* buildSparifiedGraph(graph *Gin, double alpha);

void buildOld2NewMap(long N, long *C, long *commIndex); //Build the reordering map

#endif
