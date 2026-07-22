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

#include "defs.h"
#include "basic_comm.h"
using namespace std;

long vertexFollowing(graph *G, long *C)
{
	long    NV        = G->numVertices;
	long    *vtxPtr   = G->edgeListPtrs;
	edge    *vtxInd   = G->edgeList;
	long numNode = 0;
	double time1 = omp_get_wtime();
// Initialize the Communities
#pragma omp parallel for  //Parallelize on the outer most loop
  	for (long i=0; i<NV; i++) {
		C[i] = i; //Initialize each vertex to its own cluster
	}

// Remove Isolated and degree-one vertices
#pragma omp parallel for
    	for (long i=0; i<NV; i++) {
		long adj1 = vtxPtr[i];
		long adj2 = vtxPtr[i+1];
		if(adj1 == adj2) {	// Isolated vertex
			__sync_fetch_and_add(&numNode, 1);
			C[i] = -1;
		} else {
			if( (adj2-adj1) == 1 ) { //Degree one
			    //Check if the tail has degree greater than one:
			    long tail = vtxInd[adj1].tail;
			    long adj11 = vtxPtr[tail];
			    long adj12 = vtxPtr[tail+1];
                            if( ((adj12-adj11) > 1)||(i > tail) ) { //Degree of tail greater than one
				__sync_fetch_and_add(&numNode, 1);
				C[i] = tail;
			    } //else don't do anything
			}//End of if(degree one)
		}//End of else
	}//End of for(i)

        time1 = omp_get_wtime() - time1;
#ifdef PRINT_DETAILED_STATS_
#endif
	return numNode; //These are nodes that need to be removed
}//End of vertexFollowing()

//WARNING: Will assume that the cluster id have been renumbered contiguously
//Return the total time for building the next level of graph
//This will not add any self-loops
double buildNewGraphVF(graph *Gin, graph *Gout, long *C, long numUniqueClusters) {
  int nT;
#pragma omp parallel
  {
    nT = omp_get_num_threads();
  }
#ifdef PRINT_DETAILED_STATS_
#endif

  double time1, time2, TotTime=0; //For timing purposes
  double total = 0, totItr = 0;
  //Pointers into the input graph structure:
  long    NV_in        = Gin->numVertices;
  long    NE_in        = Gin->numEdges;
  long    *vtxPtrIn    = Gin->edgeListPtrs;
  edge    *vtxIndIn    = Gin->edgeList;

  time1 = omp_get_wtime();
  // Pointers into the output graph structure
  long NV_out = numUniqueClusters;
  long NE_self = 0; //Not all vertices get self-loops
  long NE_out = 0;  //Cross edges
  long *vtxPtrOut = (long *) malloc ((NV_out+1)*sizeof(long));
  assert(vtxPtrOut != 0);
  vtxPtrOut[0] = 0; //First location is always a zero
  /* Step 1 : Regroup the node into cluster node */
  map<long,long>** cluPtrIn = (map<long,long>**) malloc(numUniqueClusters*sizeof(map<long,long>*));
  assert(cluPtrIn != 0);

#pragma omp parallel for
  for (long i=0; i<numUniqueClusters; i++) {
	cluPtrIn[i] = new map<long,long>();
	//Do not add self-loops
        //(*(cluPtrIn[i]))[i] = 0; //Add for a self loop with zero weight
  }
#pragma omp parallel for
  for (long i=1; i<=NV_out; i++)
	vtxPtrOut[i] = 0;

  //Create an array of locks for each cluster
  omp_lock_t *nlocks = (omp_lock_t *) malloc (numUniqueClusters * sizeof(omp_lock_t));
  assert(nlocks != 0);
#pragma omp parallel for
  for (long i=0; i<numUniqueClusters; i++) {
    omp_init_lock(&nlocks[i]); //Initialize locks
  }
  time2 = omp_get_wtime();
  TotTime += (time2-time1);
#ifdef PRINT_DETAILED_STATS_
#endif
  time1 = omp_get_wtime();
#pragma omp parallel for
  for (long i=0; i<NV_in; i++) {
	if((C[i] < 0)||(C[i]>numUniqueClusters))
		continue; //Not a valid cluster id
	long adj1 = vtxPtrIn[i];
	long adj2 = vtxPtrIn[i+1];
	map<long, long>::iterator localIterator;
        assert(C[i] < numUniqueClusters);
  	//Now look for all the neighbors of this cluster
	for(long j=adj1; j<adj2; j++) {
		long tail = vtxIndIn[j].tail;
		assert(C[tail] < numUniqueClusters);
		//Add the edge from one endpoint
		if(C[i] >= C[tail]) {
                        omp_set_lock(&nlocks[C[i]]);  // Locking the cluster

			localIterator = cluPtrIn[C[i]]->find(C[tail]); //Check if it exists
			if( localIterator != cluPtrIn[C[i]]->end() ) {	//Already exists
                                localIterator->second += (long)vtxIndIn[j].weight;
			} else {
				(*(cluPtrIn[C[i]]))[C[tail]] = (long)vtxIndIn[j].weight; //Self-edge
				__sync_fetch_and_add(&vtxPtrOut[C[i]+1], 1);
				if(C[i] == C[tail])
                                	__sync_fetch_and_add(&NE_self, 1); //Keep track of self #edges
				if(C[i] > C[tail]) {
					__sync_fetch_and_add(&NE_out, 1); //Keep track of non-self #edges
					__sync_fetch_and_add(&vtxPtrOut[C[tail]+1], 1); //Count edge j-->i
				}
			}

                        omp_unset_lock(&nlocks[C[i]]); // Unlocking the cluster
		} //End of if
	}//End of for(j)
  }//End of for(i)
  //Prefix sum:
  for(long i=0; i<NV_out; i++) {
	vtxPtrOut[i+1] += vtxPtrOut[i];
  }

  time2 = omp_get_wtime();
  TotTime += (time2-time1);
#ifdef PRINT_DETAILED_STATS_
#endif
  assert(vtxPtrOut[NV_out] == (NE_out*2+NE_self)); //Sanity check

  time1 = omp_get_wtime();
  // Step 3 : build the edge list:
  long numEdges   = vtxPtrOut[NV_out];
  long realEdges  = NE_out + NE_self; //Self-loops appear once, others appear twice
  edge *vtxIndOut = (edge *) malloc (numEdges * sizeof(edge));
  assert (vtxIndOut != 0);
  long *Added = (long *) malloc (NV_out * sizeof(long)); //Keep track of what got added
  assert (Added != 0);

#pragma omp parallel for
  for (long i=0; i<NV_out; i++) {
	Added[i] = 0;
  }
  //Now add the edges in no particular order
#pragma omp parallel for
  for (long i=0; i<NV_out; i++) {
	long Where;
	map<long, long>::iterator localIterator = cluPtrIn[i]->begin();
	//Now go through the other edges:
	while ( localIterator != cluPtrIn[i]->end()) {
		Where = vtxPtrOut[i] + __sync_fetch_and_add(&Added[i], 1);
		vtxIndOut[Where].head = i; //Head
		vtxIndOut[Where].tail = localIterator->first; //Tail
		vtxIndOut[Where].weight = localIterator->second; //Weight
		if(i != localIterator->first) {
			Where = vtxPtrOut[localIterator->first] + __sync_fetch_and_add(&Added[localIterator->first], 1);
			vtxIndOut[Where].head = localIterator->first;
			vtxIndOut[Where].tail = i; //Tail
			vtxIndOut[Where].weight = localIterator->second; //Weight
		}
		localIterator++;
	}
  }//End of for(i)
  time2 = omp_get_wtime();
  TotTime += (time2-time1);
#ifdef PRINT_DETAILED_STATS_
#endif
#ifdef PRINT_TERSE_STATS_
#endif
  // Set the pointers
  Gout->numVertices  = NV_out;
  Gout->sVertices    = NV_out;
  //Note: Self-loops are represented ONCE, but others appear TWICE
  Gout->numEdges     = realEdges; //Add self loops to the #edges
  Gout->edgeListPtrs = vtxPtrOut;
  Gout->edgeList     = vtxIndOut;

  //Clean up
  free(Added);
#pragma omp parallel for
  for (long i=0; i<numUniqueClusters; i++)
	delete cluPtrIn[i];
  free(cluPtrIn);

#pragma omp parallel for
  for (long i=0; i<numUniqueClusters; i++) {
    omp_destroy_lock(&nlocks[i]);
  }
  free(nlocks);

  return TotTime;
}//End of buildNextLevelGraph2()


