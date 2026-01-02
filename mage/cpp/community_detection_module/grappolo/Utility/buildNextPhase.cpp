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
#include <mg_procedure.h>
#include "basic_util.h"
using namespace std;

//WARNING: Will overwrite the old cluster vector
//Returns the number of unique clusters
long renumberClustersContiguously(long *C, long size) {
#ifdef PRINT_DETAILED_STATS_
#endif
    double time1 = omp_get_wtime();
    //Count the number of unique communities and internal edges
    map<long, long> clusterLocalMap; //Map each neighbor's cluster to a local number
    map<long, long>::iterator storedAlready;
    long numUniqueClusters = 0;

    //Do this loop in serial
    //Will overwrite the old cluster id with the new cluster id
    for(long i=0; i<size; i++) {
        assert(C[i]<size);
        if (C[i] >= 0) { //Only if it is a valid number
            storedAlready = clusterLocalMap.find(C[i]); //Check if it already exists
            if( storedAlready != clusterLocalMap.end() ) {	//Already exists
                C[i] = storedAlready->second; //Renumber the cluster id
            } else {
                clusterLocalMap[C[i]] = numUniqueClusters; //Does not exist, add to the map
                C[i] = numUniqueClusters; //Renumber the cluster id
                numUniqueClusters++; //Increment the number
            }
        }//End of if()
    }//End of for(i)
    time1 = omp_get_wtime() - time1;
#ifdef PRINT_DETAILED_STATS_
#endif

    return numUniqueClusters; //Return the number of unique cluster ids
}//End of renumberClustersContiguously()

//WARNING: Will assume that the cluster id have been renumbered contiguously
//Return the total time for building the next level of graph
double buildNextLevelGraphOpt(graph *Gin, mgp_graph *mg_graph, graph *Gout, long *C, long numUniqueClusters, int nThreads) {

#ifdef PRINT_DETAILED_STATS_
#endif
    if (nThreads < 1)
        omp_set_num_threads(1);
    else
        omp_set_num_threads(nThreads);
    int nT;
#pragma omp parallel
    {
        nT = omp_get_num_threads();
    }
#ifdef PRINT_DETAILED_STATS_
#endif
    long percentage = 80;
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
    long NE_out = 0;
    long *vtxPtrOut = (long *) malloc ((NV_out+1)*sizeof(long)); assert(vtxPtrOut != 0);
    vtxPtrOut[0] = 0; //First location is always a zero
    /* Step 1 : Regroup the node into cluster node */
    map<long,double>** cluPtrIn = (map<long,double>**) malloc (numUniqueClusters*sizeof(map<long,double>*));
    assert(cluPtrIn != 0);

#pragma omp parallel
{
  [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(mg_graph);
#pragma omp for
    for (long i=0; i<numUniqueClusters; i++) {
        cluPtrIn[i] = new map<long,double>();
        (*(cluPtrIn[i]))[i] = 0; //Add for a self loop with zero weight
    }
  [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(mg_graph);
}
#pragma omp parallel for
    for (long i=1; i<=NV_out; i++)
        vtxPtrOut[i] = 1; //Count self-loops for every vertex

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

#pragma omp parallel
{
    [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(mg_graph);
#pragma omp for
    for (long i=0; i<NV_in; i++) {
        long adj1 = vtxPtrIn[i];
        long adj2 = vtxPtrIn[i+1];
        map<long, double>::iterator localIterator;
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
                    //				  (*(cluPtrIn[C[i]]))[C[tail]] += (long)vtxIndIn[j].weight;
                    localIterator->second += vtxIndIn[j].weight;
                } else {
                    (*(cluPtrIn[C[i]]))[C[tail]] = vtxIndIn[j].weight; //Add edge i-->j
                    __sync_fetch_and_add(&vtxPtrOut[C[i]+1], 1);
                    if(C[i] > C[tail]) {
                        __sync_fetch_and_add(&NE_out, 1); //Keep track of non-self #edges
                        __sync_fetch_and_add(&vtxPtrOut[C[tail]+1], 1); //Count edge j-->i
                    }
                }
                omp_unset_lock(&nlocks[C[i]]); // Unlocking the cluster
            }//End of if
        }//End of for(j)
    }//End of for(i)
    [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(mg_graph);
}

    //Prefix sum:
    for(long i=0; i<NV_out; i++) {
        vtxPtrOut[i+1] += vtxPtrOut[i];
    }

    time2 = omp_get_wtime();
    TotTime += (time2-time1);

#ifdef PRINT_DETAILED_STATS_
#endif
    assert(vtxPtrOut[NV_out] == (NE_out*2+NV_out)); //Sanity check
    time1 = omp_get_wtime();

    // Step 3 : build the edge list:
    long numEdges   = vtxPtrOut[NV_out];
    long realEdges  = numEdges - NE_out; //Self-loops appear once, others appear twice
    edge *vtxIndOut = (edge *) malloc (numEdges * sizeof(edge));
    assert (vtxIndOut != 0);
    long *Added = (long *) malloc (NV_out * sizeof(long)); //Keep track of what got added
    assert (Added != 0);

#pragma omp parallel for
    for (long i=0; i<NV_out; i++) {
        Added[i] = 0;
    }

    //Now add the edges in no particular order
#pragma omp parallel
{
  [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(mg_graph);
#pragma omp for
    for (long i=0; i<NV_out; i++) {
        long Where;
        map<long, double>::iterator localIterator = cluPtrIn[i]->begin();
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
  [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(mg_graph);
}
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
#pragma omp parallel
{
    [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(mg_graph);
#pragma omp for
    for (long i=0; i<numUniqueClusters; i++)
        delete cluPtrIn[i];
    [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(mg_graph);
}
    free(cluPtrIn);

#pragma omp parallel for
    for (long i=0; i<numUniqueClusters; i++) {
        omp_destroy_lock(&nlocks[i]);
    }

    free(nlocks);

    return TotTime;
}//End of buildNextLevelGraph2()

//WARNING: Will assume that the cluster ids have been renumbered contiguously
void buildNextLevelGraph(graph *Gin, graph *Gout, long *C, long numUniqueClusters) {
#ifdef PRINT_DETAILED_STATS_
#endif
    double time1, time2, time3, time4; //For timing purposes
    double total = 0, totItr = 0;
    long percentage = 80;
    //Pointers into the input graph structure:
    long    NV_in        = Gin->numVertices;
    long    NE_in        = Gin->numEdges;
    long    *vtxPtrIn    = Gin->edgeListPtrs;
    edge    *vtxIndIn    = Gin->edgeList;

    long vecSize = (numUniqueClusters*(numUniqueClusters+1))/2;
    long *tmpCounter = (long *) malloc ( vecSize * sizeof(long));
    assert(tmpCounter != 0);

    /////STEP-1: Count the number of internal edges and cut edges (edges between two clusters):
    /////Stored in a lower triangular matrix (complete matrix). Size of matrix = |#of clusters|
    ///// The diagonal entries in the matrix store the number of internal edges of a cluster
    ///// Off-diagonal entries are from i-->j iff i>j
#pragma omp parallel for
    for (long i=0; i<vecSize; i++)
        tmpCounter[i] = 0;

#pragma omp parallel for
    for (long i=0; i<NV_in; i++) {
        long adj1 = vtxPtrIn[i];
        long adj2 = vtxPtrIn[i+1];
        for(long j=adj1; j<adj2; j++) {
            long tail = vtxIndIn[j].tail;
            assert((C[i] < numUniqueClusters)&&(C[tail] < numUniqueClusters));
            if(C[i] == C[tail]) { //Internal edge
                long location = (C[i]*(C[i]+1))/2 + C[i]; //Diagonal element
                __sync_fetch_and_add(&tmpCounter[location], vtxIndIn[j].weight);
            } else {
                //One store it one way: if C[i] > C[tail]
                if(C[i] > C[tail] ) {
                    long location = (C[i]*(C[i]+1))/2 + C[tail];
                    __sync_fetch_and_add(&tmpCounter[location], vtxIndIn[j].weight);
                }//End of if
            }//End of else
        }//End of for(j)
    }//End of for(i)

    /////STEP-2: Build the graph data structure:
    long NV_out = numUniqueClusters;
    long NE_out = 0;
    long *vtxPtrOut = (long *) malloc ((NV_out+1)*sizeof(long));
    assert(vtxPtrOut != 0);
    vtxPtrOut[0] = 0; //First location is always a zero
#pragma omp parallel for
    for (long i=1; i<=NV_out; i++)
        vtxPtrOut[i] = 1; //Count self loops

    //Count the number of edges:
#pragma omp parallel for
    for (long i=0; i<NV_out; i++) {
        long location = (i*(i+1))/2; //Starting location for i
        for (long j=0; j<i; j++) {
            if(i == j)
                continue; //Self-loops have already been counted
            if (tmpCounter[location+j] > 0) {
                __sync_fetch_and_add(&vtxPtrOut[i+1], 1); //Add edge i-->j (leave the zeroth location as-is
                __sync_fetch_and_add(&vtxPtrOut[j+1], 1); //Add edge j-->i
                __sync_fetch_and_add(&NE_out, 1); //Keep track of #edges
            }
        }//End of for(j)
    }//End of for(i)

    //Prefix sum:
    for(long i=0; i<NV_out; i++) {
        vtxPtrOut[i+1] += vtxPtrOut[i];
    }
    assert(vtxPtrOut[NV_out] == (NE_out*2+NV_out)); //Sanity check

    //Now build the edge list:
    long numEdges = NE_out*2 + NV_out; //Self-loops appear once, others appear twice
    edge *vtxIndOut = (edge *) malloc (numEdges * sizeof(edge));
    long *Added = (long *) malloc (NV_out * sizeof(long)); //Keep track of what got added
#pragma omp parallel for
    for (long i=0; i<NV_out; i++)
        Added[i] = 0;

    //Now add the edges: NOT IN SORTED ORDER
#pragma omp parallel for
    for (long i=0; i<NV_out; i++) {
        long location = (i*(i+1))/2; //Starting location for i
        //Add the self-loop: i-i
        long Where = vtxPtrOut[i] + __sync_fetch_and_add(&Added[i], 1);

        vtxIndOut[Where].head = i;
        vtxIndOut[Where].tail = i;
        if (tmpCounter[location+i] == 0)
        {
            //tmpCounter[location+i] = tmpCounter[location+i]/2;
        }
        vtxIndOut[Where].weight = tmpCounter[location+i];
        //Now go through the other edges:
        for (long j=0; j<i; j++) {
            if (tmpCounter[location+j] > 0) {
                Where = vtxPtrOut[i] + __sync_fetch_and_add(&Added[i], 1);
                vtxIndOut[Where].head = i; //Head
                vtxIndOut[Where].tail = j; //Tail
                vtxIndOut[Where].weight = tmpCounter[location+j]; //Weight

                Where = vtxPtrOut[j] + __sync_fetch_and_add(&Added[j], 1);
                vtxIndOut[Where].head = j; //Head
                vtxIndOut[Where].tail = i; //Tail
                vtxIndOut[Where].weight = tmpCounter[location+j]; //Weight
            }//End of if
        }//End of for(j)
    }//End of for(i)

    // Set the pointers
    Gout->numVertices  = NV_out;
    Gout->sVertices    = NV_out;
    //Note: Self-loops are represented ONCE, but others appear TWICE
    Gout->numEdges     = NE_out + NV_out; //Add self loops to the #edges
    Gout->edgeListPtrs = vtxPtrOut;
    Gout->edgeList     = vtxIndOut;

    //Clean up
    free(Added);
    free(tmpCounter);


}//End of buildNextLevelGraph

//This code is for finding communities in power grids based on
//voltage levels of nodes. All nodes in a connected components with
//the same voltage belong to a community: Complexity is O(|E|)
long buildCommunityBasedOnVoltages(graph *G, long *Volts, long *C, long *Cvolts) {
    /* Graph data structure */
    long    NV        = G->numVertices;
    long    NE        = G->numEdges;
    long    *vtxPtr   = G->edgeListPtrs;
    edge    *vtxInd   = G->edgeList;

    short *Visited = (short *) malloc (NV * sizeof(short));
    for (long i=0; i<NV; i++) {
        C[i] = -1;
        Visited[i] = 0;
    }
    long myCommunity = 0;
    bool found = false; //Make sure that a community with no vertices does not exist
    for (long v=0; v<NV; v++) {
        if (Visited[v] > 0) //Already visited
            continue;
        //Find the community of this vertex
        C[v] = myCommunity; assert(Volts[v] > 0);
        Cvolts[myCommunity] = Volts[v]; //Store the voltage of that community
        Visited[v] = 1;
        found = true; //At least one vertex exists in this community
        Visit(v, myCommunity, Visited, Volts, vtxPtr, vtxInd, C); //Find others
        if (found) {
            myCommunity++; //Increment to next community
            found = false;
        }
    }
    free(Visited);
    return myCommunity;
}

//Recursive call for finding neighbors
inline void Visit(long v, long myCommunity, short *Visited, long *Volts,
                  long* vtxPtr, edge* vtxInd, long *C) {
    long adj1 = vtxPtr[v];   //Beginning
    long adj2 = vtxPtr[v+1]; //End
    for(long i=adj1; i<adj2; i++) {
        long w = vtxInd[i].tail;
        if (Visited[w] != 0) { //Already visited?
            continue;
        }
        if (Volts[v] == Volts[w]) { //belong to the same community
            C[w] = myCommunity; //Set the community
            Visited[w] = 1; //Mark as visited
            Visit(w, myCommunity, Visited, Volts, vtxPtr, vtxInd, C); //Find others recursively
        }
    }//End of for(i)
}//End of Visit

void segregateEdgesBasedOnVoltages(graph *G, long *Volts) {
    /* Graph data structure */
    long    NV        = G->numVertices;
    long    NE        = G->numEdges;
    long    *vtxPtr   = G->edgeListPtrs;
    edge    *vtxInd   = G->edgeList;

    //Count the number of unique communities and internal edges
    map<long, long> voltsMap; //Map each neighbor's cluster to a local number
    map<long, long>::iterator storedAlready;

    //voltsMap[0] = 0;
    //long numUniqueVolts = 1;

    long numUniqueVolts = 0;
    //Do this loop in serial
    //Will overwrite the old cluster id with the new cluster id
    for (long i=0; i<NV; i++) {
        storedAlready = voltsMap.find(Volts[i]); //Check if it already exists
        if( storedAlready == voltsMap.end() ) {	//Does not exist, add to the map
            voltsMap[Volts[i]] = numUniqueVolts;
            numUniqueVolts++;
        }
    }//End of for(i)

    storedAlready = voltsMap.begin();
    do {
        long myVolt = storedAlready->first;
        for (long v=0; v<NV; v++) {
            long adj1 = vtxPtr[v];
            long adj2 = vtxPtr[v+1];
            for(long j=adj1; j<adj2; j++) {
                long w = vtxInd[j].tail;
                if ((Volts[v]==myVolt)&&(Volts[w]==myVolt)) {
                }
            }//End of for(j)
        }//End of for(v)
        storedAlready++; //Go to the next cluster
    } while ( storedAlready != voltsMap.end() );
}//End of segregateEdgesBasedOnVoltages()
