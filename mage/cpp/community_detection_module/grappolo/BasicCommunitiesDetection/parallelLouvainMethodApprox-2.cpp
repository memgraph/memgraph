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

#include <mg_procedure.h>
#include "defs.h"
#include "basic_util.h"
#include "utilityClusteringFunctions.h"
#include "basic_comm.h"
#include <stdlib.h>

using namespace std;

double parallelLouvianMethodApprox2(graph *G, mgp_graph *mg_graph, long *C, int nThreads, double Lower,
                                    double thresh, double *totTime, int *numItr, int percentage) {
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
    double time1, time2, time3, time4; //For timing purposes
    double total = 0, totItr = 0;
    //long percentage = clustering_parameters.percentage;
    long    NV        = G->numVertices;
    long    NS        = G->sVertices;
    long    NE        = G->numEdges;
    long    *vtxPtr   = G->edgeListPtrs;
    edge    *vtxInd   = G->edgeList;

    /* Variables for computing modularity */
    long totalEdgeWeightTwice;
    double constantForSecondTerm;
    double prevMod=-1;
    double currMod=-1;
    //double thresMod = 0.000001;
    double thresMod = thresh; //Input parameter
    int numItrs = 0;

    /********************** Initialization **************************/
    time1 = omp_get_wtime();
    //Store the degree of all vertices
    double* vDegree = (double *) malloc (NV * sizeof(double)); assert(vDegree != 0);
    //Community info. (ai and size)
    Comm *cInfo = (Comm *) malloc (NV * sizeof(Comm)); assert(cInfo != 0);
    //use for updating Community
    Comm *cUpdate = (Comm*)malloc(NV*sizeof(Comm)); assert(cUpdate != 0);
    //use for Modularity calculation (eii)
    double* clusterWeightInternal = (double*) malloc (NV*sizeof(double)); assert(clusterWeightInternal != 0);

    sumVertexDegree(vtxInd, vtxPtr, vDegree, NV , cInfo);	// Sum up the vertex degree

    /*** Compute the total edge weight (2m) and 1/2m ***/
    constantForSecondTerm = calConstantForSecondTerm(vDegree, NV); // 1 over sum of the degree

    //cout<<"CHECK THIS:              "<<constantForSecondTerm<<endl;
    //Community assignments:
    //Store previous iteration's community assignment
    long* pastCommAss = (long *) malloc (NV * sizeof(long)); assert(pastCommAss != 0);
    //Store current community assignment
    long* currCommAss = (long *) malloc (NV * sizeof(long)); assert(currCommAss != 0);
    //Store the target of community assignment
    long* targetCommAss = (long *) malloc (NV * sizeof(long)); assert(targetCommAss != 0);

    //Vectors used in place of maps: Total size = |V|+2*|E| -- The |V| part takes care of self loop
    //  mapElement* clusterLocalMapX = (mapElement *) malloc ((NV + 2*NE) * sizeof(mapElement)); assert(clusterLocalMapX != 0);
    //double* Counter             = (double *)     malloc ((NV + 2*NE) * sizeof(double));     assert(Counter != 0);

    //Initialize each vertex to its own cluster
    //initCommAssOpt(pastCommAss, currCommAss, NV, clusterLocalMapX, vtxPtr, vtxInd, cInfo, constantForSecondTerm, vDegree);

    //Initialize each vertex to its own cluster
    initCommAss(pastCommAss, currCommAss, NV);

    //Loop perforation:
    double *randValues = (double *) malloc (NV*sizeof(double)); //Array to hold random number for each vertex

    time2 = omp_get_wtime();

#ifdef PRINT_DETAILED_STATS_
#endif
#ifdef PRINT_TERSE_STATS_
#endif





    //Start maximizing modularity
    while(true) {
        numItrs++;
        time1 = omp_get_wtime();
        /* Re-initialize datastructures */
        generateRandomNumbers(randValues, NV); //Generate random numbers for each iteration
        //so that the same set of vertices are not chosen for skipping computation
#pragma omp parallel for
        for (long i=0; i<NV; i++) {
            clusterWeightInternal[i] = 0;
            cUpdate[i].degree =0;
            cUpdate[i].size =0;
        }
#pragma omp parallel
{
        [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(mg_graph);
#pragma omp for
        for (long i=0; i<NV; i++) {
            if(randValues[i] < percentage) { //process this vertex
                long adj1 = vtxPtr[i];
                long adj2 = vtxPtr[i+1];
                double selfLoop = 0;
                //Build a datastructure to hold the cluster structure of its neighbors
                map<long, long> clusterLocalMap; //Map each neighbor's cluster to a local number
                map<long, long>::iterator storedAlready;
                vector<double> Counter; //Number of edges in each unique cluster
                //Add v's current cluster:
                if(adj1 != adj2){
                    clusterLocalMap[currCommAss[i]] = 0;
                    Counter.push_back(0); //Initialize the counter to ZERO (no edges incident yet)
                    //Find unique cluster ids and #of edges incident (eicj) to them
                    selfLoop = buildLocalMapCounter(adj1, adj2, clusterLocalMap, Counter, vtxInd, currCommAss, i);
                    // Update delta Q calculation
                    clusterWeightInternal[i] += Counter[0]; //(e_ix)
                    //Calculate the max
                    targetCommAss[i] = max(clusterLocalMap, Counter, selfLoop, cInfo, vDegree[i], currCommAss[i], constantForSecondTerm);
                    //assert((targetCommAss[i] >= 0)&&(targetCommAss[i] < NV));
                } else {
                    targetCommAss[i] = -1;
                }
                clusterLocalMap.clear();
                Counter.clear();

                //Update
                if(targetCommAss[i] != currCommAss[i]  && targetCommAss[i] != -1) {
#pragma omp atomic update
                    cUpdate[targetCommAss[i]].degree += vDegree[i];
#pragma omp atomic update
                    cUpdate[targetCommAss[i]].size += 1;
#pragma omp atomic update
                    cUpdate[currCommAss[i]].degree -= vDegree[i];
#pragma omp atomic update
                    cUpdate[currCommAss[i]].size -=1;
                    /*
                     __sync_fetch_and_add(&cUpdate[targetCommAss[i]].size, 1);
                     __sync_fetch_and_sub(&cUpdate[currCommAss[i]].degree, vDegree[i]);
                     __sync_fetch_and_sub(&cUpdate[currCommAss[i]].size, 1);*/
                }//End of If()
            } else { //Do not process this vertex (loop perforation)
                targetCommAss[i]=currCommAss[i];
            }

        }//End of for(i)
        [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(mg_graph);
}
        time2 = omp_get_wtime();

        time3 = omp_get_wtime();
        double e_xx = 0;
        double a2_x = 0;

#pragma omp parallel for \
reduction(+:e_xx) reduction(+:a2_x)
        for (long i=0; i<NV; i++) {
            e_xx += clusterWeightInternal[i];
            a2_x += (cInfo[i].degree)*(cInfo[i].degree);
        }
        time4 = omp_get_wtime();

        currMod = (e_xx*(double)constantForSecondTerm) - (a2_x*(double)constantForSecondTerm*(double)constantForSecondTerm);
        totItr = (time2-time1) + (time4-time3);
        total += totItr;
#ifdef PRINT_DETAILED_STATS_
#endif
#ifdef PRINT_TERSE_STATS_
#endif

        //Break if modularity gain is not sufficient
        if((currMod - prevMod) < thresMod) {
            break;
        }

        //Else update information for the next iteration
        prevMod = currMod;
        if(prevMod < Lower)
            prevMod = Lower;
#pragma omp parallel for
        for (long i=0; i<NV; i++) {
            cInfo[i].size += cUpdate[i].size;
            cInfo[i].degree += cUpdate[i].degree;
        }

        //Do pointer swaps to reuse memory:
        long* tmp;
        tmp = pastCommAss;
        pastCommAss = currCommAss; //Previous holds the current
        currCommAss = targetCommAss; //Current holds the chosen assignment
        targetCommAss = tmp;      //Reuse the vector

    }//End of while(true)
    *totTime = total; //Return back the total time for clustering
    *numItr  = numItrs;

#ifdef PRINT_DETAILED_STATS_
#endif
#ifdef PRINT_TERSE_STATS_
#endif

    //Store back the community assignments in the input variable:
    //Note: No matter when the while loop exits, we are interested in the previous assignment
#pragma omp parallel for
    for (long i=0; i<NV; i++) {
        C[i] = pastCommAss[i];
    }
    //Cleanup
    free(pastCommAss);
    free(currCommAss);
    free(targetCommAss);
    free(vDegree);
    free(cInfo);
    free(cUpdate);
    free(clusterWeightInternal);
    free(randValues);

    return prevMod;
}
