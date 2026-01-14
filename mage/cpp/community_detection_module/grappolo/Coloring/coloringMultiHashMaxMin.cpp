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
#include "coloring.h"
#include "stdlib.h"
#include "time.h"
//Return the number of colors used (zero is a valid color)
//Algorithm: Adaptation of Luby-Jones-Plusman
//Source: http://on-demand.gputechconf.com/gtc/2012/presentations/S0332-Efficient-Graph-Matching-and-Coloring-on-GPUs.pdf
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////

void generateRandomNumbers2(double* randValues, long NVer)
{
    for(int v = 0; v<NVer; v++)
    {
        randValues[v] = (double)rand();
    }
}

int algoColoringMultiHashMaxMin(graph *G, int *vtxColor, int nThreads, double *totTime, int nHash, int nItrs)
{
#ifdef PRINT_DETAILED_STATS_
    std::cout << "Within algoColoringMultiHashMaxMin(nHash= " << nHash << " -- nItrs= " << nItrs << ")\n";
#endif

    if (nThreads < 1)
        omp_set_num_threads(1); //default to one thread
    else
        omp_set_num_threads(nThreads);
    int nT;

#pragma omp parallel
    {
        nT = omp_get_num_threads();
    }

#ifdef PRINT_DETAILED_STATS_
#endif
    assert(nItrs > 0); assert(nHash > 0);
    double time1=0, time2=0, totalTime=0;
    //Get the iterators for the graph:
    long NVer    = G->numVertices;
    long NEdge   = G->numEdges;
    long *verPtr = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
    edge *verInd = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)

    int maxColor = (2 * nHash * nItrs); //Two colors for each hash per iteration; zero is a valid color
    int totalColored = 0;
#ifdef PRINT_DETAILED_STATS_
#endif

    //Build a vector of random numbers:
    //Note: Cheating a little bit now -- need to fix this with a hash function
    /*
     double *randValues = (double*) malloc (NVer * sizeof(double));
     assert(randValues != 0); */
    time1 = omp_get_wtime();
    double **randValuesPtr = (double**) malloc(nHash * sizeof(double*));
    for(int i=0; i < nHash; i++)
    {
        randValuesPtr[i] = (double*) malloc (NVer * sizeof(double));
        generateRandomNumbers2(randValuesPtr[i], NVer);
    }
    time2 = omp_get_wtime();
    totalTime = time2-time1;
#ifdef PRINT_DETAILED_STATS_
#endif
    //Color all the vertices to a maximum number (means that the vertex did not get colored)
#pragma omp parallel for
    for (long v=0; v<NVer; v++) {
        vtxColor[v] = maxColor; //Set the color to maximum
    }
    int iterFreq = 0;
    //Loop through the iterations:
    for (int itr=0; itr<nItrs; itr++) {
        //Iterate for the number of hashes
        time1 = omp_get_wtime();
        for (int ihash=0; ihash<nHash; ihash++) {
            int currentColor = (2*itr*nHash + 2*ihash); //Color to be used in current itr-hash combination
#pragma omp parallel for
            for (long v=0; v<NVer; v++) {
                //Iterate over all the vertices:
                //Check if this vertex has already been colored
                if(vtxColor[v] != maxColor)
                    continue; //The vertex has already been colored
                //Vertex v has not been colored. Check to see if it is a local max or a local min
                long adj1 = verPtr[v];
                long adj2 = verPtr[v+1];
                //Browse the adjacency set of vertex v
                bool isMax = true, isMin = true;
                for(long k = adj1; k < adj2; k++ ) {
                    if ( v == verInd[k].tail ) //Self-loops
                        continue;
                    //if(vtxColor[verInd[k].tail] < maxColor)
                    if(vtxColor[verInd[k].tail] < currentColor) //Colored in previous iterations
                        continue; //It has already been colored -- ignore this neighbor
                    if ( randValuesPtr[ihash][v] <= randValuesPtr[ihash][verInd[k].tail] ) {
                        isMax = false;
                    }
                    if ( randValuesPtr[ihash][v] >= randValuesPtr[ihash][verInd[k].tail] ) {
                        isMin = false;
                    }
                    //Corner case: if all neighbors have been colored,
                    //both isMax and isMin will be true, but it doesn't matter
                }//End of for(k)
                if (isMax == true) {
                    vtxColor[v] = currentColor;
                    __sync_fetch_and_add(&iterFreq,1);
                } else if (isMin == true) {
                    vtxColor[v] = currentColor+1;
                    __sync_fetch_and_add(&iterFreq,1);
                }
            }//End of for(v)

        }//End of for(ihash)
        totalColored += iterFreq;
        time2 = omp_get_wtime();
        totalTime = time2-time1;
        if(iterFreq == 0) {
            if(totalColored == NVer) {
                maxColor = (2*(itr-1)*nHash) + 2*nHash + 1;
                break;
            }
        } else {
            iterFreq = 0; //reset the counter
        }
    } //End of for(itr)

    //Verify Results and Cleanup
    long myConflicts = 0;
    long unColored = 0;
#pragma omp parallel for
    for (long v=0; v < NVer; v++ ) {
        long adj1 = verPtr[v];
        long adj2 = verPtr[v+1];
        if ( vtxColor[v] == maxColor ) {//Ignore uncolored vertices
            __sync_fetch_and_add(&unColored, 1);
            continue;
        }
        //Browse the adjacency set of vertex v
        for(long k = adj1; k < adj2; k++ ) {
            if ( v == verInd[k].tail ) //Self-loops
                continue;
            if ( vtxColor[v] == vtxColor[verInd[k].tail] ) {
                __sync_fetch_and_add(&myConflicts, 1); //increment the counter
            }
        }//End of inner for loop: w in adj(v)
    }//End of outer for loop: for each vertex

    myConflicts = myConflicts / 2; //Have counted each conflict twice

#ifdef PRINT_DETAILED_STATS_
#endif
    *totTime = totalTime;

    //Cleanup:
    for(int i = 0; i < nHash; i++)
    {
        //	for(int j =0; j < NVer; j++)
        if (randValuesPtr[i] != 0)
            free(randValuesPtr[i]);
    }
    if (randValuesPtr != 0)
        free(randValuesPtr);
    return maxColor; //Return the number of colors used (maxColor is also a valid color)

}//End of algoColoringMultiHashMaxMin()


