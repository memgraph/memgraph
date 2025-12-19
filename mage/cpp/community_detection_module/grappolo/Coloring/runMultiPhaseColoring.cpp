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
#include "basic_comm.h"
#include "color_comm.h"
using namespace std;
//WARNING: This will overwrite the original graph data structure to
//         minimize memory footprint
// Return: C_orig will hold the cluster ids for vertices in the original graph
//         Assume C_orig is initialized appropriately
//WARNING: Graph G will be destroyed at the end of this routine
void runMultiPhaseColoring(graph *G, mgp_graph *mg_graph, long *C_orig, int coloring, int numColors, int replaceMap, long minGraphSize,
                           double threshold, double C_threshold, int numThreads, int threadsOpt)
{
    assert((coloring>0) && (coloring<4)); //Check for the correct coloring specification
    double totTimeClustering=0, totTimeBuildingPhase=0, totTimeColoring=0, tmpTime;
    int tmpItr=0, totItr = 0;
    long NV = G->numVertices;
    //long minGraphSize = 100000; //Need at least 100,000 vertices to turn coloring on

    int *colors = (int *) malloc (G->numVertices * sizeof(int)); assert (colors != 0);
#pragma omp parallel for
    for (long i=0; i<G->numVertices; i++) {
        colors[i] = -1;
    }
    int nColors = 0;
    // Coloring Steps
    if((coloring == 1)||(coloring == 2)) {
        nColors = algoDistanceOneVertexColoringOpt(G, colors, numThreads, &tmpTime)+1;
        totTimeColoring += tmpTime;
        //Check if balanced coloring is enabled:
        if(coloring == 2)
            vBaseRedistribution(G, colors, nColors, 0);
    }
    //Check if incomplete coloring is requested:
    if(coloring == 3) {
        //maxColor = 2 * nHash * nItrs; //Two colors for each hash per iteration
        int nHash = 2; //Use two hash functions
        int nItrs = (int) (ceil(numColors / 4)); //Round off to the number of iterations
        if (nItrs <= 0)
            nItrs = 1;
        //int nItrs = numColors / 4;
        nColors = algoColoringMultiHashMaxMin(G, colors, numThreads, &tmpTime, nHash, nItrs)+1;
        totTimeColoring += tmpTime;
    }

    /* Step 3: Find communities */
    double prevMod = -1;
    double currMod = -1;
    long phase = 1;

    graph *Gnew; //To build new hierarchical graphs
    long numClusters;
    long *C = (long *) malloc (NV * sizeof(long));
    assert(C != 0);
#pragma omp parallel for
    for (long i=0; i<NV; i++) {
        C[i] = -1;
    }

    bool nonColor = false; //Make sure that at least one phase with lower threshold runs
    while(1){
        prevMod = currMod;
        //Compute clusters
        if(nonColor == false) {
			//Use higher modularity for the first few iterations when graph is big enough
        	if (replaceMap == 1)
        		currMod = algoLouvainWithDistOneColoringNoMap(G, C, numThreads, colors, nColors, currMod, C_threshold, &tmpTime, &tmpItr);
        	else
        	    currMod = algoLouvainWithDistOneColoring(G, mg_graph, C, numThreads, colors, nColors, currMod, C_threshold, &tmpTime, &tmpItr);
            totTimeClustering += tmpTime;
            totItr += tmpItr;
        } else {
			if (replaceMap == 1)
		    	currMod = parallelLouvianMethodNoMap(G, C, numThreads, currMod, threshold, &tmpTime, &tmpItr);
        	else
            	currMod = parallelLouvianMethod(G, mg_graph, C, numThreads, currMod, threshold, &tmpTime, &tmpItr);
            totTimeClustering += tmpTime;
            totItr += tmpItr;
            nonColor = true;
        }
        //Renumber the clusters contiguously
        numClusters = renumberClustersContiguously(C, G->numVertices);

        //Keep track of clusters in C_orig
        if(phase == 1) {
#pragma omp parallel for
            for (long i=0; i<NV; i++) {
                C_orig[i] = C[i]; //After the first phase
            }
        } else {
#pragma omp parallel for
            for (long i=0; i<NV; i++) {
                assert(C_orig[i] < G->numVertices);
                if (C_orig[i] >=0)
                    C_orig[i] = C[C_orig[i]]; //Each cluster in a previous phase becomes a vertex
            }
        }
        //Break if too many phases or iterations
        if((phase > 200)||(totItr > 10000)) {
            break;
        }

        //Check for modularity gain and build the graph for next phase
        //In case coloring is used, make sure the non-coloring routine is run at least once
        if( (currMod - prevMod) > threshold ) {
            Gnew = (graph *) malloc (sizeof(graph)); assert(Gnew != 0);
            tmpTime =  buildNextLevelGraphOpt(G, mg_graph, Gnew, C, numClusters, numThreads);
            totTimeBuildingPhase += tmpTime;
            //Free up the previous graph
            free(G->edgeListPtrs);
            free(G->edgeList);
            free(G);
            G = Gnew; //Swap the pointers
            G->edgeListPtrs = Gnew->edgeListPtrs;
            G->edgeList = Gnew->edgeList;
            //Free up the previous cluster & create new one of a different size
            free(C);
            C = (long *) malloc (numClusters * sizeof(long)); assert(C != 0);
#pragma omp parallel for
            for (long i=0; i<numClusters; i++) {
                C[i] = -1;
            }
            phase++; //Increment phase number
            //If coloring is enabled & graph is of minimum size, recolor the new graph
            if((coloring > 0)&&(G->numVertices > minGraphSize)&&(nonColor == false)){
#pragma omp parallel for
                for (long i=0; i<G->numVertices; i++){
                    colors[i] = -1;
                }
                nColors = 0;
                // Coloring Steps
                if((coloring == 1)||(coloring == 2)) {
                    nColors = algoDistanceOneVertexColoringOpt(G, colors, numThreads, &tmpTime)+1;
                    totTimeColoring += tmpTime;
                    //Check if balanced coloring is enabled:
                    if(coloring == 2)
                        vBaseRedistribution(G, colors, nColors, 0);
                }
                //Check if incomplete coloring is requested:
                if(coloring == 3) {
                    //maxColor = 2 * nHash * nItrs; //Two colors for each hash per iteration
                    int nHash = 2; //Use two hash functions
                    int nItrs = (int) (ceil((float)(numColors / 4))); //Round off to the number of iterations
                    if (nItrs <= 0)
                        nItrs = 1;
                    //int nItrs = numColors / 4;
                    nColors = algoColoringMultiHashMaxMin(G, colors, numThreads, &tmpTime, nHash, nItrs)+1;
                    totTimeColoring += tmpTime;
                }
            }
        } else { //To force another phase with coloring again
            if ( (coloring > 0)&&(nonColor == false) ) {
                nonColor = true; //Run at least one loop of non-coloring routine
            }
            else {
                break; //Modularity gain is not enough. Exit.
            }
        }
    } //End of while(1)

    //Clean up:

    free(C);
    if(G != 0) {
        free(G->edgeListPtrs);
        free(G->edgeList);
        free(G);
    }

    if(coloring > 0) {
        if(colors != 0) free(colors);
    }


}//End of runMultiPhaseLouvainAlgorithm()
