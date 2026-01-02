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

using namespace std;

// Merges two subarrays of arr[].
// First subarray is arr[l..m]
// Second subarray is arr[m+1..r]
// Code from: http://www.geeksforgeeks.org/merge-sort/
void merge(edge *arr, long l, long m, long r) {
    long i, j, k;
    long n1 = m - l + 1;
    long n2 =  r - m;
    
    /* create temp arrays */
    //long L[n1], R[n2];
    edge *L = (edge *) malloc( n1 * sizeof(edge)); assert(L != 0);
    edge *R = (edge *) malloc( n2 * sizeof(edge)); assert(R != 0);
    
    /* Copy data to temp arrays L[] and R[] */
    for (i = 0; i < n1; i++) {
        //L[i] = arr[l + i];
        L[i].tail   = arr[l + i].tail;
        L[i].weight = arr[l + i].weight;
    }
    for (j = 0; j < n2; j++) {
        //R[j] = arr[m + 1+ j];
        R[j].tail   = arr[m + 1+ j].tail;
        R[j].weight = arr[m + 1+ j].weight;
    }
    
    /* Merge the temp arrays back into arr[l..r]*/
    i = 0; // Initial index of first subarray
    j = 0; // Initial index of second subarray
    k = l; // Initial index of merged subarray
    while (i < n1 && j < n2) {
        if (L[i].tail <= R[j].tail) {
            //arr[k] = L[i];
            arr[k].tail   = L[i].tail;
            arr[k].weight = L[i].weight;
            i++;
        } else {
            //arr[k] = R[j];
            arr[k].tail   = R[j].tail;
            arr[k].weight = R[j].weight;
            j++;
        }
        k++;
    }//End of while()
    
    /* Copy the remaining elements of L[], if there are any */
    while (i < n1) {
        //arr[k] = L[i];
        arr[k].tail   = L[i].tail;
        arr[k].weight = L[i].weight;
        i++;
        k++;
    }
    
    /* Copy the remaining elements of R[], if there are any */
    while (j < n2) {
        //arr[k] = R[j];
        arr[k].tail   = R[j].tail;
        arr[k].weight = R[j].weight;
        j++;
        k++;
    }
    free(L);
    free(R);
}//End of merge()

/* l is for left index and r is right index of the sub-array of arr to be sorted */
// Code from: http://www.geeksforgeeks.org/merge-sort/
void mergeSort(edge *arr, long l, long r) {
    if (l < r) {
        // Same as (l+r)/2, but avoids overflow for
        // large l and h
        long m = l+(r-l)/2;
        
        // Sort first and second halves
        mergeSort(arr, l, m);
        mergeSort(arr, m+1, r);
        merge(arr, l, m, r);
    }
} //End of mergeSort()

void SortNeighborListUsingInsertionAndMergeSort(graph *G) {
    double time1=0, time2=0;
    //Get the iterators for the graph:
    long NVer     = G->numVertices;
    long NEdge    = G->numEdges;       //Returns the correct number of edges (not twice)
    long *verPtr  = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
    edge *verInd  = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)
    
    time1 = omp_get_wtime();
    double* simWeights = (double*) malloc (2*NEdge*sizeof(double)); assert(simWeights != 0);
#pragma omp parallel for
    for (long v = 0; v < NVer; v++) {
        long adj1 = verPtr[v];
        long adj2 = verPtr[v+1];
        if((adj2 - adj1) < 1000 ) {
            //Use insertion sort: https://en.wikipedia.org/wiki/Insertion_sort
            for (long i = adj1+1; i < adj2; i++) {
                long x     = verInd[i].tail;
                double wtX = verInd[i].weight;
                long j = i - 1;
                while ((j >= adj1) && (verInd[j].tail > x)) {
                    verInd[j+1].tail   = verInd[j].tail;
                    verInd[j+1].weight = verInd[j].weight;
                    j--;
                }//end of while()
                verInd[j+1].tail   = x;
                verInd[j+1].weight = wtX;
            }
        } else {
            //Use merge sort:
            mergeSort(verInd, adj1, adj2);
        }//End of else
    }//End of for(v)
    
}//End of SortNeighborListUsingInsertionAndMergeSort()

//WARNING: Assume that the neighbor lists are sorted
double* computeEdgeSimilarityMetrics(graph *G) {
    double time1=0, time2=0;
    //Get the iterators for the graph:
    long NVer     = G->numVertices;
    long NEdge    = G->numEdges;       //Returns the correct number of edges (not twice)
    long *verPtr  = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
    edge *verInd  = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)
    
    time1 = omp_get_wtime();
    double* simWeights = (double*) malloc (2*NEdge*sizeof(double)); assert(simWeights != 0);
#pragma omp parallel for
    for (long v = 0; v < NVer; v++) {
        long adjV1 = verPtr[v];
        long adjV2 = verPtr[v+1];
        //Process all the neighbors of v:
        for(long i = adjV1; i < adjV2; i++ ) {
            long w = verInd[i].tail;
            if(w < v)
                continue; //This vertex has already been processed from the other end
            long setIntersect = 0, setUnion = 0;
            long adjW1 = verPtr[w];
            long adjW2 = verPtr[w+1];
            //Process both the sets (v and w) in an order:
            long c1 = adjV1;
            long c2 = adjW1;
            while((c1<adjV2) && (c2<adjW2)) {
                if(verInd[c1].tail == verInd[c2].tail) {
                    setIntersect++; setUnion++;
                    c1++; c2++;
                } else {
                    if(verInd[c1].tail < verInd[c2].tail) {
                        setUnion++;
                        c1++;
                    } else {
                        setUnion++;
                        c2++;
                    }
                }
            }//End of while(c1,c2)
            //Now compute the similarity score:
            double similarity = 0;
            if (setUnion > 0) //Avoid division by zero
                similarity = setIntersect / setUnion;
            simWeights[i] = similarity;
            //Find the position for edge (w --> v)
            for (long j=adjW1; j<adjW2; j++) {
                if (verInd[j].tail == v) {
                    simWeights[j] = similarity;
                    break;
                }
            }
            
        }//End of for(i)
    }//End of for(v)
    time2 = omp_get_wtime();
    
    return(simWeights);
}

//Build a sparsified graph and return it:
graph* buildSparifiedGraph(graph *Gin, double alpha) {
    assert (alpha <= 1.0);
    double time1=0, time2=0, totalTime=0;
    //Get the iterators for the graph:
    long NVer     = Gin->numVertices;
    long NEdge    = Gin->numEdges;       //Returns the correct number of edges (not twice)
    long *verPtr  = Gin->edgeListPtrs;   //Vertex Pointer: pointers to endV
    edge *verInd  = Gin->edgeList;       //Vertex Index: destination id of an edge (src -> dest)
    
    //Step 1: Sort the neighbors based on their indices:
    time1 = omp_get_wtime();
    SortNeighborListUsingInsertionAndMergeSort(Gin);
    time2 = omp_get_wtime();
    
    //Step 2: Compute Similarities:
    double * simWeights = computeEdgeSimilarityMetrics(Gin);
    
    //Step 3: Determine top edges for each vertex:
    bool* isEdgePresent = (bool*) malloc (2*NEdge*sizeof(bool)); assert(isEdgePresent != 0);
#pragma omp parallel for
    for (long v = 0; v < NVer; v++) {
        long adj1 = verPtr[v];
        long adj2 = verPtr[v+1];
        if(adj2 == adj1)
            continue; //isolated vertex; do nothing
        long numTopEdges = round(pow((adj2-adj1), alpha)); //Number of top edges
        if(numTopEdges <1)
            numTopEdges = 1; //Add at least one edge (otherwise vertices can become isolated)
        //Add the first neighbor by default
        double minWeight     = simWeights[adj1]; //Similarity value of the first neighbor
        long   minNeighbor   = adj1;           //Position of the first neighbor
        isEdgePresent[adj1]  = true;           //Mark this edge as true
        long edgesAddedSoFar = 1;            //Added one edge so far
        //Process all the neighbors of v:
        for(long i = adj1+1; i < adj2; i++ ) {
            //Always maintain the least weighted neighbor for each vertex;
            //... this neighbor will get bounced if there is no space
            if(edgesAddedSoFar < numTopEdges) {
                //Add the current edge to the list of top-k edges:
                isEdgePresent[i] = true;   //Mark this edge as true
                edgesAddedSoFar++;
                //Check if this is the current smallest edge:
                if(simWeights[i] < minWeight) {
                    minWeight = simWeights[i];
                    minNeighbor = i; //This is the position of the smallest edge added so far
                }
            } else {
                //Replace an existing edge and find the next minimum edge:
                if(simWeights[i] > minWeight) { //Found a heavier neighbor
                    isEdgePresent[i] = true;   //Mark this edge as true
                    isEdgePresent[minNeighbor] = false;   //Mark this edge as false
                    //Set the new edge as the minimum; need to fix this next:
                    minWeight = simWeights[i];
                    minNeighbor = i; //This is the position of the smallest edge added so far
                    //Now find the next minimum to replace:
                    for(long k = adj1; k < i; k++ ) { //Only look within unprocessed list
                        if((isEdgePresent[k]) && (simWeights[k] < minWeight)) {
                            minWeight = simWeights[k];
                            minNeighbor = k; //This is the position of the smallest edge added so far
                        }
                    }//End of for(k)
                }//End of if(simWeights[i] > minWeight)
            }//End of else
        }//End of for(i)
    }//End of for(v)
    
    //Step 4: Build the new graph:
    //From each edge count the number of active edges and add them to the data structure
    //Tricky because of the double storage requirements.
    
    
}

