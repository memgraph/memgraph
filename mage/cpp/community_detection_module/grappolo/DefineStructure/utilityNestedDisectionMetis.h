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

#ifndef _graph_NestDissect_
#define _graph_NestDissect_

/*
 int METIS NodeND(idx t *nvtxs, idx t *xadj, idx t *adjncy, idx t *vwgt, idx t *options,
 idx t *perm, idx t *iperm)
 
 Description
 This function computes fill reducing orderings of sparse matrices using the multilevel nested dissection algorithm.
 
 Parameters
 nvtxs: The number of vertices in the graph.
 xadj, adjncy:  The adjacency structure of the graph as described in Section 5.5.
 vwgt (NULL):  An array of size nvtxs specifying the weights of the vertices. If the graph is weighted, the nested dissection ordering computes vertex separators that minimize the sum of the weights of the vertices
 on the separators.  A NULL can be passed to indicate a graph with equal weight vertices (or unweighted).
 
 options (NULL)
 This is the array of options as described in Section 5.4. The following options are valid:
 
 METIS_OPTION_CTYPE, METIS_OPTION_RTYPE, METIS_OPTION_NO2HOP,
 METIS_OPTION_NSEPS, METIS_OPTION_NITER, METIS_OPTION_UFACTOR,
 METIS_OPTION_COMPRESS, METIS_OPTION_CCORDER, METIS_OPTION_SEED,
 METIS_OPTION_PFACTOR, METIS_OPTION_NUMBERING, METIS_OPTION_DBGLVL
 
 perm, iperm: These are vectors, each of size nvtxs. Upon successful completion, they store the fill-reducing permutation and inverse-permutation. Let A be the original matrix and A0 be the permuted matrix. The
 arrays perm and iperm are defined as follows.
 Row (column) i of A0 is the perm[i] row (column) of A, and row (column) i of A is the iperm[i] row (column) of A0. The numbering of this vector starts from either 0 or 1, depending on the value of options[METIS OPTION NUMBERING].
 
 Returns:
 METIS OK Indicates that the function returned normally.
 METIS ERROR INPUT Indicates an input error.
 METIS ERROR MEMORY Indicates that it could not allocate the required memory.
 METIS ERROR Indicates some other type of error.
 */

extern "C" {
#include "metis.h"
}

using namespace std;

/*
 #ifdef __cplusplus
 extern "C" {
 #endif
 //Nested dissection
 int METIS_NodeND(idx t *nvtxs, idx t *xadj, idx t *adjncy, idx t *vwgt, idx t *options,
 idx t *perm, idx t *iperm);
 
 #ifdef __cplusplus
 }
 #endif
 */
//METIS Graph Partitioner:
void MetisNDReorder( graph *G, long *old2NewMap ) {
    
    
    //Get the iterators for the graph:
    long   NV        = G->numVertices;
    long   NE        = G->numEdges;
    long   *vtxPtr   = G->edgeListPtrs;
    edge   *vtxInd   = G->edgeList;
    int status=0;
    
    idx_t nvtxs = (idx_t) NV;
    idx_t *xadj = (idx_t *) malloc ((NV+1) * sizeof(idx_t));
    assert(xadj != 0);
#pragma omp parallel for
    for(long i=0; i<=NV; i++) {
        xadj[i] = (idx_t) vtxPtr[i];
    }
    
    idx_t *adjncy = (idx_t *) malloc (2*NE * sizeof(idx_t));
    assert(adjncy != 0);
#pragma omp parallel for
    for(long i=0; i<2*NE; i++) {
        adjncy[i] = (idx_t) vtxInd[i].tail;
    }
    
    idx_t *adjwgt = (idx_t *) malloc (2*NE * sizeof(idx_t));
    assert(adjwgt != 0);
#pragma omp parallel for
    for(long i=0; i<2*NE; i++) {
        adjwgt[i] = (idx_t) vtxInd[i].weight;
    }
    
    idx_t *perm  = (idx_t *) malloc (NV * sizeof(idx_t)); assert(perm != 0);
    idx_t *iperm = (idx_t *) malloc (NV * sizeof(idx_t)); assert(iperm != 0);
    
    
    real_t ubvec = 1.03;
    
    idx_t options[METIS_NOPTIONS];
    
    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_CTYPE]    = METIS_CTYPE_SHEM;  //Sorted heavy-edge matching
    options[METIS_OPTION_IPTYPE]   = METIS_IPTYPE_NODE; //Grows a bisection using a greedy strategy.
    options[METIS_OPTION_RTYPE]    = METIS_RTYPE_SEP1SIDED; //FM-based cut refinement.
    options[METIS_OPTION_DBGLVL]   = 1; //#different separators at each level of nested dissection.
    options[METIS_OPTION_UFACTOR]  = 200; //Maximum allowed load imbalance among partitions
    options[METIS_OPTION_NO2HOP]   = 0; //The 2–hop matching (0=perform; 1=Do not)
    options[METIS_OPTION_COMPRESS] = 1; //Combine vertices with identical adjacency lists (0=do not)
    options[METIS_OPTION_CCORDER]  = 0; //Connected components identified and ordered separately (1=Yes)
    options[METIS_OPTION_SEED]     = 786; //Specifies the seed for the random number generator.
    options[METIS_OPTION_NITER]    = 10; //#iterations for the refinement algorithms
    options[METIS_OPTION_NSEPS]    = 1; //#different separators
    options[METIS_OPTION_PFACTOR]  = 10; //Min degree of the vertices that will be ordered last
    options[METIS_OPTION_NUMBERING]= 0; //C-style numbering, starting from 0
    
    /* int returnVal = METIS_PartGraphKway(&nvtxs, &ncon, xadj, adjncy, NULL, NULL, adjwgt,
     &nparts, NULL, NULL, options, &objval, part); */
    
    status = METIS_NodeND(&nvtxs, xadj, adjncy, NULL, options, perm, iperm);
    
    if(status == METIS_OK)
    else {
        if(status == METIS_ERROR_MEMORY)
        else if(status == METIS_ERROR_INPUT)
        else
    }
    
#pragma omp parallel for
    for(long i=0; i<=NV; i++) {
        old2NewMap[i] = (long) perm[i]; //Do explicit typecasts
    }
    
    //Cleanup:
    free(xadj); free(adjncy); free(adjwgt);
    free(perm); free(iperm);
}

#endif
