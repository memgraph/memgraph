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

#ifndef _graph_partitioner_
#define _graph_partitioner_

/*
int METIS PartGraphKway(idx_t *nvtxs, idx_t *ncon, idx_t *xadj, idx_t *adjncy,
idx_t *vwgt, idx_t *vsize, idx_t *adjwgt, idx_t *nparts, real_t *tpwgts,
real_t ubvec, idx_t *options, idx_t *objval, idx_t *part)

nvtxs: The number of vertices in the graph.

ncon: The number of balancing constraints. It should be at least 1.

xadj, adjncy: The adjacency structure of the graph as described in Section 5.5.

vwgt (NULL): The weights of the vertices as described in Section 5.5.

vsize (NULL): The size of the vertices for computing the total communication volume as described in Section 5.7.

adjwgt (NULL): The weights of the edges as described in Section 5.5.

nparts The number of parts to partition the graph.

tpwgts (NULL): This is an array of size npartsncon that speciﬁes the desired weight for each partition and constraint.
The target partition weight for the ith partition and jth constraint is speciﬁed at tpwgts[i*ncon+j]
(the numbering for both partitions and constraints starts from 0). For each constraint, the sum of the
tpwgts[] entries must be 1.0 (i.e., \Sum_i tpwgts[i*ncon + j] = 1:0).
A NULL value can be passed to indicate that the graph should be equally divided among the partitions.

ubvec (NULL): This is an array of size ncon that speciﬁes the allowed load imbalance tolerance for each constraint.
For the ith partition and jth constraint the allowed weight is the ubvec[j]*tpwgts[i*ncon+j] fraction
of the jth’s constraint total weight. The load imbalances must be greater than 1.0.
A NULL value can be passed indicating that the load imbalance tolerance for each constraint should
be 1.001 (for ncon=1) or 1.01 (for ncon<1).

options (NULL):
This is the array of options as described in Section 5.4.
The following options are valid for METIS PartGraphRecursive:
METIS_OPTION_CTYPE, METIS_OPTION_IPTYPE, METIS_OPTION_RTYPE,
METIS_OPTION_NO2HOP, METIS_OPTION_NCUTS, METIS_OPTION_NITER,
METIS_OPTION_SEED, METIS_OPTION_UFACTOR, METIS_OPTION_NUMBERING,
METIS_OPTION_DBGLVL

The following options are valid for METIS PartGraphKway:
METIS_OPTION_OBJTYPE, METIS_OPTION_CTYPE, METIS_OPTION_IPTYPE,
METIS_OPTION_RTYPE, METIS_OPTION_NO2HOP, METIS_OPTION_NCUTS,
METIS_OPTION_NITER, METIS_OPTION_UFACTOR, METIS_OPTION_MINCONN,
METIS_OPTION_CONTIG, METIS_OPTION_SEED, METIS_OPTION_NUMBERING,
METIS_OPTION_DBGLVL

objval: Upon successful completion, this variable stores the edge-cut or the total communication volume of
the partitioning solution. The value returned depends on the partitioning’s objective function.

part: This is a vector of size nvtxs that upon successful completion stores the partition vector of the graph.
The numbering of this vector starts from either 0 or 1, depending on the value of
options[METIS OPTION NUMBERING].

Returns
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
  //Multilevel k-way Partitioning
  int METIS_PartGraphKway(idx_t *nvtxs, idx_t *ncon, idx_t *xadj, idx_t *adjncy,
     idx_t *vwgt, idx_t *vsize, idx_t *adjwgt, idx_t *nparts, real_t *tpwgts,
     real_t ubvec, idx_t *options, idx_t *objval, idx_t *part);

#ifdef __cplusplus
}
#endif
*/
//METIS Graph Partitioner:
void MetisGraphPartitioner( graph *G, long *VertexPartitioning, int numParts ) {

  
  //Get the iterators for the graph:
  long   NV        = G->numVertices;  
  long   NE        = G->numEdges;
  long   *vtxPtr   = G->edgeListPtrs;
  edge   *vtxInd   = G->edgeList;  

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
 
  idx_t nparts = (idx_t) numParts;
  real_t ubvec = 1.03; 
 
  idx_t options[METIS_NOPTIONS];
  METIS_SetDefaultOptions(options);

  options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_CUT; //Edgecut minimization
  options[METIS_OPTION_CTYPE] = METIS_CTYPE_SHEM; //Sorted heavy-edge matching
  options[METIS_OPTION_NUMBERING]= 0; //C-style numbering, starting from 0
  //options[METIS_OPTION_NO2HOP]= 0; //Performs a 2-hop matching -- effective for power-law graphs 
  options[METIS_OPTION_NSEPS]= 10; //Number of iterations for refinement
  //options[METIS_OPTION_UFACTOR] = 30;
  
  idx_t ncon = 1; //Number of balancing constraints (at least 1)
  idx_t objval = 0; //Will contain the edgecut (or total communication)

  idx_t *part = (idx_t *) malloc (NV * sizeof(idx_t)); //Partition information
  assert(part != 0);

  int returnVal = METIS_PartGraphKway(&nvtxs, &ncon, xadj, adjncy, NULL, NULL, adjwgt, 
		                      &nparts, NULL, NULL, options, &objval, part);

  if(returnVal == METIS_OK)
  else {
     if(returnVal == METIS_ERROR_MEMORY)
     else 
  }

#pragma omp parallel for
  for(long i=0; i<=NV; i++) {
     VertexPartitioning[i] = (long) part[i]; //Do explicit typecasts
  }
  
  //Cleanup:
  free(xadj); free(adjncy); free(adjwgt);
  free(part);
}

#endif
