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
// Code adapted from Howard (Hao) Lu
// ************************************************************************

#include <mg_procedure.h>
#include "defs.h"
#include "coloring.h"
//Compute the size of each color class
//Return: pointer to a vector that stores the size of each color class
void buildColorSize(long NVer, int *vtxColor, int numColors, long *colorSize) {
  assert(colorSize != 0);
  //Count the frequency of each color
#pragma omp parallel for
  for(long i =0; i<NVer; i++) {
    __sync_fetch_and_add(&colorSize[vtxColor[i]], 1);
  }
}//end of buildColorSize()

/**********************************************************************************/
//double calVariance(GraphElem nv, ColorElem ncolors, ColorVector freq, std::string output)
void computeVariance(long NVer, int numColors, long *colorSize) {
  double avg = (double)NVer/(double)numColors;
  double variance = 0;
  long max = 0;    //Initialize to zero
  long min = NVer; //Initialize to some large number

//#pragma omp parallel for reduction(+:variance), reduction(max:max), reduction(min:min)
  for(long ci=0; ci<numColors; ci++) {
    variance  += (avg - (double)colorSize[ci])*(avg - (double)colorSize[ci]);
    if(colorSize[ci] > max)
      max = colorSize[ci];
    if(colorSize[ci] < min)
      min = colorSize[ci];
  }
  variance = variance / (double)numColors;

}//End of calVariance()

//Perform recoloring based on the CFF & CLU schemes
//type: Specifies the type for First-Fit (1 -- default) or Least-Used (2)
void equitableDistanceOneColorBased(graph *G, mgp_graph *mg_graph, int *vtxColor, int numColors, long *colorSize,
				    int nThreads, double *totTime, int type) {

  /*
  if (nThreads < 1)
    omp_set_num_threads(1); //default to one thread
  else
    omp_set_num_threads(nThreads);
  */
  int nT;
#pragma omp parallel
  {
    nT = omp_get_num_threads();
  }

  double time1=0, time2=0, totalTime=0;
  //Get the iterators for the graph:
  long NVer    = G->numVertices;
  long NEdge   = G->numEdges;
  long *verPtr = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
  edge *verInd = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)
#ifdef PRINT_DETAILED_STATS_
#endif

  //STEP-1: Create a CSR-like data structure for vertex-colors
  time1 = omp_get_wtime();

  long *colorIndex = (long *) malloc (NVer * sizeof(long)); assert(colorIndex != 0);
  long *colorAdded = (long *) malloc (numColors * sizeof(long)); assert(colorAdded != 0);

#pragma omp parallel for
  for(long i = 0; i < numColors; i++) {
    colorAdded[i] = 0;
  }


 long *colorPtr = (long *) malloc ((numColors+1) * sizeof(long)); assert(colorPtr != 0);
#pragma omp parallel for
  for(long i = 0; i <= numColors; i++) {
    colorPtr[i] = 0;
  }



  // Count the size of each color
//#pragma omp parallel for
  for(long i = 0; i < NVer; i++) {
    __sync_fetch_and_add(&colorPtr[(long)vtxColor[i]+1],1);
  }
  //Prefix sum:
  for(long i=0; i<numColors; i++) {
    colorPtr[i+1] += colorPtr[i];
  }

  //Group vertices with the same color in particular order
//#pragma omp parallel for
  for (long i=0; i<NVer; i++) {
    long tc = (long)vtxColor[i];
    long Where = colorPtr[tc] + __sync_fetch_and_add(&(colorAdded[tc]), 1);
    colorIndex[Where] = i;
  }
  time2 = omp_get_wtime();
  totalTime += time2 - time1;

  //long avgColorSize = (long)ceil( (double)NVer/(double)numColor );
  long avgColorSize = (NVer + numColors - 1) / numColors;


  //STEP-2: Start moving the vertices from one color bin to another
  time1 = omp_get_wtime();
  for (long ci=0; ci<numColors; ci++) {
    if(colorSize[ci] <= avgColorSize)
      continue;  //Dont worry if the size is less than the average
    //Now move the vertices to bring the size to average
    long adjC1 = colorPtr[ci];
    long adjC2 = colorPtr[ci+1];
#pragma omp parallel
{
    [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(mg_graph);
#pragma omp for
    for (long vi=adjC1; vi<adjC2; vi++) {
      if(colorSize[ci] <= avgColorSize)
	continue; //break the loop when enough vertices have been moved
      //Now recolor the vertex:
      long v = colorIndex[vi];

      long adj1 = verPtr[v];
      long adj2 = verPtr[v+1];
      long myDegree = verPtr[v+1] - verPtr[v];
      bool *Mark = (bool *) malloc ( numColors * sizeof(bool) );
      assert(Mark != 0);
      for (int i=0; i<numColors; i++) {
	if ( colorSize[i] >= avgColorSize )
	  Mark[i] = true; //Cannot use this color
	else
	  Mark[i] = false; //Else, It is okay to use
      }
      //Browse the adjacency set of vertex v
      for(long k = adj1; k < adj2; k++ ) {
        if ( v == verInd[k].tail ) //Self-loops
	  continue;
	int adjColor = vtxColor[verInd[k].tail];
	assert(adjColor >= 0);
	assert(adjColor < numColors); //Fail-safe check
	Mark[adjColor] = true;
      } //End of for loop(k)
      int myColor;
      //Start from the other end:
      for (myColor=0; myColor<numColors; myColor++) {
	if ( Mark[myColor] == false )
	  break;
      }
      if ((myColor >= 0) && (myColor < numColors) ) { //Found a valid choice
	vtxColor[v] = myColor; //Re-color the vertex
	__sync_fetch_and_add(&colorSize[myColor], 1); //Increment the size of the new color
	__sync_fetch_and_sub(&colorSize[ci], 1); //Decrement the size of the old color
      }
      free(Mark);
    } //End of outer for loop(vi)
  }//End of for(ci)
  [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(mg_graph);
}
  time2  = omp_get_wtime();
  totalTime += time2 - time1;
#ifdef PRINT_DETAILED_STATS_
#endif

  *totTime = totalTime;


  ///////////////////////////////////////////////////////////////////////////
  /////////////////// VERIFY THE COLORS /////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////
  //Verify Results and Cleanup
  int myConflicts = 0;
#pragma omp parallel for
  for (long v=0; v < NVer; v++ ) {
    long adj1 = verPtr[v];
    long adj2 = verPtr[v+1];
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

}//End of colorBasedEquitable()

