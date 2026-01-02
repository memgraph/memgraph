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

//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////  DISTANCE ONE COLORING      ///////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//Return the number of colors used (zero is a valid color)
int algoDistanceOneVertexColoringOpt(graph *G, int *vtxColor, int nThreads, double *totTime)
{
#ifdef PRINT_DETAILED_STATS_
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

  double time1=0, time2=0, totalTime=0;
  //Get the iterators for the graph:
  long NVer    = G->numVertices;
  long NEdge   = G->numEdges;
  long *verPtr = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
  edge *verInd = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)

#ifdef PRINT_DETAILED_STATS_
#endif

  //Build a vector of random numbers
  double *randValues = (double*) malloc (NVer * sizeof(double));
  assert(randValues != 0);
  generateRandomNumbers(randValues, NVer);

  long *Q    = (long *) malloc (NVer * sizeof(long)); assert(Q != 0);
  long *Qtmp = (long *) malloc (NVer * sizeof(long)); assert(Qtmp != 0);
  long *Qswap;
  if( (Q == NULL) || (Qtmp == NULL) ) {
    exit(1);
  }
  long QTail=0;    //Tail of the queue
  long QtmpTail=0; //Tail of the queue (implicitly will represent the size)
  long realMaxDegree = 0;

	#pragma omp parallel for
  for (long i=0; i<NVer; i++) {
      Q[i]= i;     //Natural order
      Qtmp[i]= -1; //Empty queue
  }
  QTail = NVer;	//Queue all vertices


	// Cal real Maximum degree, 2x for maxDegree to be safe
	#pragma omp parallel for reduction(max: realMaxDegree)
	for (long i = 0; i < NVer; i++) {
		long adj1, adj2, de;
		adj1 = verPtr[i];
		adj2 = verPtr[i+1];
		de = adj2-adj1;
		if ( de > realMaxDegree)
			realMaxDegree = de;
	}
	//realMaxDegree *= 1.5;

	ColorVector freq(MaxDegree,0);
  /////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////// START THE WHILE LOOP ///////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////
  long nConflicts = 0; //Number of conflicts
  int nLoops = 0;     //Number of rounds of conflict resolution

#ifdef PRINT_DETAILED_STATS_
#endif
  do{
    ///////////////////////////////////////// PART 1 ////////////////////////////////////////
    //Color the vertices in parallel - do not worry about conflicts
#ifdef PRINT_DETAILED_STATS_
#endif

    time1 = omp_get_wtime();
		#pragma omp parallel for
    for (long Qi=0; Qi<QTail; Qi++) {
      long v = Q[Qi]; //Q.pop_front();
			int maxColor = 0;
			BitVector mark(MaxDegree, false);
			maxColor = distanceOneMarkArray(mark,G,v,vtxColor);

			int myColor;
			for (myColor=0; myColor<=maxColor; myColor++) {
				if ( mark[myColor] == false )
					break;
			}
			vtxColor[v] = myColor; //Color the vertex
		} //End of outer for loop: for each vertex

		time1  = omp_get_wtime() - time1;
		totalTime += time1;

#ifdef PRINT_DETAILED_STATS_
#endif
    ///////////////////////////////////////// PART 2 ////////////////////////////////////////
    //Detect Conflicts:
    //Add the conflicting vertices into a Q:
    //Conflicts are resolved by changing the color of only one of the
    //two conflicting vertices, based on their random values
    time2 = omp_get_wtime();

#pragma omp parallel for
		for (long Qi=0; Qi<QTail; Qi++) {
			long v = Q[Qi]; //Q.pop_front();
			distanceOneConfResolution(G, v, vtxColor, randValues, &QtmpTail, Qtmp, freq, 0);
		} //End of outer for loop: for each vertex

		time2  = omp_get_wtime() - time2;
		totalTime += time2;
		nConflicts += QtmpTail;
		nLoops++;

#ifdef PRINT_DETAILED_STATS_
#endif

    //Swap the two queues:
    Qswap = Q;
    Q = Qtmp; //Q now points to the second vector
    Qtmp = Qswap;
    QTail = QtmpTail; //Number of elements
    QtmpTail = 0; //Symbolic emptying of the second queue
  } while (QTail > 0);
  //Check the number of colors used
  int nColors = -1;
  for (long v=0; v < NVer; v++ )
    if (vtxColor[v] > nColors) nColors = vtxColor[v];
#ifdef PRINT_DETAILED_STATS_
#endif
  *totTime = totalTime;
  //////////////////////////// /////////////////////////////////////////////////////////////
  ///////////////////////////////// VERIFY THE COLORS /////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////

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

  //Clean Up:
  free(Q);
  free(Qtmp);
  free(randValues);

  return nColors; //Return the number of colors used
}


//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////  DISTANCE ONE COLORING      ///////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//Return the number of colors used (zero is a valid color)
int algoDistanceOneVertexColoring(graph *G, int *vtxColor, int nThreads, double *totTime)
{
	if (nThreads < 1)
		omp_set_num_threads(1); //default to one thread
	else
		omp_set_num_threads(nThreads);
	int nT;
#pragma omp parallel
	{
		nT = omp_get_num_threads();
	}


	double time1=0, time2=0, totalTime=0;
  //Get the iterators for the graph:
  long NVer    = G->numVertices;
  long NS      = G->sVertices;
  long NT      = NVer - NS;
  long NEdge           = G->numEdges;
  long *verPtr         = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
  edge *verInd         = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)

  //const int MaxDegree = 4096; //Increase if number of colors is larger

  //Build a vector of random numbers
  double *randValues = (double*) malloc (NVer * sizeof(double));
  if( randValues == NULL ) {
    exit(1);
  }
  generateRandomNumbers(randValues, NVer);

  //The Queue Data Structure for the storing the vertices
  //   the need to be colored/recolored
  //Have two queues - read from one, write into another
  //   at the end, swap the two.
  long *Q    = (long *) malloc (NVer * sizeof(long));
  long *Qtmp = (long *) malloc (NVer * sizeof(long));
  long *Qswap;
  if( (Q == NULL) || (Qtmp == NULL) ) {
    exit(1);
  }
  long QTail=0;    //Tail of the queue
  long QtmpTail=0; //Tail of the queue (implicitly will represent the size)

#pragma omp parallel for
  for (long i=0; i<NVer; i++) {
      Q[i]= i;     //Natural order
      Qtmp[i]= -1; //Empty queue
  }
  QTail = NVer;	//Queue all vertices
  /////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////// START THE WHILE LOOP ///////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////
  long nConflicts = 0; //Number of conflicts
  int nLoops = 0;     //Number of rounds of conflict resolution
  int *Mark = (int *) malloc ( MaxDegree * NVer * sizeof(int) );
  if( Mark == NULL ) {
    exit(1);
  }
#pragma omp parallel for
  for (long i=0; i<MaxDegree*NVer; i++)
     Mark[i]= -1;

  do {
    ///////////////////////////////////////// PART 1 ////////////////////////////////////////
    //Color the vertices in parallel - do not worry about conflicts
    time1 = omp_get_wtime();
#pragma omp parallel for
    for (long Qi=0; Qi<QTail; Qi++) {
      long v = Q[Qi]; //Q.pop_front();
      long StartIndex = v*MaxDegree; //Location in Mark
      if (nLoops > 0) //Skip the first time around
	for (long i=StartIndex; i<(StartIndex+MaxDegree); i++)
	  Mark[i]= -1;
      long adj1 = verPtr[v];
      long adj2 = verPtr[v+1];
      int maxColor = -1;
      int adjColor = -1;
      //Browse the adjacency set of vertex v
      for(long k = adj1; k < adj2; k++ ) {
	//if ( v == verInd[k] ) //Skip self-loops
	//continue;
	adjColor =  vtxColor[verInd[k].tail];
	if ( adjColor >= 0 ) {
	  Mark[StartIndex+adjColor] = v;
	  //Find the largest color in the neighborhood
	  if ( adjColor > maxColor )
	    maxColor = adjColor;
	}
      } //End of for loop to traverse adjacency of v
      int myColor;
      for (myColor=0; myColor<=maxColor; myColor++) {
	if ( Mark[StartIndex+myColor] != v )
	  break;
      }
      if (myColor == maxColor)
	myColor++; /* no available color with # less than cmax */
      vtxColor[v] = myColor; //Color the vertex
    } //End of outer for loop: for each vertex

    time1  = omp_get_wtime() - time1;
    totalTime += time1;

    ///////////////////////////////////////// PART 2 ////////////////////////////////////////
    //Detect Conflicts:
    //Add the conflicting vertices into a Q:
    //Conflicts are resolved by changing the color of only one of the
    //two conflicting vertices, based on their random values
    time2 = omp_get_wtime();
#pragma omp parallel for
    for (long Qi=0; Qi<QTail; Qi++) {
      long v = Q[Qi]; //Q.pop_front();
      long adj1 = verPtr[v];
      long adj2 = verPtr[v+1];
      //Browse the adjacency set of vertex v
      for(long k = adj1; k < adj2; k++ ) {
	//if ( v == verInd[k] ) //Self-loops
	//continue;
	if ( vtxColor[v] == vtxColor[verInd[k].tail] ) {
	  //Q.push_back(v or w)
	  if ( (randValues[v] < randValues[verInd[k].tail]) ||
	       ((randValues[v] == randValues[verInd[k].tail])&&(v < verInd[k].tail)) ) {
	    long whereInQ = __sync_fetch_and_add(&QtmpTail, 1);
	    Qtmp[whereInQ] = v;//Add to the queue
	    vtxColor[v] = -1;  //Will prevent v from being in conflict in another pairing
	    break;
	  }
	} //End of if( vtxColor[v] == vtxColor[verInd[k]] )
      } //End of inner for loop: w in adj(v)
    } //End of outer for loop: for each vertex

    time2  = omp_get_wtime() - time2;
    totalTime += time2;
    nConflicts += QtmpTail;
    nLoops++;
    //Swap the two queues:
    Qswap = Q;
    Q = Qtmp; //Q now points to the second vector
    Qtmp = Qswap;
    QTail = QtmpTail; //Number of elements
    QtmpTail = 0; //Symbolic emptying of the second queue
  } while (QTail > 0);
  //Check the number of colors used
  int nColors = -1;
  for (long v=0; v < NVer; v++ )
    if (vtxColor[v] > nColors) nColors = vtxColor[v];

  *totTime = totalTime;
  /////////////////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////// VERIFY THE COLORS /////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////
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
	//#pragma omp atomic
	__sync_fetch_and_add(&myConflicts, 1); //increment the counter
      }
    }//End of inner for loop: w in adj(v)
  }//End of outer for loop: for each vertex

  myConflicts = myConflicts / 2; //Have counted each conflict twice
  //Clean Up:
  free(Q);
  free(Qtmp);
  free(Mark);
  free(randValues);

  return nColors; //Return the number of colors used
}
