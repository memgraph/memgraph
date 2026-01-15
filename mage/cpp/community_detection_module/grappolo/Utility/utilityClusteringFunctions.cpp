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

#include "utilityClusteringFunctions.h"

using namespace std;

void updateAxForOpt(Comm* cInfo, long* currCommAss, double* vDegree, long NV)
{
  #pragma omp parallel for
  for(long i = 0; i < NV; i++)
  {
    if(currCommAss[i] != i){
/*    __sync_fetch_and_sub(&(cInfo[i].degree),vDegree[i]);
      __sync_fetch_and_sub(&(cInfo[i].size),1);
      __sync_fetch_and_add(&(cInfo[currCommAss[i]].degree),vDegree[i]);
      __sync_fetch_and_add(&(cInfo[currCommAss[i]].size),1);
*/
      #pragma omp atomic update
      cInfo[i].degree -= vDegree[i];
      #pragma omp atomic update
      cInfo[i].size -= 1;
      #pragma omp atomic update
      cInfo[currCommAss[i]].degree += vDegree[i];
      #pragma omp atomic update
      cInfo[currCommAss[i]].size += 1;
   }
  }
}

void sumVertexDegree(edge* vtxInd, long* vtxPtr, double* vDegree, long NV, Comm* cInfo) {
#ifdef USE_OMP_DYNAMIC
#pragma omp parallel for schedule(dynamic)
#else
#pragma omp parallel for
#endif
  for (long i=0; i<NV; i++) {
    long adj1 = vtxPtr[i];	    //Begin
    long adj2 = vtxPtr[i+1];	//End
    double totalWt = 0;
    for(long j=adj1; j<adj2; j++) {
      totalWt += vtxInd[j].weight;
    }
    vDegree[i] = totalWt;	//Degree of each node
    cInfo[i].degree = totalWt;	//Initialize the community
    cInfo[i].size = 1;
  }
}//End of sumVertexDegree()

double calConstantForSecondTerm(double* vDegree, long NV) {
  double totalEdgeWeightTwice = 0;
  #pragma omp parallel for reduction(+:totalEdgeWeightTwice)
  for (long i=0; i<NV; i++) {
      totalEdgeWeightTwice += vDegree[i];
  }
  return (double)1/totalEdgeWeightTwice;
}//End of calConstantForSecondTerm()

void initCommAss(long* pastCommAss, long* currCommAss, long NV) {
#pragma omp parallel for
  for (long i=0; i<NV; i++) {
    pastCommAss[i] = i; //Initialize each vertex to its cluster
    currCommAss[i] = i;
  }
}//End of initCommAss()

//Smart initialization assuming that each vertex is assigned to its own cluster
//WARNING: Will ignore duplicate edge entries (multi-graph)
void initCommAssOpt(long* pastCommAss, long* currCommAss, long NV,
		    mapElement* clusterLocalMap, long* vtxPtr, edge* vtxInd,
		    Comm* cInfo, double constant, double* vDegree) {

#pragma omp parallel for
  for (long v=0; v<NV; v++) {
    long adj1  = vtxPtr[v];
    long adj2  = vtxPtr[v+1];
    long sPosition = vtxPtr[v]+v; //Starting position of local map for v

    pastCommAss[v] = v; //Initialize each vertex to its own cluster
    //currCommAss[v] = v; //Initialize with a self cluster

    //Step-1: Build local map counter (without a map):
    long numUniqueClusters = 0;
    double selfLoop = 0;
    clusterLocalMap[sPosition].cid     = v; //Add itself
    clusterLocalMap[sPosition].Counter = 0; //Initialize the count
    numUniqueClusters++;
    //Parse through the neighbors
    for(long j=adj1; j<adj2; j++) {
      if(vtxInd[j].tail == v) {	// SelfLoop need to be recorded
	      selfLoop += (long)vtxInd[j].weight;
        clusterLocalMap[sPosition].Counter = vtxInd[j].weight; //Initialize the count
        continue;
      }
      //Assume each neighbor is assigned to a separate cluster
      //Assume no duplicates (only way to improve performance at this step)
      clusterLocalMap[sPosition + numUniqueClusters].cid     = vtxInd[j].tail; //Add the cluster id (initialized to itself)
      clusterLocalMap[sPosition + numUniqueClusters].Counter = vtxInd[j].weight; //Initialize the count
      numUniqueClusters++;
    }//End of for(j)

    //Step 2: Find max:
    long maxIndex = v;	//Assign the initial value as the current community
    double curGain = 0;
    double maxGain = 0;
    double eix = clusterLocalMap[sPosition].Counter - selfLoop; //NOT SURE ABOUT THIS.
    double ax  = cInfo[v].degree - vDegree[v];
    double eiy = 0;
    double ay  = 0;
    for(long k=0; k<numUniqueClusters; k++) {
      if(v != clusterLocalMap[sPosition + k].cid) {
        ay = cInfo[clusterLocalMap[sPosition + k].cid].degree; // degree of cluster y
        eiy = clusterLocalMap[sPosition + k].Counter; 	//Total edges incident on cluster y
        curGain = 2*(eiy - eix) - 2*vDegree[v]*(ay - ax)*constant;

        if( (curGain > maxGain) || ((curGain==maxGain) && (curGain != 0) && (clusterLocalMap[sPosition + k].cid < maxIndex)) ) {
          maxGain  = curGain;
          maxIndex = clusterLocalMap[sPosition + k].cid;
        }
      }
    }//End of for()

    if(cInfo[maxIndex].size == 1 && cInfo[v].size == 1 && maxIndex > v) { //Swap protection
      maxIndex = v;
    }
    currCommAss[v] = maxIndex; //Assign the new community
  }

  updateAxForOpt(cInfo,currCommAss,vDegree,NV);
}//End of initCommAssOpt()


double buildLocalMapCounter(long adj1, long adj2, map<long, long> &clusterLocalMap,
			 vector<double> &Counter, edge* vtxInd, long* currCommAss, long me) {

  map<long, long>::iterator storedAlready;
  long numUniqueClusters = 1;
  double selfLoop = 0;
  for(long j=adj1; j<adj2; j++) {
    if(vtxInd[j].tail == me) {	// SelfLoop need to be recorded
      selfLoop += vtxInd[j].weight;
    }

    storedAlready = clusterLocalMap.find(currCommAss[vtxInd[j].tail]); //Check if it already exists
    if( storedAlready != clusterLocalMap.end() ) {	//Already exists
      Counter[storedAlready->second]+= vtxInd[j].weight; //Increment the counter with weight
    } else {
      clusterLocalMap[currCommAss[vtxInd[j].tail]] = numUniqueClusters; //Does not exist, add to the map
      Counter.push_back(vtxInd[j].weight); //Initialize the count
      numUniqueClusters++;
    }
  }//End of for(j)

  return selfLoop;
}//End of buildLocalMapCounter()

//Build the local-map data structure using vectors
double buildLocalMapCounterNoMap(long v, mapElement* clusterLocalMap, long* vtxPtr, edge* vtxInd,
                               long* currCommAss, long &numUniqueClusters) {
    long adj1  = vtxPtr[v];
    long adj2  = vtxPtr[v+1];
    long sPosition = vtxPtr[v]+v; //Starting position of local map for v

    long storedAlready = 0;
    double selfLoop = 0;
    for(long j=adj1; j<adj2; j++) {
        if(vtxInd[j].tail == v) {	// SelfLoop need to be recorded
            selfLoop += vtxInd[j].weight;
        }
        bool storedAlready = false; //Initialize to zero
        for(long k=0; k<numUniqueClusters; k++) { //Check if it already exists
            if(currCommAss[vtxInd[j].tail] ==  clusterLocalMap[sPosition+k].cid) {
                storedAlready = true;
                clusterLocalMap[sPosition + k].Counter += vtxInd[j].weight; //Increment the counter with weight
                break;
            }
        }
        if( storedAlready == false ) {	//Does not exist, add to the map
            clusterLocalMap[sPosition + numUniqueClusters].cid     = currCommAss[vtxInd[j].tail];
            clusterLocalMap[sPosition + numUniqueClusters].Counter = vtxInd[j].weight; //Initialize the count
            numUniqueClusters++;
        }
    }//End of for(j)
    return selfLoop;
}//End of buildLocalMapCounter()

long max(map<long, long> &clusterLocalMap, vector<double> &Counter,
         double selfLoop, Comm* cInfo, double degree, long sc, double constant ) {

    map<long, long>::iterator storedAlready;
    long maxIndex = sc;	//Assign the initial value as self community
    double curGain = 0;
    double maxGain = 0;
    double eix = Counter[0] - selfLoop;
    double ax = cInfo[sc].degree - degree;
    double eiy = 0;
    double ay = 0;

    storedAlready = clusterLocalMap.begin();
    do {
        if(sc != storedAlready->first) {
            ay = cInfo[storedAlready->first].degree; // degree of cluster y
            eiy = Counter[storedAlready->second]; 	//Total edges incident on cluster y
            curGain = 2*(eiy - eix) - 2*degree*(ay - ax)*constant;

            if( (curGain > maxGain) ||
               ((curGain==maxGain) && (curGain != 0) && (storedAlready->first < maxIndex)) ) {
                maxGain = curGain;
                maxIndex = storedAlready->first;
            }
        }
        storedAlready++; //Go to the next cluster
    } while ( storedAlready != clusterLocalMap.end() );

    if(cInfo[maxIndex].size == 1 && cInfo[sc].size ==1 && maxIndex > sc) { //Swap protection
        maxIndex = sc;
    }

    return maxIndex;
}//End max()



long maxNoMap(long v, mapElement* clusterLocalMap, long* vtxPtr, double selfLoop, Comm* cInfo, double degree,
              long sc, double constant, long numUniqueClusters ) {

    long maxIndex = sc;	//Assign the initial value as the current community
    double curGain = 0;
    double maxGain = 0;
    long sPosition = vtxPtr[v]+v; //Starting position of local map for v
    double eix = clusterLocalMap[sPosition].Counter - selfLoop;
    double ax  = cInfo[sc].degree - degree;
    double eiy = 0;
    double ay  = 0;

    for(long k=0; k<numUniqueClusters; k++) {
        if(sc != clusterLocalMap[sPosition + k].cid) {
            ay = cInfo[clusterLocalMap[sPosition + k].cid].degree; // degree of cluster y
            eiy = clusterLocalMap[sPosition + k].Counter; 	//Total edges incident on cluster y
            curGain = 2*(eiy - eix) - 2*degree*(ay - ax)*constant;

            if( (curGain > maxGain) ||
               ((curGain==maxGain) && (curGain != 0) && (clusterLocalMap[sPosition + k].cid < maxIndex)) ) {
                maxGain  = curGain;
                maxIndex = clusterLocalMap[sPosition + k].cid;
            }
        }
    }//End of for()

    if(cInfo[maxIndex].size == 1 && cInfo[sc].size ==1 && maxIndex > sc) { //Swap protection
        maxIndex = sc;
    }

    return maxIndex;
}//End maxNoMap()



