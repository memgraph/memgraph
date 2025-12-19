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
#include "utilityClusteringFunctions.h"
#include <algorithm>

using namespace std;

//Assume that clusters have been numbered in a contiguous manner
//Assume that C1 is the truth data
void computeCommunityComparisons(vector<long>& C1, long N1, vector<long>& C2, long N2) {
    int nT;
#pragma omp parallel
    {
        nT = omp_get_num_threads();
    }
    
    assert(N1>0 && N2>0);
    //Compute number of communities in each set:
    //Assume zero is a valid community id
    long nC1=-1;
    bool isZero1 = false;
    for(long i = 0; i < N1; i++) {
        if(C1[i] == 0)
            isZero1 = true; //Check if zero is a valid community
        if (C1[i] > nC1) {
            nC1 = C1[i];
        }
    }//End of for(i)
    if(isZero1)
        nC1++;
    assert(nC1>0);
    //Bug fix for isolated vertices with -1 community assignments
    bool found = false;
    for(long i = 0; i < N1; i++) {
        if(C1[i] == -1) {
            C1[i] = nC1;
            found = true;
        }
    }//End of for(i)
    if(found)
        nC1++;
    
    long nC2=-1;
    bool isZero2 = false;
    for(long i = 0; i < N2; i++) {
        if(C2[i] == -1)
            C2[i] = N2+1; //Bug fix for isolated vertices
        if(C2[i] == 0)
            isZero2 = true; //Check if zero is a valid community
        if (C2[i] > nC2) {
            nC2 = C2[i];
        }
    }//End of for(i)
    if(isZero2)
        nC2++;
    assert(nC2>0);
    //Bug fix for isolated vertices with -1 community assignments
    found = false;
    for(long i = 0; i < N1; i++) {
        if(C2[i] == -1) {
            C2[i] = nC2;
            found = true;
        }
    }//End of for(i)
    if(found)
        nC2++;
    
    //////////STEP 1: Create a CSR-like datastructure for communities in C1
    long * commPtr1 = (long *) malloc ((nC1+1) * sizeof(long)); assert(commPtr1 != 0);
    long * commIndex1 = (long *) malloc (N1 * sizeof(long)); assert(commIndex1 != 0);
    long * commAdded1 = (long *) malloc (nC1 * sizeof(long)); assert(commAdded1 != 0);
    long * clusterDist1 = (long *) malloc (nC1 * sizeof(long)); assert(clusterDist1 != 0); //For Gini coefficient
    
    // Initialization
#pragma omp parallel for
    for(long i = 0; i < nC1; i++) {
        commPtr1[i] = 0;
        commAdded1[i] = 0;
    }
    commPtr1[nC1] = 0;
    // Count the size of each community
#pragma omp parallel for
    for(long i = 0; i < N1; i++) {
        if(isZero1)
            __sync_fetch_and_add(&commPtr1[C1[i]+1],1); //Zero-based indexing
        else
            __sync_fetch_and_add(&commPtr1[C1[i]],1); //One-based indexing
    }
#pragma omp parallel for
    for(long i = 0; i < nC1; i++) {
        clusterDist1[i] = commPtr1[i+1]; //Zeroth position is not valid
    }
    //Prefix sum:
    for(long i=0; i<nC1; i++) {
        commPtr1[i+1] += commPtr1[i];
    }
    //Group vertices with the same community in particular order
#pragma omp parallel for
    for (long i=0; i<N1; i++) {
        long tc = (long)C1[i];
        if(!isZero1)
            tc--; //Convert to zero based index
        long Where = commPtr1[tc] + __sync_fetch_and_add(&(commAdded1[tc]), 1);
        commIndex1[Where] = i; //The vertex id
    }
    free(commAdded1);
    
    //////////STEP 2: Create a CSR-like datastructure for communities in C2
    long * commPtr2 = (long *) malloc ((nC2+1) * sizeof(long)); assert(commPtr2 != 0);
    long * commIndex2 = (long *) malloc (N2 * sizeof(long)); assert(commIndex2 != 0);
    long * commAdded2 = (long *) malloc (nC2 * sizeof(long)); assert(commAdded2 != 0);
    long * clusterDist2 = (long *) malloc (nC2 * sizeof(long)); assert(clusterDist2 != 0);
    
    // Initialization
#pragma omp parallel for
    for(long i = 0; i < nC2; i++) {
        commPtr2[i] = 0;
        commAdded2[i] = 0;
    }
    commPtr2[nC2] = 0;
    // Count the size of each community
#pragma omp parallel for
    for(long i = 0; i < N2; i++) {
        if(isZero2)
            __sync_fetch_and_add(&commPtr2[C2[i]+1],1); //Zero-based indexing
        else
            __sync_fetch_and_add(&commPtr2[C2[i]],1); //One-based indexing
    }
#pragma omp parallel for
    for(long i = 0; i < nC2; i++) {
        clusterDist2[i] = commPtr2[i+1]; //Zeroth position is not valid
    }
    //Prefix sum:
    for(long i=0; i<nC2; i++) {
        commPtr2[i+1] += commPtr2[i];
    }
    //Group vertices with the same community in particular order
#pragma omp parallel for
    for (long i=0; i<N2; i++) {
        long tc = (long)C2[i];
        if(!isZero2)
            tc--;
        long Where = commPtr2[tc] + __sync_fetch_and_add(&(commAdded2[tc]), 1);
        commIndex2[Where] = i; //The vertex id
    }
    free(commAdded2);
    
    //////////STEP 3:  Compute statistics:
    long tSameSame[nT], tSameDiff[nT], tDiffSame[nT], nAgree[nT];
#pragma omp parallel for
    for (int i=0; i < nT; i++) {
        tSameSame[i] = 0;
        tSameDiff[i] = 0;
        tDiffSame[i] = 0;
        nAgree[i]    = 0;
    }
    //Compare all pairs of vertices from the perspective of C1 (ground truth):
#pragma omp parallel
    {
        int myRank = omp_get_thread_num();
#pragma omp parallel for schedule(static)
        for(long ci = 0; ci < nC1; ci++) {
            long adj1 = commPtr1[ci];
            long adj2 = commPtr1[ci+1];
            for(long i=adj1; i<adj2; i++) {
                for(long j=i+1; j<adj2; j++) {
                    //Check if the two vertices belong to the same community in C2
                    if(C2[commIndex1[i]] == C2[commIndex1[j]]) {
                        //__sync_fetch_and_add(&SameSame,1); //Increment the counter: SameSame -- True Positive
                        tSameSame[myRank]++;
                    } else {
                        //__sync_fetch_and_add(&SameDiff,1); //Increment the counter: SameDiff -- False Negative
                        tSameDiff[myRank]++;
                    }
                }//End of for(j)
            }//End of for(i)
        }//End of for(ci)
    }//End of parallel region
#pragma omp parallel
    {
        int myRank = omp_get_thread_num();
        //Compare all pairs of vertices from the perspective of C2:
#pragma omp parallel for schedule(static)
        for(long ci = 0; ci < nC2; ci++) {
            long adj1 = commPtr2[ci];
            long adj2 = commPtr2[ci+1];
            for(long i=adj1; i<adj2; i++) {
                for(long j=i+1; j<adj2; j++) {
                    //Check if the two vertices belong to the same community in C1
                    if(C1[commIndex2[i]] == C1[commIndex2[j]]) {
                        //__sync_fetch_and_add(&nAgree,1); //Increment the counter: SameSame -- True Positive
                        nAgree[myRank]++;
                    } else {
                        //__sync_fetch_and_add(&DiffSame,1); //Increment the counter: DiffSame -- False Positive
                        tDiffSame[myRank]++;
                    }
                }//End of for(j)
            }//End of for(i)
        }//End of for(ci)
    }//End of parallel region
    
    long SameSame = 0, SameDiff = 0, DiffSame = 0, Agree = 0;
#pragma omp parallel for reduction(+:SameSame) reduction(+:SameDiff) \
reduction(+:DiffSame) reduction(+:Agree)
    for (long i=0; i<nT; i++) {
        SameSame += tSameSame[i];
        SameDiff += tSameDiff[i];
        DiffSame += tDiffSame[i];
        Agree    += nAgree[i];
    }
    
    double precision = (double)SameSame / (double)(SameSame + DiffSame);
    double recall    = (double)SameSame / (double)(SameSame + SameDiff);
    
    //F-score (F1 score) is the harmonic mean of precision and recall --
    //multiplying the constant of 2 scales the score to 1 when both recall and precision are 1
    double fScore = 2*((precision * recall) / (precision + recall));
    //Compute Gini coefficient for each cluster:
    double Gini1 = computeGiniCoefficient(clusterDist1, nC1);
    double Gini2 = computeGiniCoefficient(clusterDist2, nC2);
    
    
    //Cleanup:
    free(commPtr1); free(commIndex1);
    free(commPtr2); free(commIndex2);
    free(clusterDist1); free(clusterDist2);
    
} //End of computeCommunityComparisons()


//WARNING: Assume that colorSize is populated with the frequency for each color
//Will sort the array within the function
double computeGiniCoefficient(long *colorSize, int numColors) {
    
    //Step 1: Sort the color size vector -- use STL sort function
    double time1 = omp_get_wtime();
    sort(colorSize, colorSize+numColors);
    double time2 = omp_get_wtime();
    //Step 2: Compute Gini coefficient
    double numFunc=0.0, denFunc=0.0;
    for (long i=0; i < numColors; i++) {
        numFunc = numFunc + ((i+1)*colorSize[i]);
        denFunc = denFunc + colorSize[i];
    }
    double giniCoeff = ((2*numFunc)/(numColors*denFunc)) - ((double)(numColors+1)/(double)numColors);
    
    
    return giniCoeff; //Return the Gini coefficient
}//End of computeGiniCoefficient()


/* Merkin distance computed as described in "Comparing Clustering -- An
 Axiomatic View" by Marina Meila. Proceedings of the 22nd International
 Conference on Machine Learning, Bonn, Germany. 2005.
 */
//Assume that clusters have been numbered in a contiguous manner
double computeMerkinMetric(long* C1, long N1, long* C2, long N2) {
    assert(N1>0 && N2>0);
    assert((C1 != 0) && (C2 != 0));
    //Compute number of communities in each set:
    //Assume zero is a valid community id
    long nC1=-1;
    for(long i = 0; i < N1; i++) {
        if (C1[i] > nC1) {
            nC1 = C1[i];
        }
    }
    assert(nC1>0);
    
    //STEP 1: Create a CSR-like datastructure for communities in C1
    long * commPtr1 = (long *) malloc ((nC1+1) * sizeof(long)); assert(commPtr1 != 0);
    long * commIndex1 = (long *) malloc (N1 * sizeof(long)); assert(commIndex1 != 0);
    long * commAdded1 = (long *) malloc (nC1 * sizeof(long)); assert(commAdded1 != 0);
    
    // Initialization
#pragma omp parallel for
    for(long i = 0; i < nC1; i++) {
        commPtr1[i] = 0;
        commAdded1[i] = 0;
    }
    commPtr1[nC1] = 0;
    // Count the size of each community
#pragma omp parallel for
    for(long i = 0; i < N1; i++) {
        __sync_fetch_and_add(&commPtr1[(long)C1[i]+1],1);
    }
    //Prefix sum:
    for(long i=0; i<nC1; i++) {
        commPtr1[i+1] += commPtr1[i];
    }
    //Group vertices with the same color in particular order
#pragma omp parallel for
    for (long i=0; i<N1; i++) {
        long tc = (long)C1[i];
        long Where = commPtr1[tc] + __sync_fetch_and_add(&(commAdded1[tc]), 1);
        commIndex1[Where] = i; //The vertex id
    }
    
    //Compare all pairs of vertices in each community from C1 to those in C2:
    long nDisagree = 0;
#pragma omp parallel for
    for(long ci = 0; ci < nC1; ci++) {
        long adj1 = commPtr1[ci];
        long adj2 = commPtr1[ci+1];
        for(long i=adj1; i<adj2; i++) {
            for(long j=i+1; j<adj2; j++) {
                //Check if the two vertices belong to the same community in C2
                if(C2[commIndex1[i]] != C2[commIndex1[j]])
                    __sync_fetch_and_add(&nDisagree,1); //Increment the counter
            }//End of for(j)
        }//End of for(i)
    }//End of for(ci)
    
    double dM = (2 * nDisagree) / (N1 * N2);
    
    //Cleanup:
    free(commPtr1); free(commIndex1); free(commAdded1);
    
    return dM;
    
} //End of computeMerkinDistance()

//Assume that clusters have been numbered in a contiguous manner
double computeVanDongenMetric(long* C1, long N1, long* C2, long N2) {
    cout << "Function computeVanDongenMetric() has not been implemented.\n";
}

double computeModularity(graph *G, long* C1) {
    cout << "Function computeModularity() has not been implemented.\n";
}

