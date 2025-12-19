#include "coloringUtils.h"

/*void outPut(const ColorVector &colors, std::string output, const ColorVector& freq, const ColorElem ncolors)
{
	std::ofstream myFile;
	myFile.open(output);
	for(ColorElem ci =0; ci<ncolors; ci++)
		myFile<<ci << " " << freq[ci] << std::endl;
	myFile.close();	

}*/


void computeBinSizes(ColorVector &binSizes, int* colors, long nv, int numColors)
{
	long*  bigHolder;
	#pragma omp parallel default(none), shared(binSizes, colors, bigHolder,numColors, nv)
	{
		const int nthreads = omp_get_num_threads();
		const int ithread = omp_get_thread_num();
		const int ipost = ithread*numColors;

		#pragma omp single 
		{
			bigHolder = new long[numColors*nthreads]() ;
		}

		#pragma omp for schedule(guided)
		for (long ci = 0; ci < nv; ci++){
			bigHolder[colors[ci]+ipost]++;
		}

		#pragma omp for schedule(guided)
		for(int ci=0; ci < numColors; ci++) {
			for(int t=0; t<nthreads; t++) {
				binSizes[ci] += bigHolder[numColors*t + ci];
			}
		}
	}
	delete bigHolder;
}

// Loop to mark the used colors
int distanceOneMarkArray(BitVector &mark, graph *G, long v, int *vtxColor)
{
	long *verPtr = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
  edge *verInd = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)
	int maxColor = -1, adjColor = -1;
	long adj1 = verPtr[v];
	long adj2 = verPtr[v+1];
	
	for (long k = adj1; k < adj2; k++) {
		if(verInd[k].tail == v)
			continue;
		adjColor = vtxColor[verInd[k].tail];
		if (adjColor >= 0) {
			if (adjColor >= MaxDegree) {
				std::cerr << "Maximum number of colors exceeded: " << adjColor << " Increase the MaxDegree"<<std::endl;
				exit(EXIT_FAILURE);
			}
			mark[adjColor] = true;
			if (adjColor > maxColor)
				maxColor = adjColor;
		}
	}
	return maxColor;
}


void distanceOneConfResolution(graph* G, long v, int* vtxColor, double* randValues, long* QtmpTail, long* Qtmp, ColorVector& freq, int type)
{
	long *verPtr = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
  edge *verInd = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)
	int maxColor = -1, adjColor = -1;
	long adj1 = verPtr[v];
	long adj2 = verPtr[v+1];
	
	//Browse the adjacency set of vertex v
	for(long k = adj1; k < adj2; k++ ) {
		if ( v == verInd[k].tail ) //Self-loops
			continue;
		if ( vtxColor[v] == vtxColor[verInd[k].tail] ) {
			if ( (randValues[v] < randValues[verInd[k].tail]) || ((randValues[v] == randValues[verInd[k].tail])&&(v < verInd[k].tail)) ) {
				long whereInQ = __sync_fetch_and_add(QtmpTail, 1);
				Qtmp[whereInQ] = v;//Add to the queue
				if(type!= 0 &&  vtxColor[v] != -1 )
				{
					#pragma omp atomic update 
					freq[vtxColor[v]]--;
				}
				vtxColor[v] = -1;  //Will prevent v from being in conflict in another pairing
				break;
			}
		} //End of if( vtxColor[v] == vtxColor[verInd[k]] )
	} //End of inner for loop: w in adj(v) 
}
/*
void distanceOneConfResolutionWeighted(const Graph &g, const GraphElem &sv,  ColorVector &colors, ColorVector &freq, const RandVec& randVec,  ColorQueue& qtmp, GraphElem& qtmpPos, int type)
{
    GraphElem e0, e1;
    g.getEdgeRangeForVertex(sv, e0, e1);
        for (GraphElem k = e0; k < e1; k++) {
            const Edge &edge = g.getEdge(k);
            if (sv != edge.tail && colors[sv] == colors[edge.tail]){
                if ((randVec[sv] < randVec[edge.tail]) || ((randVec[sv] == randVec[edge.tail]) && (sv < edge.tail))) {
                    GraphElem qtInsPos;
                    #pragma omp atomic capture
                    qtInsPos = qtmpPos++;
                    qtmp[qtInsPos] = sv;
                               //if(colors[sv] != -1 && type != 0)

          				  if(type!= 0 &&  colors[sv] != -1 )
                    {
                      ColorElem vDeg = getDegree(sv,g);
              			  #pragma omp atomic update 
					            freq[colors[sv]] -= vDeg;
				            } 
                    colors[sv]=-1;
				            break;
			          }         
		        }
	      } 
}
*/

void distanceOneChecked(graph* G, long nv ,int* colors)
{
	long *verPtr = G->edgeListPtrs;   //Vertex Pointer: pointers to endV
  edge *verInd = G->edgeList;       //Vertex Index: destination id of an edge (src -> dest)

	for (long ci = 0U; ci < nv; ci++){
		long adj1 = verPtr[ci];
        long adj2 = verPtr[ci+1];
		for (long k = adj1; k < adj2; k++) {
			if(ci != verInd[k].tail && colors[ci] == colors[verInd[k].tail]){
				std::cout<<"Fail"<<std::endl;
				exit(1);
			}
		}
	}
	std::cout<<"Success" <<std::endl;
	return;
}

/*

ColorElem getDegree(const GraphElem ci, const Graph &g)
{
    GraphElem e0,e1;
    g.getEdgeRangeForVertex(ci, e0, e1);
    return (e1-e0);
}


void computeBinSizesWeighted(ColorVector &binSizes,const ColorVector &colors, const GraphElem nv, ColorElem numColors, const Graph &g)
{
	ColorElem*  bigHolder;
        #pragma omp parallel default(none), shared(binSizes, colors, bigHolder,numColors,g)
        {
                const int nthreads = omp_get_num_threads();
                const int ithread = omp_get_thread_num();
                const int ipost = ithread*numColors;

                #pragma omp single 
                {
                        bigHolder = new ColorElem[numColors*nthreads]() ;
                }

                #pragma omp for schedule(guided)
                for (GraphElem ci = 0U; ci < nv; ci++){
                        bigHolder[colors[ci]+ipost] += getDegree(ci,g);
                }

                #pragma omp for schedule(guided)
                for(ColorElem ci=0; ci < numColors; ci++) {
                        for(int t=0; t<nthreads; t++) {
                                binSizes[ci] += bigHolder[numColors*t + ci];
                        }
                }
        }
        delete bigHolder;
}
*/
/*void buildColorsIndex(int* colors, const int numColors, const long nv, ColorVector& colorPtr,  ColorVector& colorIndex, ColorVector& binSizes)
{
	ColorVector colorAdded(numColors,0);
	computeBinSizes(freq,colors,nv,numColors);
	// Build partial sum using the freq(binSizes)
	{
		ColorVector colorPtrSum(numColors+1);
		colorPtrSum[0] = 0;
		std::partial_sum(freq.begin(),freq.end(),colorPtrSum.begin()+1);
		colorPtr=colorPtrSum;
	}
	// Fill in vertices
	#pragma omp parallel for
	for(long vi = 0;vi<nv;vi++){
		const int color = colors[vi];
		long where;
		where =  colorPtr[color] + __sync_fetch_and_add(&(colorAdded[color]), 1);
		colorIndex[where]=vi;
	}
}
*/
