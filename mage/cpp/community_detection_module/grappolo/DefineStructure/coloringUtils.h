#ifndef __ColorUtils__
#define __ColorUtils__
#include <cstdint>

#include <vector>
#include <omp.h>
#include <cassert>
#include <cstdint>

#include <algorithm>
#include <deque>
#include <iostream>
#include <sstream>
#include <vector>
#include <fstream>
#include "RngStream.h"
//##include <timer.h>
#include <ctime>
#include <cstdlib>
#include <omp.h>
#include "defs.h"



/*struct MoveInfo
{
	ColorElem source;
	ColorElem target;
	GraphElem numVertices;
	GraphElem startPost;

};*/

typedef std::vector<bool> BitVector;
typedef std::vector<long> ColorVector;
typedef int ColorElem;
#define MaxDegree 4096
//using namespace std;

int distanceOneMarkArray(BitVector &mark, graph *G, long v, int *vtxColor);
void computeBinSizes(ColorVector &binSizes, int* colors, long nv, int numColors);
void distanceOneConfResolution(graph* G, long v, int* vtxColor, double* randValues, long* QtmpTail, long* Qtmp, ColorVector& freq, int type);
void distanceOneChecked(graph* G, long nv ,int* colors);
void buildColorsIndex(int* colors, const int numColors, const long nv, ColorVector& colorPtr,  ColorVector& colorIndex, ColorVector& binSizes);

/******* UtilityFunctions *****
void computeBinSizes(ColorVector &binSizes, const ColorVector &colors, const GraphElem nv, const ColorElem numColors);
ColorElem getDegree(const GraphElem ci, const Graph &g);
void computeBinSizesWeighted(ColorVector &binSizes, const ColorVector &colors, const GraphElem nv, const ColorElem numColors, const Graph &g);
void outPut(const ColorVector &colors, std::string output, const ColorVector& freq, const ColorElem ncolors);

void buildColorsIndex(const ColorVector& colors, const ColorElem numColors, const ColorElem nv, ColorVector& colorPtr,  ColorVector& colorIndex, ColorVector& binSizes);

//bool findConflicts(const Graph &g, const ColorElem targetColor, const ColorVector &colors, const GraphElem vertex);

void distanceOneConfResolution(const Graph &g, const GraphElem &sv,  ColorVector &colors, ColorVector &freq, const RandVec& randVec,  ColorQueue& qtmp, GraphElem& qtmpPos, int type);

void distanceOneConfResolutionWeighted(const Graph &g, const GraphElem &sv,  ColorVector &colors, ColorVector &freq, const RandVec& randVec,  ColorQueue& qtmp, GraphElem& qtmpPos, int type);


ColorElem distanceOneMarkArray(BitVector &mark, const Graph &g, const GraphElem &sv, const ColorVector &colors);

void distanceOneChecked(const Graph &g, const GraphElem nv ,const ColorVector &colors);
void generateRandomNumbers(std::vector<double> &randVec);

/******* Coloring Functions ******

/* Basic coloring (unbalanced) in initialColoring.cpp
ColorElem initColoring(const Graph &g, ColorVector &colors, std::string input);
/* Basic coloring (ab-inital) in initialColoringLU.cpp
ColorElem initColoringLU(const Graph &g, ColorVector &colors, std::string input);

/* Vertex base redistribution in vBase.cpp
 * type: 0) FF, 1) LU
ColorElem vBaseRedistribution(const Graph &g, ColorVector &baseColors, std::string input, ColorElem ncolors, int type);
ColorElem TrueSerialvBaseRedistribution(const Graph &g, ColorVector &baseColors, std::string input, ColorElem ncolors, int type);
ColorElem wBaseRedistribution(const Graph &g, ColorVector &baseColors, std::string input, ColorElem ncolors, int type);


ColorElem mBaseRedistribution(const Graph &g, ColorVector &baseColors, std::string input, ColorElem ncolors, int type);

/* Color base redistribution in cBase.cpp
 * type: 0) FF, 1) LU
ColorElem cBaseRedistribution(const Graph &g, ColorVector &baseColors, std::string input, ColorElem ncolors, int type);

ColorElem reColor(const Graph &g, ColorVector &baseColors, std::string input, ColorElem ncolors,double factor);
ColorElem schRedistribution(const Graph &g, ColorVector &colors, std::string input, ColorElem ncolors);*/
#endif
