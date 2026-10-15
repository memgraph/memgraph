#ifndef __input__output
#define __input__output
#include "defs.h"

long removeEdges(long NV, long NE, edge *edgeList); //Remove duplicates
void SortEdgesUndirected(long NV, long NE, edge *list1, edge *list2, long *ptrs);

void loadMetisFileFormat(graph *G, const char* filename); //Metis (DIMACS#10)
bool parse_MatrixMarket(graph * G, char *fileName); //Matrix-Market
void parse_MatrixMarket_Sym_AsGraph(graph * G, char *fileName);

void parse_DirectedEdgeList(dGraph * G, char *fileName); //Directed graph
void parse_UndirectedEdgeListWeighted(graph * G, char *fileName); // for John F's graphs
void parse_UndirectedEdgeList(graph * G, char *fileName);
void parse_EdgeListBinaryNew(graph * G, char *fileName);
void parse_PajekFormatUndirected(graph* G, char* fileName);
void parse_PajekFormat(graph* G, char* fileName);
void parse_Dimacs9FormatDirectedNewD(graph* G, char* fileName);
void parse_SNAP(graph * G, char *fileName);
void parse_SNAP_GroundTruthCommunities(char *fileVertexMap, char *fileGroundTruth);
void parse_UndirectedEdgeListFromJason(graph * G, char *fileName); //Data from Jason
void parse_UndirectedEdgeListDarpaHive(graph * G, char *fileName); //DARPA-HIVE Challenge
void parse_EdgeListFromGorder(graph * G, char *fileName); //Gorder files

void writeGraphBinaryFormatNew(graph* G, char *filename, long weighted);
void writeGraphMetisSimpleFormat(graph* G, char *filename);
void writeGraphMatrixMarketFormatSymmetric(graph* G, char *filename);

void writeGraphPajekFormat(graph* G, char *filename); //Pajek
void writeGraphPajekFormatWithCommunityInfo(graph* G, char *filename, long *C); //Pajek+Community

void writeGraphMatrixMarketFormatSymmetricReordered(graph* G, char *filename, long *old2NewMap);
void writeGraphMatrixMarketFormatBipartiteReordered(graph* G, char *filename, long *old2NewMap);

void parse_EdgeListCompressedHDF5(graph * G, char *fileName);
void parse_EdgeListCompressedHDF5NoDuplicates(graph * G, char *fileName);

using namespace std;

#endif
