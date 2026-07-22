/*---------------------------------------------------------------------------*/
/*                                                                           */
/*                          Mahantesh Halappanavar                           */
/*                        High Performance Computing                         */
/*                Pacific Northwest National Lab, Richland, WA               */
/*                                                                           */
/*---------------------------------------------------------------------------*/
/*                                                                           */
/* Copyright (C) 2012 Mahantesh Halappanavar                                 */
/*                                                                           */
/* This program is free software; you can redistribute it and/or             */
/* modify it under the terms of the GNU General Public License               */
/* as published by the Free Software Foundation; either version 2            */
/* of the License, or (at your option) any later version.                    */
/*                                                                           */
/* This program is distributed in the hope that it will be useful,           */
/* but WITHOUT ANY WARRANTY; without even the implied warranty of            */
/* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the             */
/* GNU General Public License for more details.                              */
/*                                                                           */
/* You should have received a copy of the GNU General Public License         */
/* along with this program; if not, write to the Free Software               */
/* Foundation, Inc., 59 Temple Place-Suite 330,Boston,MA 02111-1307,USA.     */
/*                                                                           */
/*---------------------------------------------------------------------------*/
/* Reference: Classic Data Structures in C++ by Timothy Budd                 */
/*---------------------------------------------------------------------------*/

#include "defs.h"
#include <dataStructureHeap.h>

/*
 const int MAX_SIZE = 1600;
 typedef struct
 {
 int maxsize = MAX_SIZE; // Maximum size
 int size;               // Current size
 term * elements;        // vector of elements
 } heap;
 */

//Initialize the heap data structure to size HEAP_MAX_SIZE
void heapInitialize(heap *myHeap) {
    myHeap->maxsize = HEAP_MAX_SIZE;
    myHeap->size = 0;
    term* data = (term *) malloc (HEAP_MAX_SIZE * sizeof(term));
    myHeap->elements = data;
}

void heapInitializeToN(heap *myHeap, long n) {
    myHeap->maxsize = n+1;
    myHeap->size = 0;
    term* data = (term *) malloc (n * sizeof(term));
    myHeap->elements = data;
}

//Add an element to the heap:
void heapAdd(heap *myHeap, term t1) {
    if (myHeap->size < (myHeap->maxsize-1)) {
        //myHeap->size++; //Increment the size
        //Start from the last position and move up to the correct location
        long position = myHeap->size++;
        term *data = myHeap->elements;
        while ( (position > 0)&&(t1.weight<data[(position-1)/2].weight) ){
            data[position].id     = data[(position-1)/2].id;
            data[position].weight = data[(position-1)/2].weight;
            position = (position-1)/2;  //Keep moving up
        }
        //Found the location to insert the new element:
        data[position].id     = t1.id;
        data[position].weight = t1.weight;
    }//End of if()
}//End of heapAdd()

//Remove the minimum element from heap:
void heapRemoveMin(heap *myHeap) {
    term *data = myHeap->elements;
    //First move the last element into the first position:
    if ( myHeap->size > 0 ){
        data[0].id     = data[myHeap->size-1].id;
        data[0].weight = data[myHeap->size-1].weight;
        myHeap->size--; //Decrement the size to reflect the deletion
    } else {
    }
    //Rebuild the heap only if it is still not empty
    if ( myHeap->size > 0 ){
        long position = 0;
        term value;
        value.id     = data[position].id;
        value.weight = data[position].weight;
        
        while ( position < myHeap->size ){
            //Replace position with the smaller of the two children
            //replace with the last element, otherwise
            long childPosition = position*2 + 1;
            if ( childPosition < myHeap->size ){
                if ( (childPosition+1 < myHeap->size) &&
                    (data[childPosition+1].weight < data[childPosition].weight) ) {
                    childPosition += 1;
                }
                //childPosition is smaller of the two children:
                if ( value.weight < data[childPosition].weight ) {
                    //Found the right location:
                    data[position].id     = value.id;
                    data[position].weight = value.weight;
                    break;
                }
                else {
                    data[position].id     = data[childPosition].id;
                    data[position].weight = data[childPosition].weight;
                    position = childPosition;
                    //recur and keep moving down
                }
            }
            else { //No children exist
                data[position].id     = value.id;
                data[position].weight = value.weight;
                break;
            }
        }//End of while()
    }//End of if()
}//End of heapRemoveMin

