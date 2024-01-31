#ifndef SORT_INCLUDE
#define SORT_INCLUDE

#include <vector>

typedef std::pair<int, int> kv;

void swap(int *a, int *b);

int partition(int arr[], int low, int high);

void quickSort(int arr[], int low, int high);

void insertionSort(int arr[], int n);

void insertionSortPair(std::vector<kv> arr, int n);

void swapPair(kv *a, kv *b);

int partitionPair(std::vector<kv> arr, int low, int high);

void quickSortPair(std::vector<kv> arr, int low, int high);

void bubbleSort(int arr[], int n); 

void quickSortIterative(int arr[], int l, int h);
#endif