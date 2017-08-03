#include <iostream>
#include <algorithm>
#include "pthread.h"
#include <cstdio>
#include <vector>
#include <string>
#include <cstdlib>
#include "list.h"
#include <ctime>
#include <sys/time.h>
#include <climits>

using namespace std;

#define FOR(i,a,b) for(int i=(a);i<(b);++i)
#define MAXP 128
#define SCAN_ITER 32

double rand_double(){
  return ((double) rand())/RAND_MAX;
}

double rand_int(int max_int){
  return rand_double()*max_int;
}

struct timestamping{
  vector<long long> stamps;
  vector<long long> finished;
  atomic<long long> maxTS;
  int n;
  timestamping(int n_) {
    n = n_;
    stamps.resize(n,0);
    finished.resize(n,0);
    maxTS.store(0);
  }

  long long get_minimal(int id) {
    long long sol = INT_MAX;
    FOR(i,0,n)
      if(finished[i] == 0 && i != id) sol = min(stamps[i],sol);
    return sol;
  }

  long long increase_timestamp(int id) {
    long long newTS = maxTS.fetch_add(1) + 1;
    stamps[id] = newTS;
    return newTS;
  }

  void mark_finished(int id) {
    finished[id] = 1;
  }
};

struct thread_data {
  int id;
  int op_cnt, max_int;
  int add_cnt, remove_cnt, find_cnt, total_op;
  double find_threshold, add_threshold;
  int* values;
  int* tasks;
  timestamping* timestamps;
  int max_buffer_size;
  int reclaimed_cnt;
  vector<node_t<int>* > buffer;

  list_t<int>* list;
  thread_data(list_t<int>* ptr, int id_, timestamping* timestamps_, int max_buffer_size_,
              int op_cnt_, int max_int_, double find_=0.6, double add_ = 0.8) {
    list = ptr;
    id = id_;
    timestamps = timestamps_;
    max_buffer_size = max_buffer_size_;
    reclaimed_cnt = 0;
    op_cnt = op_cnt_;
    max_int = max_int_;
    add_cnt = 0;
    remove_cnt = 0;
    find_cnt = 0;
    total_op = 0;
    find_threshold = find_;
    add_threshold = add_;
    init_tasks();
  }

  void init_tasks() {
    values = (int *)malloc(op_cnt*sizeof(int));
    tasks = (int *)malloc(op_cnt*sizeof(int));
    FOR(i,0,op_cnt) {
      double x = rand_double();
      int n = rand_int(max_int);
      values[i] = n;
      if( x < find_threshold )
        tasks[i] = 0;
      else if (x < add_threshold)
        tasks[i] = 1;
      else
        tasks[i] = 2;
    }
  }
};

void *print_info( void* data) {
  thread_data* ptr = (thread_data*)data;
  cout << "Thread " << ptr->id << endl;
  cout << "Read: " << ptr->find_cnt << " Add: " << ptr->add_cnt << " Remove: " << ptr->remove_cnt << endl;
  cout << "Deallocated: " << ptr->reclaimed_cnt << " To be freed: " << ptr->buffer.size() << endl;
  return NULL;
}

void *print_set( void* ptr ){
  thread_data* data = (thread_data*)ptr;
  FILE *out = fopen("out.txt","w+");
  FOR(i,0,10) {
    list_t<int>::iterator it = data->list->begin();
    list_t<int>::iterator endit = data->list->end();
    while(it!= endit) {
      fprintf(out,"%d ",*it);
      ++it;
    }
    fprintf(out,"\n");
    fflush(out);
  }
  fclose(out);
  return NULL;
}

void scan_buffer(void *ptr){
  thread_data* data = (thread_data*)ptr;
  node_t<int>* tmp;
  int min_timestamp = data->timestamps->get_minimal(data->id);
  printf("Memory reclamation process %d Min timestamp %lld Size %d\n",data->id,min_timestamp,data->buffer.size());
  vector<node_t<int>* > tmp_buffer;
  FOR(i,0,data->buffer.size()) {
    int ts = (data->buffer[i])->timestamp;
    node_t<int>* next = list_t<int>::get_unflagged(data->buffer[i]->next.load());
    //printf("Deleting: %d %d %d %d %lld %d\n",data->id,i,ts,data->buffer[i]->data,(long long)data->buffer[i],data->buffer[i]->ref_count.load());
    if (ts < min_timestamp && data->buffer[i]->ref_count.load() == 0) {
      next->ref_count.fetch_add(-1);
      free(data->buffer[i]);
      ++(data->reclaimed_cnt);
    }
    else {
      tmp_buffer.push_back(data->buffer[i]);
    }
  }
  data->buffer = tmp_buffer;
}

void *test(void *ptr){
    thread_data* data = (thread_data*)ptr;
    int opcnt = data->op_cnt;
    int maxint = data->max_int;
    int id = data->id;
    list_t<int>* list = data->list;
    FOR(i,0,opcnt) {
      /*
      double x = rand_double();
      int n = rand_int(maxint);
      //cout << x << " " << n << endl;
      if (x < data->find_threshold) {
        //cout << 0 << endl;
        list->contains(n);
        ++(data->find_cnt);
      } else if(x < data->add_threshold) {
        //cout << 1 << endl;
        if(list->add(n)) {
            ++(data->add_cnt);
        }
      } else {
        //cout << 2 << endl;
        if(list->remove(n)) {
            ++(data->remove_cnt);
        }
      }
      ++(data->total_op);
      */
      int n = data->values[i];
      int op = data->tasks[i];
      long long ts = data->timestamps->increase_timestamp(id);

      //printf("Time: %lld Process: %d Operation count: %d Operation type: %d Value: %d\n",ts,id,i,op,n);

      if (op == 0) {
        list->contains(n);
        ++(data->find_cnt);
      } else if(op == 1) {
        if(list->add(n)) {
          ++(data->add_cnt);
        }
      } else {
        node_t<int>* node_ptr = list->remove(n);
        if(((long long) node_ptr)%4 !=0 ){
          printf("oslo u pm\n"); fflush(stdout);
          exit(0);
        }
        if(node_ptr != NULL ) {
            node_ptr->timestamp = data->timestamps->maxTS.load() + 1;
            data->buffer.push_back(node_ptr);
            //printf("Process %d at time %d added reclamation node: %d %lld\n",id,node_ptr->timestamp,node_ptr->data,(long long)node_ptr);
            ++(data->remove_cnt);
        }
      }
      fflush(stdout);
      if( i % SCAN_ITER == 0 &&  data->buffer.size() >= data->max_buffer_size )
              scan_buffer(ptr);
    }
    data->timestamps->mark_finished(id);

    return NULL;
}

int main(int argc, char **argv){

  int P = 1;
  int op_cnt = 100;

  if(argc > 1){
    sscanf(argv[1],"%d",&P);
    sscanf(argv[2],"%d",&op_cnt);
  }
  int max_int = 2048;
  int limit = 1e9;
  int initial = max_int/2;
  int max_buffer_size = max_int/16;
  struct timeval start,end;
  timestamping timestamps(P);

  vector<pthread_t> threads(P+1);
  vector<thread_data> data;

  list_t<int> *list = new list_t<int>(-limit,limit);

  cout << "constructed list" << endl;
  FOR(i,0,initial) {
    list->add(i);
  }
  cout << "initialized list elements" << endl;

  FOR(i,0,P) data.push_back(thread_data(list,i,&timestamps,max_buffer_size,op_cnt,max_int));

  cout << "created thread inputs" << endl;

  gettimeofday(&start,NULL);

  FOR(i,0,P) pthread_create(&threads[i],NULL,test,((void *)(&data[i])));
  pthread_create(&threads[P],NULL,print_set,((void *)(&data[0])));

  cout << "created threads" << endl;

  FOR(i,0,P+1) pthread_join(threads[i],NULL);
  gettimeofday(&end,NULL);

  cout << "execution finished" << endl;

  FOR(i,0,P) print_info((void*)(&data[i]));
  int exp_len = initial;
  FOR(i,0,P)
    exp_len += (data[i].add_cnt - data[i].remove_cnt);
  uint64_t duration = (end.tv_sec*(uint64_t)1000000 + end.tv_usec) - (start.tv_sec*(uint64_t)1000000 + start.tv_usec);

  cout << "Actual length: " << list->length.load() << endl;
  cout << "Expected length: " << exp_len << endl;
  cout << "Time(s): " << duration/1000000.0 << endl;

  return 0;
}
