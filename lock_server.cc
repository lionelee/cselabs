// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&smutex, NULL);
  pthread_cond_init(&scond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  printf("request from clt %d acquire %lld\n", clt, lid);
  pthread_mutex_lock(&smutex);
  if(locks.find(lid) != locks.end()){
  	while(locks[lid])
  	  pthread_cond_wait(&scond, &smutex);
  }
  locks[lid] = true;
  pthread_mutex_unlock(&smutex);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  printf("request from clt %d release %lld\n", clt, lid);
  pthread_mutex_lock(&smutex);
  if(locks.find(lid) == locks.end()){
  	printf("lock %lld not found\n",lid);
  	ret = lock_protocol::NOENT;
  } else{
  	locks[lid] = false;
    pthread_cond_broadcast(&scond);
  }
  pthread_mutex_unlock(&smutex);
  r = nacquire;
  return ret;
}
