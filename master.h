/**
 * \file master.h
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Header file for master process functions
 *
 * Contains initialization, broadcast file, and main functions for master process.
 */

#ifndef MASTER_H__
#define MASTER_H__

#include <assert.h>
#include <errno.h>
#include <error.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mpi.h>

#include "hspwrap.h"
#include "process_pool.h"
#include "controller.h"


/**
 * \brief Slave information structure.
 *
 * The rank corresponds to the MPI rank of the slave processes even though the master sets this rank using consecutive integers starting at 1 (master is always process 0).
 * The sflag is used by the master to keep track if a slave is idle or has data in flight.
 * The request structure is sent from slave to master to tell the master what the slaves wants (work unit or abort execution), and the number of sequences to pack into a work unit.
 * The workunit structure is sent from master to slave, it contains metadata regarding the workunit data, such as type of workunit (work unit data or abort execution), number of sequences, total length of work unit in bytes, and the index identifier of the work unit (which corresponds to the input query file index of the first sequence in the work unit data).
 * The actual work unit data is stored wu_data.
 */
struct controller_info {
  int rank;                    //!< Rank of controller processes [1-ncontrollers]
  int sflag;                   //!< Send flag (0=idle, 1=in flight)
  struct request     request;  //!< Controller request-type and sequence count in work unit
  struct workunit    workunit; //!< Storage for work unit metadata 
  char              *wu_data;  //!< Storage for work unit data
};


/**
 * \brief Master context structure.
 *
 * The controller_info structure is an array used to manage and communicate with controller processes. 
 * mpi_req are used when receiving/waiting for the requests send by controllers.
 * Keeps track of the number controller processes.
 */
struct master_ctx {
  struct controller_info *controllers;    //!< Array of controllers info structures 
  MPI_Request       *mpi_req;        //!< Array of outstanding MPI request-types (request)
  int                ncontrollers;   //!< Number of controllers 
  int                nworkers;       //!< Number of workers 
  int                input_fmt;      //!< Format of input file
  int                shmSz;          //!< Size of shared memory for workunit data
};


/**
 * \brief Reader control structure
 */
struct reader_ctl {
  pthread_cond_t cmd_ready;    //!< Condition variable for checking if command is ready
  pthread_cond_t data_ready;   //!< Condition variable for checking if data is ready
  pthread_mutex_t lock;        //!< Mutex to lock
  ssize_t  read_cnt;           //!< Bytes read
  char    *buf;                //!< Buffer
  int      fd;                 //!< File descriptor
};


/**
 * \brief Initialization of master context.
 *
 * Master context structure is allocated, the number of controllers and workers is set, the input format, 
 *
 * \param[in] ncontrollers Number of controller processes
 * \param[in] nworkers Number of worker processes
 * \param[in] input_fmt Format of input file
 * \retval * No error
 * \retval NULL An error occurred 
 *
 * \todo Threaded read (previous comment).
 */
struct master_ctx *
master_init (int ncontrollers, int nworkers, char input_fmt);


/**
 * \brief Master reader waits bytes are ready, then signals that data is ready.
 *
 * Master reader function, waits until reader reads bytes, gets the number of bytes, invalidates the read count variable, and signal that data is ready.
 *
 * \param[in] arg Not used 
 * \retval * Null pointer
 *
 * \todo Check the lock/unlock mutex mechanism, it seems that the reader count should be locked when invalidated to the value of -1. No need to lock the reader twice in this function.
 */
void *master_reader(void *arg);


/**
 * \brief Broadcast master files 
 *
 * Open/Read file provided and distribute to slave processes in chunks. This function is used to distribute the program binaries and the database shared files. The slave processes use different broadcast_file functions for the binaries and database shared file.
 *
 * \param[in] path Full path to file 
 * \param[in] bcast_chunk_size Chunk size for broadcast messages 
 * \retval sz Size of broadcasted file 
 * \retval -1 An error occurred
 *
 * \todo Threaded read (previous comment):
 * \todo 1. A thread reads file into buffer and another broadcasts buffer
 * \todo 2. Multiple threads read file and broadcast
 * \todo 3. Use MPI I/O
 * \todo 4. Asynchronous send/receive
 * \todo Refactor using the broadcast method used in filterfasta program.
 */
ssize_t master_broadcast_file (const char *path, size_t bcast_chunk_size);


/**
 * \brief Main function for master process. Read input files, services controller requests, and controls controller termination.
 *
 * Master process interacts with the slave processes. It receives/waits for requests from slaves and services them. If it is a work unit request, then the master sends work unit metadata and the data itself. Master keeps track of the sequence indexes and their file offset, outstanding statistics (per slave/total), and active slaves. If the slave request is to abort, master will signal all other slaves to terminate and system is shut down. After all input sequences have been distributed, master waits for all slaves to complete (a signal of abort is sent to slaves).  
 *
 * \param[inout] pool_ctl Process pool control structure
 * \param[inout] master Master process context structure 
 * \retval 0 Successful completion
 * \retval 1 An error occurred
 *
 * \bug Memory leak, free previous malloc
 *
 * \todo Place the "file load and map" code into functions.
 * \todo Merge initialization for loops (vectorizable if possible)
 * \todo Support multiple input files in HSP_INFILES
 * \todo Refactor how input sequences are counted, add validation
 * \todo Slave abort request does not do anything
 * \todo Memory leaks on exits
 */
int  master_main (struct process_pool_ctl *pool_ctl, struct master_ctx *master);

#endif // MASTER_H__
