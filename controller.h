/**
 * \file controller.h
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Contains functions for slave processes.
 *
 * Contains initialization, broadcast of files, and main functions for slave processes.
 *
 * \todo Wrap slave global variables in a slave context structure (similar to master).
 */

#ifndef SLAVE_H__
#define SLAVE_H__

#include <assert.h>
#include <errno.h>
#include <error.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <mpi.h>

#include "process_control.h"
#include "process_pool.h"
#include "writer.h"
#include "hspwrap.h"

/*
 * \brief Linked list of cache buffer structures. Slave processes store the work unit metadata and data sent by the master process in this buffers.
 */
struct cache_buffer
{
  struct cache_buffer *next;   //!< Pointer to next cache buffer
  char *r_ptr;                 //!< Current read pointer in data buffer
  size_t size;                 //!< Size of data buffer
  int count;                   //!< Number of sequences in data buffer
  int len;                     //!< Length of valid data
  char data[];                 //!< Work unit data buffer
};

/*
 * \brief Stores an array of filenames. Used for multiple input/output files. 
 */
struct fileSet
{
  //char fn[1][MAX_FILE_PATH];     //!< File names
  char **fn;                   //!< File names
  int nfiles;                  //!< Number of files
};

/*
 * \brief Controller context structure for managing its operations.
 */
struct controller_ctx
{
  struct process_control *ps_ctl;    //!< Shared process control structure
  struct writer_ctx *writers;        //!< Writer context structure
  char *outdir;                      //!< Output directory for workers
  int rank;                          //!< MPI rank
  int nworkers;                      //!< Number of workers
  struct fileSet *ifiles;            //!< Input files 
  struct fileSet *ofiles;            //!< Output files 
  char input_fmt;                    //!< Format of input files
  int shmSz;                         //!< Buffer size for shared memory files
};

/**
 * \brief Creates and changes to output directory, initializes shared process control, and maps input/output files to streams.
 *
 * This function is called per controller process, where rank is the MPI rank of the controller process and nworkers is the number of worker processes. The output directory gets created and process changes to directory as well. Initializes the shared process control structure. Maps the input and output file to shared memory streams. 
 *
 * \param[in] rank Index of controller process
 * \param[in] nworkers Number of worker processes
 * \param[in] input_fmt Format of input file 
 * \retval * No error 
 * \retval NULL An error occurred
 *
 * \todo Check length of buffer allocation for directory names.
 * \todo Support multiple input files
 */
struct controller_ctx * controller_init (int rank, int nworkers, char input_fmt);


/**
 * \brief Controller processes receive shared database files from master process.
 *
 * Controller processes receive shared files from master process. Shared memory is allocated and included into the virtual file table of the global process control structure.
 *
 * \param[in] ps_ctl Pointer to shared process control structure 
 * \param[in] path Pathname of shared database file
 * \param[in] bcast_chunk_size Chunk size for broadcast messages 
 * \retval ssize_t Size of shared database file received
 * \retval -1 An error occurred 
 */
ssize_t controller_broadcast_shared_file (struct process_control *ps_ctl, const char *path, size_t bcast_chunk_size);


/**
 * \brief Controller processes receive program binary from master process.
 *
 * This function is called per controller process to receive the program binary file. The controller creates the file and memory maps, then copies the file in chunks from master process.
 *
 * \param[in] path Pathname of binary file
 * \param[in] bcast_chunk_size Chunk size for broadcast messages 
 * \retval n Number of bytes broadcasted 
 * \retval -1 An error occurred
 */
ssize_t controller_broadcast_work_file (const char *path, size_t bcast_chunk_size);


/**
 * \brief Main function for controller processes.
 *
 * \param[inout] pool_ctl Process pool control structure
 * \param[inout] controllers Controller context structure
 * \retval 0 Successful termination
 * \retval 1 An error occurred 
 *
 * \todo Maybe move some stuff to a controller_init() function (previous comment).
 * \todo Consider reworking some of the logic here, especially if we add support for spawning processes one at a time. We could start processing, or at least cold-start the processes before receiving all the data.
 */
int controller_main (struct process_pool_ctl *pool_ctl, struct controller_ctx *controllers);


#endif // SLAVE_H__

