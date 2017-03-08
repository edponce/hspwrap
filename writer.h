/**
 * \file writer.h
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Contains functions for writer threads.
 *
 * \todo Rearrange struct members to reduce space used.
 */


#ifndef WRITER_H__
#define WRITER_H__


#include <errno.h>
#include <error.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "hspwrap.h"


/**
 * \brief Writer context structure.
 *
 * Contains metadata, lock, signals, and data buffers to allow communication between writer threads and the slave processes in order to write the workers output data to files using shared memory.
 */
struct writer_ctx {
  const char *name;                  //!< Output file name
  char   *buf;                       //!< Output buffer
  char   *ptr;                       //!< Current pointer for writer
  size_t  size;                      //!< Total size of buffers (HSP_SHM_SZ * number of workers)
  size_t  avail;                     //!< Free space in output buffer
  char   *back_buf;                  //!< Second output buffer (where writer flushes from)
  size_t  back_len;                  //!< Length of valid data in second output buffer
  int     running;                   //!< Run flag for writer thread, 1: keep running, 0: exit main loop
  int     nstreams;                  //!< Number of streams (corresponds to worker index used by slave)
  uint64_t *voffsets;                //!< Virtual offset per stream
  uint64_t  poffset;                 //!< Physical offset within actual file
  int fd;                            //!< Output file descriptor
  int idx;                           //!< Index file descriptor
  pthread_t       thread;            //!< Pthread object
  pthread_mutex_t lock;              //!< Lock for entire writer context
  pthread_cond_t  space_avail;       //!< Signal from writer to slave that there is space in available in writer context (output buffer)
  pthread_cond_t  data_pending;      //!< Signal from slave to writer that writer context has updated data
};


/**
 * \brief Initializes a writer context structure and spawns a writer thread.
 *
 * Writer context fields are initialized using default and argument values, mutex and condition variables are initialized, and a pthread is created with entry function writer_main. This function is called by the slave process once per output file.
 *
 * \param[in] ctx Writer context structure
 * \param[in] name Output file name
 * \param[in] buff_size Size of output buffers
 * \param[in] nstreams Index of worker process data corresponds to
 * \retval 0 Successful termination 
 *
 * \todo Error checking
 */
int  writer_init (struct writer_ctx *ctx, const char *name, size_t buff_size, int nstreams);


/**
 * \brief Write function for writer threads.
 *
 * Data from worker process is copied into the writer context buffer. Metadata is stored in the index file. This function is called indirectly by the slave process.
 *
 * \param[in] ctx Writer context structure
 * \param[in] stream Index of worker process the data corresponds to
 * \param[in] buf Data buffer to write out
 * \param[in] count Size of data buffer provided
 * \retval None
 *
 * \todo Consider FILE*, or at least building a record in memory and one write().
 * \todo Possible race-condition. Writer context index file descriptor may not be initialized yet.
 */
void writer_write (struct writer_ctx *ctx, uint16_t stream, const void *buf, size_t count);


#endif // WRITER_H__

