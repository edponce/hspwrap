/**
 * \file process_pool.h
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Header file for process pool control functions
 *
 * Contains start and forking functions.
 *
 * \todo Enable Linux sched support for cpu-affinity, make configurable. Need to define _GNU_SOURCE and include sched.h.
 */

#ifndef PROCESS_POOL_H__
#define PROCESS_POOL_H__

#include <limits.h> // PATH_MAX
#include <unistd.h> // pid_t
#include <ctype.h>
#include <errno.h>
#include <error.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>

// For SHM/MMAP stuff
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <fcntl.h>

// Linux-specific (TODO: make configurable)
#include <sched.h>
#include <pthread.h>

#include "hspwrap.h"


/**
 * Name of shared memory for process pool control structure
 */
#define POOL_CTL_SHM_NAME "pool_ctl"

/**
 * Compute number of elements in array
 * 
 * \todo Move to util lib
 */
#define ARRAY_SIZE(a) (sizeof(a)/sizeof((a)[0]))


/**
 * \brief Worker process structure
 */
struct worker_process {
  wid_t        wid;                   //!< Worker local identifier
  pid_t        pid;                   //!< Worker process id when forked
  int          status;                //!< Status flag (0=not forked, 1=forked)
};


/**
 * \brief Process pool control structure
 */
struct process_pool_ctl {
  pthread_mutex_t lock;               //!< Structure lock
  pthread_cond_t  wait;	              //!< Block on create until pool is ready 
  pthread_cond_t  run;	              //!< Pool blocks until pool is ready to run
  int             ready;              //!< Ready flag (0=not ready, 1=ready, -1=error)
  int             nprocesses;         //!< Number of active worker processes
  char            workdir[PATH_MAX];  //!< Working directory, need to switch
};
  

/**
 * \brief Initialize process pool control structure
 *
 * Sets mutex and condition variables attributes, create and map to shared memory, and initialize process pool control structure.
 *
 * \param[in] hspwrap_pid HSP-Wrap process identifier
 * \retval * Pointer to shared process control structure 
 * \retval NULL An error occurred
 */
struct process_pool_ctl * process_pool_ctl_init (pid_t hspwrap_pid);


/**
 * \brief Process pool start for worker processes
 *
 * Get command line arguments for wrapped program, attach to shared process control structure, initialize worker process structure, fork worker processes, waits for worker processes to complete, change the worker process state to "DONE" with "NO_CMD", and signal the worker process that it needs service.
 * 
 * \param[in] wrapper_pid Wrapper process identifier
 * \param[in] pool_pid Pool process identifier
 * \param[in] lexefile Local executable file for worker execution 
 * \param[in] wrapper_pid Wrapper process identifier
 * \param[in] workdir Working directory
 * \param[in] nproc Number of processes
 * \retval 0 Successful completion
 * \retval 1 An error occurred
 *
 * \bug In call to parse_cmdline, leaks the duplicate and does not check for NULL
 *
 * \todo Refactor how memory mapped attachment is done, it is used here and in stdiowrap.
 */
//int process_pool_start (pid_t wrapper_pid, pid_t pool_pid, char *lexefile, const char *workdir, int nproc);
int process_pool_start (pid_t wrapper_pid, pid_t pool_pid, struct process_pool_ctl *pool_ctl);


/**
 * \brief Fork process pool
 *
 * Calls process pool initialization and forks a temporary process which in turn forks the process pool main process in a new session. The process pool process blocks until master process sets the number of active worker processes.
 *
 * \param[in] lexefile Name of exefile for worker execution 
 * \retval * Pointer to shared process pool structure 
 * \retval NULL An error occurred
 *
 * \todo Why are two levels of forked processes used?
 */
struct process_pool_ctl * process_pool_fork ();


#endif // PROCESS_POOL_H__

