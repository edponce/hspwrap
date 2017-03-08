/**
 * \file process_control.h
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Contains HSP-Wrap control structures
 *
 * \todo Prefix enum values?
 * \todo Kill all typedefs?
 */

#ifndef HSP_PROCESS_CONTROL_H__
#define HSP_PROCESS_CONTROL_H__

#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>
#include <error.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>

#include "util.h"


/**
 * Set functionality to trace process execution steps (useful for debugging)
 */
//#define TRACE 1
#ifdef TRACE
#define trace(s,...) fprintf(stdout, "[PID:%d, Rank:%d] " s "\n", (int)PID, mpirank, ##__VA_ARGS__)
#ifndef SET_TRACE
#define SET_TRACE 1
#endif
#else
#define trace(...) 
#endif

/**
 * Set functionality to trace function calls (useful for debugging)
 */
//#define FTRACE 1
#ifdef FTRACE
#define ftrace(s,...) fprintf(stdout, "[PID:%d, Rank:%d] %s(" s ")\n", (int)PID, mpirank, __func__, ##__VA_ARGS__)
#ifndef SET_TRACE
#define SET_TRACE 1
#endif
#else
#define ftrace(...) 
#endif

/**
 * Set functionality to output additional information (useful for debugging)
 */
#define INFO 1
#ifdef INFO
#define info(s,...) fprintf(stdout, "[PID:%d, Rank:%d] " s "\n", (int)PID, mpirank, ##__VA_ARGS__)
#ifndef SET_TRACE
#define SET_TRACE 1
#endif
#else
#define info(...) 
#endif

/**
 * Set functionality for master output of additional information (useful for debugging)
 */
#define MINFO 1
#ifdef MINFO
#define minfo(s,...) if (mpirank == MASTER) fprintf(stdout, "[PID:%d, Rank:%d] " s "\n", (int)PID, mpirank, ##__VA_ARGS__)
#ifndef SET_TRACE
#define SET_TRACE 1
#endif
#else
#define minfo(...) 
#endif

/**
 * Set functionality to trace worker process execution steps (useful for debugging)
 */
#define WTRACE 1
#ifdef WTRACE
#define wtrace(s,...) fprintf(stdout, "[PID:%d, Rank:%d, WID:%d] " s "\n", (int)PID, mpirank, (int)WID, ##__VA_ARGS__)
#ifndef SET_TRACE
#define SET_TRACE 1
#endif
#else
#define wtrace(...) 
#endif

/**
 * Set functionality to output additional worker information (useful for debugging)
 */
#define WINFO 1
#ifdef WINFO
#define winfo(s,...) fprintf(stdout, "[PID:%d, Rank:%d, WID:%d] " s "\n", (int)PID, mpirank, WID, ##__VA_ARGS__)
#ifndef SET_TRACE
#define SET_TRACE 1
#endif
#else
#define winfo(...) 
#endif

// Set global variables for output messages
#ifdef SET_TRACE
extern pid_t PID;      //!< Process identifier used for output messages 
extern int mpirank;    //!< MPI rank used for output messages
extern int16_t WID;      //!< Worker process identifier for output messages
#endif

/**
 * Print error messages 
 */
#define ERROR(s,...) fprintf(stderr, "[PID:%d, Rank:%d] Error: " s "\n", (int)PID, mpirank, ##__VA_ARGS__)
//#define ERROR(...) 

/**
 * Print warning messages 
 */
#define WARN(s,...) fprintf(stderr, "[PID:%d, Rank:%d] Warning: " s "\n", (int)PID, mpirank, ##__VA_ARGS__)
//#define WARN(...) 

/**
 * Maximum number of worker processes per slave
 */
#define MAX_PROCESSES 256 

/**
 * Maximum number of database files allowed
 */
#define MAX_DB_FILES 512

/**
 * Maximum length of a file name
 */
#define MAX_FILE_PATH 256

/**
 * Name of shared memory for process control structure
 */
#define PS_CTL_SHM_NAME "ps_ctl"

/**
 * Environment name for process ID
 */
#define PID_ENVVAR "HSPWRAP_PID"

/**
 * Environment name for worker ID
 */
#define WORKER_ID_ENVVAR "HSPWRAP_WID"

/**
 * Macro for decimal printf format for int16_t (worker ID)
 */
#define PRI_WID PRId16

/**
 * Macro for decimal scanf format for int16_t (worker ID)
 */
#define SCN_WID SCNd16

/**
 * Macro for representing an invalid worker identifier (value of -1)
 */
#define BAD_WID 0xFFFF

/**
 * Macro for representing an invalid process identifier (pid_t) (value of -1)
 */
#define BAD_PID 0xFFFFFFFF

/**
 * \brief Worker ID type
 *
 * This represents a local identifier for worker processes. The id is set by assigning an integer from 0 to the number of worker processes (minus 1).
 *
 * \todo We can probably remove this with a better N:M mapping for files
 */
typedef int16_t wid_t;

/**
 * \brief Types to mappings for files based on their usage (in/out/shared)
 */
enum file_type {
  FTE_INPUT,          /**< Input file type */
  FTE_OUTPUT,         /**< Output file type */
  FTE_SHARED          /**< Shared file type */
};

/**
 * \brief Different states a process may be in (set by process)
 */
enum process_state {
  IDLE,      //!< Nothing actually running
  RUNNING,   //!< Process in standard running mode
  EOD,       //!< End of data, give me more data if you have it 
  NOSPACE,   //!< An output buffer is full and needs to be flushed/swapped 
  FAILED,    //!< Processor failed unexpectedly 
  DONE       //!< Processor completed running and terminated for whatever reason
};

/**
 * \brief Command issued between slave and worker processes 
 */
enum process_cmd {
  NO_CMD,    //!< No command is currently provided
  RUN,       //!< More data is available, do something with it
  SUSPEND,   //!< Free as much resources as possible (kill processes, free buffers)
  RESTORE,   //!< Reinitialize after a suspend
  QUIT       //!< Free all resources and terminate
};

/**
 * \brief File description for a virtual file
 * 
 * N:M mapping of files and workers ("wid")
 * \todo N:M mapping of files and workers ("wid")
 * \todo Move "shm" member outside of "file_table_entry".
 */
struct file_table_entry {
  size_t shm_size;            //!< Size of the shm (needed for detach)
  int    shm_fd;              //!< File descriptor of the shm
  wid_t  wid;                 //!< Worker ID owning this file, -1 if shared
  size_t size;                //!< Size of the actual file data within the SHM
  char   name[MAX_FILE_PATH]; //!< Virtual name (path) of the file 
  enum file_type type;        //!< Type of mapping (in/out/shared)
  char  *shm;                 //!< Pointer to shared data
};

/**
 * \brief Table of all virtual files for workers
 */
struct file_table {
  int                     nfiles;                //!< Number of virtual files 
  struct file_table_entry file[MAX_DB_FILES];    //!< Array of virtual files
};

/**
 * \brief Shared process control structure
 * 
 * Shared process control structure. Layout of shared memory.
 * Mutexes and condition variables are used between input layer and processes.
 */
struct process_control {
  unsigned nprocesses;               //!< Number of worker processes
  pthread_mutex_t    lock;           //!< Mutex between input layer and worker processes
  pthread_cond_t     need_service;   //!< Condition variable, worker process needs service
  pthread_cond_t     process_ready[MAX_PROCESSES];    //!< Condition variable, process is ready
  enum process_state process_state[MAX_PROCESSES];    //!< Worker process states, slave uses this to know status of worker processes
  enum process_cmd   process_cmd[MAX_PROCESSES];      //!< Commands used to communicate from slave to worker processes
  struct file_table  ft;             //!< Table of all virtual files
};

/**
 * \brief Initialize shared process control structure.
 *
 * Initialization of shared mutex, condition variables, and process control structure.
 * 
 * \param[in] nprocesses Number of worker processes
 * \param[in] ps_ctl_fd List of file descriptors
 * \retval * Pointer to initialized process control structure
 */
struct process_control *ps_ctl_init (unsigned nprocesses, int *ps_ctl_fd);

/**
 * \brief Creates a shared memory space for a shared files. Updates the virtual file table in process control structure for that file.
 *
 * Creates a shared memory space for shared database files. Updates the virtual file table of the process control structure with the file attributes.
 *
 * \param[in,out] ps_ctl Process control structure (contains virtual file table)
 * \param[in] wid ID of worker process
 * \param[in] name Name of file
 * \param[in] sz Size of file
 * \param[in] type Type of virtual file
 * \retval * Pointer to virtual file added
 */
void *ps_ctl_add_file (struct process_control *ps_ctl, wid_t wid,
                       const char *name, size_t sz, enum file_type type);

/**
 * \brief Check if all worker processes are done.
 *
 * Checks status of all worker processes to see if they are done.
 * DONE is considered the done state.
 *
 * \param[in] ps_ctl Process control structure
 * \retval 0 At least one worker process is not done
 * \retval 1 All worker processes are done
 */
int ps_ctl_all_done (struct process_control *ps_ctl);

/**
 * \brief Check if all worker processes are running.
 *
 * Checks status of all worker processes to see if they are still running.
 * RUNNING is considered the running state.
 *
 * \param[in] ps_ctl Process control structure
 * \retval 0 At least one worker process is not running
 * \retval 1 All worker processes are running
 */
int ps_ctl_all_running (struct process_control *ps_ctl);

/**
 * \brief Check if all worker processes are waiting.
 *
 * Checks status of all worker processes to see if they are still waiting.
 * EOD (end of data) is considered the waiting state.
 *
 * \param[in] ps_ctl Process control structure
 * \retval 0 At least one worker process is not waiting
 * \retval 1 All worker processes are Waiting
 */
int ps_ctl_all_waiting (struct process_control *ps_ctl);

/**
 * \brief Prints virtual files from processs control structure.
 *
 * Prints the virtual files found in the process control structure
 *
 * \param[in] ps_ctl Process control structure
 * \param[in] f Output file to print
 * \retval None
 */
void  ps_ctl_print (struct process_control *ps_ctl, FILE *f);
void printVirtualFilesTable(struct file_table *ft);

#endif // HSP_PROCESS_CONTROL_H__
