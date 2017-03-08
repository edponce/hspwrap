/**
 * \file hspwrap.h
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Header for file for master-worker, writer in HSP-Wrap
 *
 * Contains main master-worker functions and utilities.
 *
 * \todo In all source files, set non-argument functions to func(void) instead of func().
 * \todo Make sure all source files end with a newline (an empty line).
 * \todo Global and static variables are set to 0 by the C standard.
 * \todo In malloc, use variable with sizeof instead of its type.
 */

#ifndef HSPWRAP_H__
#define HSPWRAP_H__

#include <stdint.h>
#include <time.h>
#include <mpi.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <ctype.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

//#include "hsp_config.h"
#include "process_control.h"
#include "util.h"

#define HSP_VERSION "1.0"

// Use /tmp or / for shared memory (POSIX only)
//#define HSP_TMP_SHM

// Use System V shared memory (default is POSIX)
//#define HSP_SYSV_SHM


/**
 * Compute number of elements in array
 */
#define ARRAY_SIZE(a) (sizeof(a)/sizeof((a)[0]))

/**
 * Set to zero all bits in an array
 */
#define ZERO_ARRAY(a) memset(a, 0, sizeof(a))

/**
 * Tag for identifying work unit metadata between master and controllers 
 */
#define TAG_WORKUNIT  0

/**
 * Tag for identifying work unit data between master and controllers 
 */
#define TAG_DATA      1

/**
 * Not used. Tag for master to indicate commands
 */
#define TAG_MASTERCMD 2

/**
 * Tag for identifying requests between master and controllers 
 */
#define TAG_REQUEST 3

/**
 * MPI rank for master process
 */
#define MASTER 0


// Default configuration values
/**
 * \brief Default number of worker processes per controller.
 */
#define HSP_NWORKERS 32

/**
 * \brief Default output directory.
 */
#define HSP_OUTDIR "hspwrap-out"

/**
 * \brief Default input directory.
 */
#define HSP_INPDIR "."

/**
 * \brief Default chunk size for MPI communication.
 */
#define HSP_BCAST_CHUNK_SIZE (4<<20)

/**
 * \brief Default shared memory size for virtual files
 */
#define HSP_SHM_SZ (4L<<20)

/**
 * \brief Format type for input files.
 */
#define HSP_INPUT_FORMAT 'l'

/**
 * \brief Default shared output directory
 */
#define HSP_SHARED_DIR "."

/**
 * \brief HSP-Wrap run mode (0 = wrapped, 1 = standard)
 */
#define HSP_STD_MODE 0

/**
 * \brief Set for debugging with gdb. Controller and worker processes sleep to allow user to attach to process using its PID. User also needs to set to 0 value in /etc/sysctl.d/10-ptrace.
 */
//#define HSP_GDB_TRACE 1

/**
 * Provides access to environment variables
 */
extern char **environ;

/**
 * Not used. Block id
 */
typedef uint32_t blockid_t;

/**
 * Not used. Block count
 */
typedef uint16_t blockcnt_t;

/**
 * \brief Types of work units from master to controller 
 */
enum workunit_type {
  WU_TYPE_EXIT,     //!< Exit work unit type
  WU_TYPE_DATA      //!< Data work unit type
};

/**
 * \brief Types of requests from controller to master
 */
enum request_type {
  REQ_WORKUNIT,     //!< Work unit request type
  REQ_ABORT         //!< Abort request type
};

/**
 * \brief Work unit metadata, used between master and controllers.
 */
struct workunit {
  enum workunit_type type;    //!< Type of work unit 
  uint32_t           count;   //!< Number of sequences in work unit
  uint32_t           len;     //!< Length of work unit in bytes 
  uint32_t           blk_id;  //!< Index of work unit, corresponds to the index of the first sequence in the work unit data based on the input query file.
};

/**
 * \brief Request message, controller sends to master
 */
struct request {
  enum request_type  type;    //!< Controller request type
  uint32_t           count;   //!< Requested number of sequences in the work unit 
};


/**
 * \brief Print straight HSP-Wrap banner
 *
 * \param[in] f Not used
 * \retval None
 */
void print_banner(FILE *f);


/**
 * \brief Print slanted HSP-Wrap banner
 *
 * \param[in] f Not used
 * \retval None
 */
void print_banner_slant(FILE *f);


/**
 * \brief Launch wrapped program in standard mode, that is, without HSP-Wrap intervention.
 *
 * Prepares environment to launch the serial version of the wrapped application. The output directory, binary file, environment variables, and command line string are prepared, then the serial program is launched (control changes and does not return).
 *
 * \retval 1 An error occurred
 */
int launchStdMode (void);


/**
 * \brief Perform cleaning operations for HSP-Wrap termination.
 *
 * \retval EXIT_SUCCESS Successful completion
 * \retval EXIT_FAILURE An error occurred
 */
int hspclean_main(void);

#endif // HSPWRAP_H__

