/**
 * \file util.h
 * \author NICS/ORNL
 * \date July 2014
 *
 * \brief Contains utility functions to be used by any process.
 */

#ifndef UTIL_H__
#define UTIL_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <sys/shm.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#include <mpi.h>


#define HSP_TMP_SHM

/**
 * \brief States for parsing command line arguments
 */
enum parse_cmdline_state {
  PCL_NIL,      //!< Whitespace between words and strings
  PCL_WORD,     //!< Unquoted string
  PCL_DQUOTE,   //!< Double quoted string
  PCL_SQUOTE    //!< Single quoted string
};


/**
 * \brief Timing structure for database communication
 */
struct timing
{
  struct timespec db_start;   //!< Starting timestamp
  struct timespec db_end;     //!< End timestamp
};


/**
 * \brief Optimized broadcast in chunks
 *
 * Send MPI data in chunks using broadcast call. For the sender process, buffer contains the data to be sent. For the receiver processes, buffer is empty and used for storing the received data. Data is read/written directly to buffer without any intermediate chunk buffer.
 *
 * \param[inout] buffer Input/output buffer depends on sender/receiver
 * \param[in] count Number of elements to be transferred
 * \param[in] bcast_chunk_size Chunk size for broadcast messages
 * \param[in] root Root MPI process
 * \param[in] comm MPI communicator
 * \retval MPI_SUCCESS Successful broadcast operation
 * \retval MPI_ERR_ Unsuccessful broadcast operation
 */
int chunked_bcast (void *buffer, size_t count, size_t bcast_chunk_size, int root, MPI_Comm comm);


/**
 * \brief Create shared memory segments (POSIX).
 *
 * Create a shared memory segment for POSIX systems and map it into virtual memory.
 * \param[in] name Name of shared memory segment
 * \param[in] shmsz Size of shared memory segment
 * \param[inout] fd File descriptor of shared file
 * \retval * Pointer to shared memory segment
 * \retval NULL An error occurred
 */
void * create_shm_posix (const char *name, long shmsz, int *fd);


/**
 * \brief Create shared memory segments for V systems
 *
 * Create shared memory segments for V systems and map it into virtual memory.
 *
 * \param[in] offset Offset of shared memory segment
 * \param[in] shmsz Size of shared memory segment
 * \param[inout] fd File descriptor of shared file
 * \retval * Pointer to shared memory segment
 * \retval NULL An error occurred 
 */
void * create_shm_sysv (int offset, long shmsz, int *fd);


/**
 * \brief Memory map shared memory (POSIX)
 *
 * \param[in] name Name for shared memory
 * \param[in] sz Size of shared memory to be mapped
 * \param[inout] fd File descriptor of shared file 
 * \retval * Pointer to mapped memory
 */
void *mmap_shm_posix (const char *name, size_t sz, int *fd);


/**
 * \brief Memory map shared memory (system V)
 *
 * \param[in] key Shared memory key
 * \param[in] sz Size of shared memory to be mapped
 * \param[inout] fd Map identifier
 * \retval * Pointer to mapped memory
 */
void *mmap_shm_sysv (key_t key, size_t sz, int *fd);


/**
 * \brief Select data iterator.
 *
 * Currently, supports FASTA sequence and line-based iterators. Iterator type is based on "input_fmt" flag which is set from HSP_INPUT_FORMAT environment variable. Only the first character from HSP_INPUT_FORMAT is used to set "input_fmt", either 'f' for FASTA sequence iterator and 'l' for line-based iterator.
 *
 * \param[in] input_fmt Format of input file
 * \param[in] s Not used
 * \param[in] e Pointer to end of data
 * \param[in] i Pointer to start position of iteration
 * \retval * Pointer to start of next data if delimiter found
 * \retval * Pointer to end of data if delimiter not found
 * \retval NULL If invalid input format 
 */
char *iter_next (char input_fmt, char *s, char *e, char *i);


/**
 * \brief FASTA sequence iterator
 *
 * FASTA sequence iterator that uses pointer-based iteration to find the beginning of the next sequence. Beginning of sequence is established by this character pair "\n>". The greater than symbol represents the beginning location of the next sequence.
 *
 * \param[in] s Not used
 * \param[in] e Pointer to end of data
 * \param[in] i Pointer to start position of iteration
 * \retval * Pointer to start of next data if delimiter found
 * \retval * Pointer to end of data if delimiter not found
 */
char *iter_fasta_next (char *s, char* e, char *i);


/**
 * \brief Line-based iterator
 *
 * Iterator that uses the newline symbol to find the beginning of the next data. The beginning of the next data starts one character after the first newline encountered.
 *
 * \param[in] s Not used
 * \param[in] e Pointer to end of data
 * \param[in] i Pointer to start position of iteration
 * \retval * Pointer to start of next data if delimiter found
 * \retval * Pointer to end of data if delimiter not found
 */
char *iter_line_next (char *s, char *e, char *i);


/**
 * \brief Duplicate a string, similar to strdup() but a bit faster. 
 *
 * \param[in] str String to copy 
 * \retval * Pointer to new string 
 * \retval NULL An error occurred
 */
char * fast_strdup(const char * str);


/**
 * \brief Strip the last element from a path string. 
 *
 * \param[in] path Path string 
 * \retval * Pointer to last element 
 * \retval NULL An error occurred
 */
char * strip_path(const char * str);

/**
 * \brief Parse command line arguments for wrapped program
 *
 * Parses a command line string into an argument vector (as a shell would do).
 * This function can deal with quoted arguments for spaces, as well as nested
 * quotes. 
 *
 * \param[in] cmdline Command line arguments
 * \retval ** Array of parsed command line arguments
 *
 * \todo Could use lots of work: escaped quotes, other escaped characters, anyway, this works reasonably.
 */
char ** parseCmdline(char *cmdline);


/**
 * \brief Read pathname stopping at each slash symbol (or end of string)  and attempt to create it
 * 
 * \param[in] path Pathname to create
 * \param[in] mode Set mode for path
 * \retval int Return value from mkdir,strdup, or other
 *
 * \todo Verify validation of path and return codes.
 */
int  mkpath (const char *path, mode_t mode);


/**
 * \brief Compute time elapsed in seconds between two time measurements
 *
 * \param[in] t1 Final time stamp
 * \param[in] t0 Initial time stamp
 * \retval double Elapsed time in seconds
 */
double timeval_subtract(struct timespec *t1, struct timespec *t0);


/**
 * \brief Initialize timing structure
 *
 * \param[inout] t Timing structure
 * \retval None
 */
void timing_init(struct timing *t);


/**
 * \brief Get system time
 *
 * \param[inout] ts Time stamp structure
 * \retval None
 */
void timing_record(struct timespec *ts);


/**
 * \brief Print database broadcast timing info
 *
 * \param[in] t Timing structure
 * \retval None
 */
void timing_print(struct timing *t);

#endif  // UTIL_H__

