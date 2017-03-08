/**
 * \file util.c
 * \author NICS/ORNL
 * \date July 2014
 *
 * \brief Contains utility functions to be used by any process.
 */


#include "util.h"


/**
 * \brief Find minimum value of size_t objects
 *
 * \param[in] a First value
 * \param[in] b Second value
 * \retval Minimum value
 */
static size_t MIN (size_t a, size_t b)
{
  return (a < b) ? a : b;
}


/**
 * \brief Find maximum value of size_t objects
 *
 * \param[in] a First value
 * \param[in] b Second value
 * \retval Maximum value
 */
static size_t MAX (size_t a, size_t b)
{
  return (a < b) ? b : a;
}


// Send MPI data in chunks using broadcast call. For the sender process, buffer contains the data to be sent. For the receiver processes, buffer is empty and used for storing the received data. Data is read/written directly to buffer without any intermediate chunk buffer.
int
chunked_bcast (void *buffer, size_t count, size_t bcast_chunk_size, int root, MPI_Comm comm)
{
  size_t  off;            // Data offset for send/receive buffer
  size_t  chunk_sz;       // Size of current chunk
  int     rc;             // Return value from MPI calls

  // Write data (MPI+mmap work-around)
  // Iterate on data based on chunk size blocking
  for (off=0; off<count; off+=chunk_sz) {

  // Compute size of current chunk
  chunk_sz = MIN(bcast_chunk_size, count-off);
    
  // Broadcast and handle error
  rc = MPI_Bcast(buffer+off, chunk_sz, MPI_CHAR, root, comm);
    if (rc != MPI_SUCCESS) {
      return rc;
    }
  }

  // Done
  return MPI_SUCCESS;
}


// Create shared memory for V systems
void *
create_shm_sysv (int offset, long shmsz, int *fd)
{
  void *shm;       // Pointer to shared memory
  int shmfd;       // File descriptor of shared file
  key_t id;        // Key for shared memory
  int rc;          // Function return code

  // Create shared memory object
  id = getpid() + offset;
  shmfd = shmget(1337+id, shmsz, IPC_CREAT | IPC_EXCL | S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  if (shmfd < 0) {
    fprintf(stderr, "Error: failed to make shared memory.\n");
    return NULL;
  }

  // Attach shared memory segment
  shm = shmat(shmfd, NULL, 0);
  if (shm == ((void*)-1)) {
    fprintf(stderr, "Error: failed to make attach shared memory.\n");
    return NULL;
  }

  // Mark the shared memory segment to be destroyed after all processes detach
  rc = shmctl(shmfd, IPC_RMID, NULL);
  if (rc < 0)
    fprintf(stderr, "Warning: failed to set shared memory control operation.\n");

  // Return the identifier for the shared memory segment via function argument
  if (fd)
    *fd = shmfd;

  // Return the created shared memory segment 
  return shm;
}


// Create shared memory for POSIX systems
void *
create_shm_posix (const char *name, long shmsz, int *fd)
{
  void *shm;            // Pointer to shared memory
  int shmfd;            // File descriptor of shared file
  int rc;               // Function return code

  // Create file descriptor for shared file object
#ifdef HSP_TMP_SHM
  char shmname[256];    // Name of shared memory
  rc = snprintf(shmname, 256, "/tmp%s", name);
  if (rc < 0) {
    fprintf(stderr, "Error: could not create shared memory name.\n");
    return NULL;
  }

  shmfd = open(shmname, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  if (shmfd == -1) {
    remove(shmname);
    shmfd = open(shmname, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (shmfd == -1) {
      fprintf(stderr, "Error: failed to create shared memory.\n");
      return NULL;
    }
  }
#else
  shmfd = shm_open(name, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  if (shmfd == -1) {
    shm_unlink(name);
    shmfd = shm_open(name, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (shmfd == -1) {
      fprintf(stderr, "Error: failed to create shared memory.\n");
      return NULL;
    }
  }
#endif

  // Set size of shared memory segment 
  rc = ftruncate(shmfd, shmsz);
  if (rc == -1) {
#ifdef HSP_TMP_SHM
    close(shmfd);
#else
    shm_unlink(name);
#endif
    fprintf(stderr, "Error: failed to resize shared memory.\n");
    return NULL;
  }

  // Create a shared memory map
  shm = mmap(NULL, shmsz, PROT_READ | PROT_WRITE, MAP_SHARED, shmfd, 0);
  if (shm == MAP_FAILED) {
#ifdef HSP_TMP_SHM
    close(shmfd);
#else
    shm_unlink(name);
#endif
    fprintf(stderr, "Error: failed to attach shared memory.\n");
    return NULL;
  }

  // Return file descriptor of the created shared memory via function argument
  if (fd) 
    *fd = shmfd;

  // Return pointer to shared memory map
  return shm;
}


// Create shared memory for POSIX systems
void *
mmap_shm_posix (const char *name, size_t sz, int *fd)
{
  void *shm;   // Pointer to shared memory
  int shmfd;   // File descriptor of shared file

  // Create file descriptor for shared file object
#ifdef HSP_TMP_SHM
  char shmname[256];
  sprintf(shmname, "/tmp%s", name);
  shmfd = open(shmname, O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  if (shmfd == -1) {
    remove(shmname);
    shmfd = open(shmname, O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (shmfd == -1) {
      fprintf(stderr, "pool: Failed to open SHM (%s): %s\n", shmname, strerror(errno));
      return NULL;
    }
  }
#else
  shmfd = shm_open(name, O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  if (shmfd == -1) {
    shm_unlink(name);
    shmfd = shm_open(name, O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (shmfd == -1) {
      fprintf(stderr, "pool: Failed to open SHM (%s): %s\n", name, strerror(errno));
      return NULL;
    }
  }
#endif

  // Create a shared memory map
  shm = mmap(NULL, sz, PROT_READ | PROT_WRITE,
             MAP_SHARED /*| MAP_LOCKED | MAP_HUGETLB*/,
             shmfd, 0);
  if (shm == MAP_FAILED) {
    fprintf(stderr, "pool: Failed to map SHM (%s): %s\n", name, strerror(errno));
    return NULL;
  }

  // Return file descriptor of the created shared memory via function argument
  if (fd)
    *fd = shmfd;
 
  // Return pointer to shared memory map
  return shm;
}


// Create shared memory for V systems
void *
mmap_shm_sysv (key_t key, size_t sz, int *fd)
{
  void *shm;    // Pointer to shared memory
  int shmfd;    // File descriptor of shared file

  // Create shared memory object
  fprintf(stderr, "mmap_shm getting shm %d ...\n", key);
  shmfd = shmget(1337+key, sz, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP); 
  if (shmfd == -1) {
    fprintf(stderr, "pool: Fail to get SHM with key %d: %s\n", key, strerror(errno));
    return NULL;
  }

  // Attach the shared memory segment
  fprintf(stderr, "mmap_shm attaching shm %d ...\n", key);
  shm = shmat(shmfd, NULL, 0);
  if (shm == ((void *) -1)) {
    fprintf(stderr, "pool: Failed to attach SHM with key %d: %s\n", key, strerror(errno));
    return NULL;
  }

  // Our parent already marked the SHMs for removal, so they will cleaned up for us later.
  fprintf(stderr, "mmap_shm done %d \n", key);

  // Return the identifier for the shared memory segment via function argument
  if (fd)
    *fd = shmfd;

  // Return the created shared memory segment
  return shm;
}


// Line-based iterator
char *
iter_line_next (char *s, char *e, char *i)
{
  char *c = i;
  for (c=i; c<e; ++c) {
    if (*c == '\n') {
      return c+1;
    }
  }
  return e;
}


// FASTA sequence iterator
char *
iter_fasta_next (char *s, char *e, char *i)
{
  char last_c = ' ';
  char *c = i;

  for (c=i; c<e; ++c) {
    if (*c == '>' && last_c == '\n') {
      return c;
    }
    last_c = *c;
  }
  return e;
}


// Current iterator
char *
iter_next (char input_fmt, char *s, char *e, char *i)
{
  switch (input_fmt) {
    case 'f':
      return iter_fasta_next(s, e, i);
    case 'l':
      return iter_line_next(s, e, i);
    default:
      return NULL;
  }
}


// Duplicate a string, similar to strdup() but a bit faster
char * fast_strdup (const char * str) {
  size_t len = strlen(str) + 1;
  char *p = malloc(len);
  return (p) ? memcpy(p, str, len) : NULL;
}


// Get last element from a path string
char * strip_path (const char *path) {
  char *s;
  size_t len;
  if (s = strrchr(path, '/'))
    return ++s;
  else {
    len = strlen(path) + 1;
    return (s = malloc(len)) ? memcpy(s, path, len) : NULL;
  }
}


// Parse command line arguments of wrapped program
char ** parseCmdline (char *cmdline) {
  enum parse_cmdline_state q;
  char **args, **a;
  char *p, *r;
  int cnt;

  // Parse
  q = PCL_NIL;
  r = cmdline;
  cnt = 0;
  for (p=cmdline; *p; ++p) {
    switch (q) {
      // Whitespace between words and strings
      case PCL_NIL:
        if (isspace(*p)) {
          continue;
        }
        switch (*p) {
          case '"':
            q = PCL_DQUOTE;
            continue;
          case '\'':
            q = PCL_SQUOTE;
            continue;
          default:
            q = PCL_WORD;
            *r++ = *p;
            continue;
        }

      // Unquoted string
      case PCL_WORD:
        if (isspace(*p)) {
          *r++ = '\0';
          cnt++;
          q = PCL_NIL;
          continue;
        }
        switch (*p) {
          case '"':
            q = PCL_DQUOTE;
            continue;
          case '\'':
            q = PCL_SQUOTE;
            continue;
          default:
            *r++ = *p;
            continue;
        }

      // Double-quoted string
      case PCL_DQUOTE:
        if (*p == '"') {
          q = PCL_WORD;
        } else {
          *r++ = *p;
        }
        continue;

      // Single-quoted string
      case PCL_SQUOTE:
        if (*p == '\'') {
          q = PCL_WORD;
        } else {
          *r++ = *p;
        }
        continue;
    }
  }
  // Null terminate last word (if any)
  if (q == PCL_WORD) {
    *r++ = '\0';
    cnt++;
  }

  // Build array
  args = malloc((cnt+1) * sizeof(char *));
  a = args;
  p = cmdline;
  while (cnt) {
    // Mark beginning of argument
    *a++ = p;
    // Eat rest of argument
    while (*p != '\0') {
      p++;
    }
    // Eat null
    p++;
    cnt--;
  }
  *a = NULL;

  return args;
}

// Make directory for slave outputs
int
mkpath (const char *path, mode_t mode)
{
  char *c = NULL;  // Character iterator
  char *p = NULL;  // Path duplicate
  int rc = 0;      // Function return code
  int len = 0;

  // Get a copy of the path
  p = strdup(path);
  if (p == NULL) {
    fprintf(stdout, "Error: could not create copy of directory name, strdup() failed.\n");
    return EXIT_FAILURE;
  }

  // Remove ending slash
  len = strlen(p);
  if (p[len-1] == '/')
    p[len-1] = '\0';

  // Set starting point
  if (p[0] == '/')
    c = p + 1;
  else
    c = p;

  // Grow the string one section at a time and mkdir
  // Read pathname stopping at each slash symbol (or end of string)  and attempt to create it
  for (;; ++c) {
    // Found a depth of the directory tree
    if (*c == '/') {
      *c = '\0';
      rc = mkdir(p, mode);
      if (rc == -1 && errno != EEXIST) {
        fprintf(stderr, "Error: could not make directory.\n");
        free(p);
        return EXIT_FAILURE;
      }
      *c = '/';
    }
    // Make directory if last directory of tree or single directory
    else if (*c == '\0') {
      rc = mkdir(p, mode);
      if (rc == -1) {
        if (errno == EEXIST)
          fprintf(stderr, "Warning: directory exists, overwriting may occur.\n");
        else {
          fprintf(stderr, "Error: could not make directory.\n");
          free(p);
          return EXIT_FAILURE;
        }
      }
      break;
    }  
  }

  // Free memory 
  free(p);

  return EXIT_SUCCESS;
}


////////////////////// Time Utility Functions /////////////////
double
timeval_subtract (struct timespec *t1, struct timespec *t0)
{
  struct timespec rv;  // Local time stamp structure
  long sec;            // Temporary computed seconds

  // Perform the carry for the later subtraction by updating t0. */
  if (t1->tv_nsec < t0->tv_nsec) {
    sec = (t0->tv_nsec - t1->tv_nsec) / 1000000000 + 1;
    t0->tv_nsec -= 1000000000 * sec;
    t0->tv_sec  += sec;
  }
  if (t1->tv_nsec - t0->tv_nsec > 1000000000) {
    sec = (t1->tv_nsec - t0->tv_nsec) / 1000000000;
    t0->tv_nsec += 1000000000 * sec;
    t0->tv_nsec -= sec;
  }


  // Compute the difference
  rv.tv_sec = t1->tv_sec - t0->tv_sec;
  rv.tv_nsec = t1->tv_nsec - t0->tv_nsec;

  return ((double)rv.tv_sec) + (rv.tv_nsec / 1000000000.0f);
}


void
timing_init (struct timing *t)
{
  // Reset timing structure
  memset(t, 0, sizeof(struct timing));
}


void
timing_record (struct timespec *ts)
{
  // Get system time stamp
  clock_gettime(CLOCK_MONOTONIC, ts);
}


void
timing_print (struct timing *t)
{
  double s;  // Elapsed time in seconds

  s = timeval_subtract(&t->db_end, &t->db_start);
  fprintf(stderr, "Timing Information: %8.2lf sec\n", s);
}

