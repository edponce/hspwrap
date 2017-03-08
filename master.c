/**
 * \file master.c
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Contains functions for master process.
 *
 * Contains initialization, broadcast file, and main functions for master process.
 */


#include "master.h"


// Global variables
pthread_t reader_thread;                     // Reader thread
struct reader_ctl reader;                    // Reader control structure


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


/**
 * \brief Sets number of slave or worker processes in process pool control structure and sets the working directory to enable spawning of slave/worker processes.
 *
 * This function is called by both, the master process and the slave processes. Sets the number of slave/worker processes in process pool control structure, sets the working directory, and signals that control structure is ready to run.
 *
 * \param[in] pool_ctl Process pool control structure
 * \param[in] workdir Working directory
 * \param[in] nprocs Number of slave/worker processes
 * \retval None
 */
static void
process_pool_spawn (struct process_pool_ctl *pool_ctl, const char *workdir, int nprocs)
{
  // Set the number of slave/worker processes
  pool_ctl->nprocesses = nprocs;

  // Set the working directory
  strncpy(pool_ctl->workdir, workdir, PATH_MAX);

  // Signal the process pool control is ready to run
  pthread_cond_signal(&pool_ctl->run);
}


// Initialization of master process
struct master_ctx *
master_init (int ncontrollers, int nworkers, char input_fmt)
{
  int i;        // Iteration variable
  int nreqs;    // Number of slave requests
  struct master_ctx *master = NULL;     // Master context structure
  char *shmSz = NULL;

  info("Call to master_init()");
 
  // Allocate space for master context structure
  master = malloc(sizeof(struct master_ctx));
  if (!master) {
    ERROR("could not allocate space for master context.");
    return NULL;
  }

  // Initialize master context numeric/char members 
  master->ncontrollers = ncontrollers;
  master->nworkers = nworkers;
  master->input_fmt = input_fmt;
  info("Initializing master process (%d controllers)", ncontrollers);
  
  // Allocate array of controller information structures
  master->controllers = malloc(ncontrollers * sizeof(struct controller_info));
  if (!master->controllers) {
    ERROR("failed to allocate space for controller information.");
    return NULL;
  }

  // Get size of shared memory buffers from environment
  shmSz = getenv("HSP_SHM_SZ");
  if (!shmSz || (int)(*shmSz) == 0) {
    WARN("could not find shared memory size in environment, using default.");
    shmSz = NULL;
    master->shmSz = HSP_SHM_SZ;
  }
  else
    master->shmSz = atoi(shmSz);
  info("Shared memory buffer size, %d", master->shmSz);

  // Initialize controller information structure, work unit buffer
  for (i = 0; i < ncontrollers; ++i) {
    master->controllers[i].rank = i + 1;
    master->controllers[i].sflag = 1;
    master->controllers[i].wu_data = malloc(master->shmSz * sizeof(char));
    if (!master->controllers[i].wu_data) {
      ERROR("failed to allocate work unit for controller.");
      return NULL;
    }
  }

  // Allocate array of controller request-types
  // Array is of the form: [ (req=0,req=1,req=2), ... ]
  nreqs = ncontrollers * 3;
  master->mpi_req = malloc(nreqs * sizeof(MPI_Request));
  if (!master->mpi_req) {
    ERROR("failed to allocate room for controller requests.");
    return NULL;
  }

  // Initialize slave requests objects
  for (i = 0; i < nreqs; ++i)
    master->mpi_req[i] = MPI_REQUEST_NULL;

  return master;
}
  

// Master reader
void *
master_reader (void *arg)
{
  // Loop until no more data is available
  while (1) {
    // Wait until command is ready signal while bytes read is 0
    pthread_mutex_lock(&reader.lock);
    while (reader.read_cnt == 0) {
      pthread_cond_wait(&reader.cmd_ready, &reader.lock);
    }
 
    // Get the number of bytes read
    pthread_mutex_unlock(&reader.lock);

    // Invalidate the number of bytes read to exit loop 
    if (reader.read_cnt == -1) {
      break;
    }

    // Signal that data is ready
    pthread_mutex_lock(&reader.lock);
    pthread_cond_signal(&reader.data_ready);
    pthread_mutex_unlock(&reader.lock);
  }

  return NULL;
}


// Master broadcast files to controllers. Used to distribute binaries and shared database files.
ssize_t
master_broadcast_file (const char *path, size_t bcast_chunk_size)
{
  struct stat st;
  size_t sz;
  int fd;
  int rc;
  void *file;

  // Open file
  if ((fd = open(path, O_RDONLY)) == -1) {
    ERROR("could not open work file.");
    return -1; 
  }

  // Get file size
  if (fstat(fd, &st) < 0) {
    ERROR("could not stat file.");
    close(fd);
    return -1; 
  }
  sz  = st.st_size;

  // Advise kernel on file usage
  rc = posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL | POSIX_FADV_WILLNEED);
  if (rc != 0)
    WARN("failed file advise to kernel.");

  // Send file size to other processes
  rc = MPI_Bcast(&sz, sizeof(sz), MPI_BYTE, MASTER, MPI_COMM_WORLD);
  if (rc != MPI_SUCCESS) {
    ERROR("failed to broadcast file size.");
    return -1;
  }

  // Memory map the file
  file = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_POPULATE, fd, 0);
  if (file == MAP_FAILED) {
    ERROR("failed to map file.");
    close(fd);
    return -1;
  }

  // Advise kernel on memory usage
  rc = posix_madvise(file, sz, MADV_SEQUENTIAL | MADV_WILLNEED);
  if (rc != 0)
    WARN("failed memory advise to kernel.");

  // Send file to other processes
  trace("Master sending file (%s) of size %zu", strip_path(path), sz);
  rc = chunked_bcast(file, sz, bcast_chunk_size, MASTER, MPI_COMM_WORLD);
  if (rc != MPI_SUCCESS) {
    ERROR("failed chunk broadcast of file.");
    munmap(file, sz);
    close(fd);
    return -1;
  }
  trace("Master sent file (%s) of size %zu", strip_path(path), sz);

  // Unmap memory and close file
  munmap(file, sz);
  close(fd);

  return sz;
} 


// Master main function
int
master_main (struct process_pool_ctl *pool_ctl, struct master_ctx *ctx)
{
  struct controller_info *s = NULL;          // Array of slave information structure
  char in_path[MAX_FILE_PATH];                 // Input query file
  char *in_tmp = NULL;
  char *in_dir = NULL;
  int in_fd;                     // File descriptor of input query file
  struct stat in_st;             // Stat structure for input query file
  char *in_data = NULL;                 // Memory map of input query file
  size_t in_size;                // Size of input query file
  unsigned in_cnt;               // Number of sequences in input query file
  char *in_s = NULL;                    // Start location of query sequence
  char *in_e = NULL;                    // End location of query sequence
  int max_nseqs;                 // Maximum number of sequences per slave
  int wu_nseqs;                  // Number of sequences in work unit
  int controller_idx;                 // Slave index
  int seq_idx;                   // Sequence index
  int rc;                        // Function return codes
  int i;                         // Iteration variable
  int req_idx;                   // Index of slave request
  int req_type;                  // Type of slave request
  int nreqs;                     // Number of slave requests
  int nrunning;                  // Number of slaves running
  int outstandings[ctx->ncontrollers]; // Array of slave outstandings
  int outstanding;               // Total slave outstandings
  int percent;                   // Percent of workload distributed
  int last_percent;              // Previous percent of workload distributed
  int *idxs = NULL;                     // Array of indexes for slave requests
  int ncontrollers;

  minfo("Call to master_main()");

  ncontrollers = ctx->ncontrollers;
 
  // Initialize outstanding variables
  for (i = 0; i < ncontrollers; ++i) {
    outstandings[i] = 0;
  }
  outstanding = 0;

  // Create array of indexes for slave requests, 3 req/slave (request, info, data)
  nreqs = ncontrollers * 3;
  idxs = malloc(nreqs * sizeof(int));
  if (!idxs) {
    ERROR("failed to allocate space for controller request indexes.");
    return EXIT_FAILURE;
  }

  // Get input directory
  in_dir = getenv("HSP_INPDIR");
  if (!in_dir || (int)(*in_dir) == 0) {
    WARN("could not find input directory in environment, using default.");
    in_dir = HSP_INPDIR;
  }

  // Load query file
  // TODO: break "load, and map" code into a function
  in_tmp = getenv("HSP_INFILES");
  if (!in_tmp || (int)(*in_tmp) == 0) {
    WARN("could not find input files in environment.");
    in_tmp = NULL;
  }
  else {
    snprintf(in_path, sizeof(in_path), "%s/%s", in_dir, in_tmp); 
  }

  if ((in_fd = open(in_path, O_RDONLY)) == -1) {
    ERROR("master: %s: Could not open file: %s", in_path, strerror(errno));
    return EXIT_FAILURE;
  }
  minfo("Input file: %s", in_path);

  // Get size of input query file
  if (fstat(in_fd, &in_st) < 0) {
    close(in_fd);
    ERROR("master: %s: Could not stat file: %s", in_path, strerror(errno));
    return EXIT_FAILURE;
  }
  in_size = in_st.st_size;

  // Advise kernel on file usage
  rc = posix_fadvise(in_fd, 0, in_size, POSIX_FADV_SEQUENTIAL | POSIX_FADV_WILLNEED);
  if (rc != 0)
    WARN("failed file advise to kernel.");

  // Memory map the input query file
  in_data = mmap(NULL, in_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, in_fd, 0);
  if (in_data == MAP_FAILED) {
    close(in_fd);
    ERROR("master: %s: Could not mmap file: %s", in_path, strerror(errno));
    return EXIT_FAILURE;
  }

  // Advise kernel on memory usage
  rc = posix_madvise(in_data, in_size, MADV_SEQUENTIAL | MADV_WILLNEED);
  if (rc != 0)
    WARN("failed memory advise to kernel.");

  // Count number of input sequences
  in_cnt=1;
  in_s=in_data;
  while((in_e = iter_next(ctx->input_fmt, in_data, in_data+in_size, in_s)) < in_data+in_size) {
    in_cnt++;
    in_s = in_e;
  }
  minfo("Counted %u inputs.", in_cnt);

  // Uniform distribution of sequences among controllers
  max_nseqs = in_cnt / ncontrollers;

  // Default number of sequences in a work unit
  wu_nseqs  = 1;


  // Set number slaves and working directory in process pool control structure
  minfo("Master sends run signal to process pool.");
  process_pool_spawn(pool_ctl, ".", -1);

  minfo("Issuing jobs...");
  
  // Post a receive for each controller information structure, track outstanding messages
  for (controller_idx = 0; controller_idx < ncontrollers; ++controller_idx) {
    rc = MPI_Irecv(&ctx->controllers[controller_idx].request, sizeof(struct request), MPI_BYTE,
                   ctx->controllers[controller_idx].rank, TAG_REQUEST, MPI_COMM_WORLD,
                   &ctx->mpi_req[controller_idx]);
    outstandings[controller_idx]++;
    outstanding++;
  }

  // Initial values to keep track of input data, slaves, and workload percent 
  in_s = in_data;
  seq_idx = 0;
  nrunning = ncontrollers;
  percent = last_percent = 0;
  
  // Iterate through all the sequences in input query file
  // Only run if all slave processes are running
  while (seq_idx < in_cnt && nrunning == ncontrollers) {

    // Check that max number of sequences conforms if fewer are left 
    if (max_nseqs > in_cnt-seq_idx) {
      max_nseqs = in_cnt-seq_idx;
    }

    // Wait until any slave request completes from previous MPI_Irecv call
    // Obtain a copy of the slaves information structure
    MPI_Waitany(nreqs, ctx->mpi_req, &req_idx, MPI_STATUSES_IGNORE);

      // Determine request-type associated with this request
      req_type  = req_idx / ncontrollers;

      // Determine the slave index of the request
      controller_idx = req_idx % ncontrollers;

      // Remove from outstanding trackers 
      outstandings[controller_idx]--;
      outstanding--;

      // Set temporary pointer to slave information structure for compactness
      s = &ctx->controllers[controller_idx];

      trace("MPI-Request completed (slave: %d, type: %d flag: %x -> ", controller_idx+1, req_type, s->sflag);

      // Set slave send flag based on request-type
      switch (req_type) {
      case 0:
        s->sflag &= ~1; // Blocks bit 1 
        break;
      case 1:
        s->sflag &= ~2; // Blocks bit 2
        break;
      case 2:
        s->sflag &= ~4; // Blocks bit 4
        break;
      default:
        // Programming error
        assert(0);
        break;
      }

      trace("%x)\n", s->sflag);

      // Slave send flag is 0, then slave is idle so we need to hand out work units 
      if (!s->sflag) {

        // Check slaves request type and service it
        switch (s->request.type) {
        
        // Hand out work units
        case REQ_WORKUNIT:

          // Figure out how many sequences to send in a work unit (fair)
          wu_nseqs = s->request.count;

          // Check that the requested work units are available
          if (wu_nseqs > max_nseqs) {
            info("Controller %d requested %d units, limiting to %d.", controller_idx+1, wu_nseqs, max_nseqs); 
            wu_nseqs = max_nseqs;
          } else {
            trace("Controller %d requested %d units.", controller_idx+1, wu_nseqs);
          }

          // Determine size of all the data we need by setting pointers at beginning and end of work unit
          for (in_e=in_s, i=wu_nseqs; i; --i) {
            in_e = iter_next(ctx->input_fmt, in_data, in_data+in_size, in_e);
          }

          // Prepare work unit for data: size, type, index, number of sequences
          s->workunit.len    = in_e - in_s;
          s->workunit.type   = WU_TYPE_DATA;
          s->workunit.blk_id = seq_idx;
          s->workunit.count  = wu_nseqs;

          // Copy data to workunit structure
          memcpy(s->wu_data, in_s, s->workunit.len);

          // Advance our data pointer iterator
          in_s = in_e;

          // Send work unit metadata to slave
          rc = MPI_Send(&s->workunit, sizeof(struct workunit), MPI_BYTE, s->rank,
              TAG_WORKUNIT, MPI_COMM_WORLD);

          // Send work unit data 
          rc = MPI_Send(s->wu_data, s->workunit.len, MPI_BYTE, s->rank, TAG_DATA, MPI_COMM_WORLD);

          trace("Controller %d send data (%d bytes, rank %d): %d", controller_idx+1, s->workunit.len, s->rank, rc);

          // Re-post a receive for this slave next request, update outstanding trackers 
          rc = MPI_Irecv(&s->request, sizeof(struct request), MPI_BYTE, s->rank,
              TAG_REQUEST, MPI_COMM_WORLD, &ctx->mpi_req[controller_idx]);
          outstandings[controller_idx]++;
          outstanding++;

          // Record that we need to wait for sends and a new request before doing any action
          s->sflag = 1;

          // Advance the sequence counter
          seq_idx += wu_nseqs;

          break;

	// A client is aborting, take down the whole system (for now)
	case REQ_ABORT:
          ERROR("master: Controller %d aborted. Exiting.", s->rank);

          // Flag that a slave aborted
	  nrunning--;
	  break;

        // Unknown request
        default:
          // Kill master process 
          WARN("master: unknown request type from rank %d. Exiting.", s->rank);
          exit(EXIT_FAILURE);
        }
      }
     
    // Compute the percent of sequences already sent to slaves 
    percent = 100 * seq_idx / in_cnt;
    if (percent != last_percent) {
      info("%d%% percent complete.", percent);
      last_percent = percent;
    }

  } // End service loop

  info("Done issuing jobs.");

  // Wait for all slaves to exit (maybe can be done slave-by-slave)
  for (; nrunning; nrunning--) {
    trace("Waiting for %d controllers to exit.", nrunning);

    // Wait for an slave abort request
    MPI_Waitany(nreqs, ctx->mpi_req, &req_idx, MPI_STATUSES_IGNORE);

    // Determine the slave and request-type associated with this request
    req_type  = req_idx / ncontrollers;
    controller_idx = req_idx % ncontrollers;

    // Set temporary pointer to slave information structure for compactness
    s = &ctx->controllers[controller_idx];

    // Update outstanding trackers
    outstandings[controller_idx]--;
    outstanding--;

    // Check request-type and service it
    switch (s->request.type) {
    case REQ_WORKUNIT:

      // Set exit work unit metadata for slave
      trace("Terminating controller %d.", controller_idx);
      s->workunit.type   = WU_TYPE_EXIT;
      s->workunit.len    = 0;
      s->workunit.blk_id = 0;

      // Send work unit to slave 
      MPI_Send(&s->workunit, sizeof(struct workunit), MPI_BYTE, s->rank,
               TAG_WORKUNIT, MPI_COMM_WORLD);
      trace("Controller terminated %d.", controller_idx);
      break;

     
    // Another slave aborted, no need to do anything
    case REQ_ABORT:
      trace("Controller %d aborted during shutdown.", s->rank);
      break;

    // Unknown request, exit master
    default:
      trace("Unknown request type from rank %d. Exiting.", s->rank);
      exit(EXIT_FAILURE);
    }
  }

  info("All jobs complete, proceeding with shutdown.");

  // Synchronize with controller processes
  MPI_Barrier(MPI_COMM_WORLD);

  return EXIT_SUCCESS;
}

