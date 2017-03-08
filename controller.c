/**
 * \file controller.c
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Contains functions for slave processes.
 *
 * Contains initialization, broadcast of files, and main functions for slave processes.
 */


#include "controller.h"


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
static void process_pool_spawn (struct process_pool_ctl *pool_ctl, const char *workdir, int nprocs)
{
  // Set the number of slave/worker processes
  pool_ctl->nprocesses = nprocs;

  // Set the working directory
  strncpy(pool_ctl->workdir, workdir, PATH_MAX);

  // Signal the process pool control is ready to run
  pthread_cond_signal(&pool_ctl->run);
}


/**
 * \brief Slave process requests work units from master process and stores them into a linked list of cache buffers.
 *
 * Counts all the sequences found in linked list of cache buffers to check if more work units should be requested. Requests a work unit from master process (sends request, receives work unit metadata, receives work unit data). Add a cache buffer to the linked list and add the work unit.
 *
 * \param[in] queue Linked list of cache buffers
 * \param[in] nprocesses Number of worker processes 
 * \retval int Number of sequences in received work unit
 * \retval  0 Do not request more work units
 * \retval -1 Master process sent exit message, terminate
 *
 * \todo Consider better prefetching policy. We are always prefetching if we can, but we probably want to let requests add up, and request a bundle.
 */
static int  request_work (struct cache_buffer **queue, int nprocesses);


/**
 * \brief Send a request with abort message to master process.
 *
 * \retval int Return code from MPI send of request object
 */
static int  request_abort ();


/**
 * \brief Searches the virtual file table for the virtual file corresponding to the worker index. The data passed is copied into the virtual file. 
 *
 * \param[in] ft Virtual file table 
 * \param[in] wid Index of worker process
 * \param[in] data Data to copy into virtual file
 * \param[in] len Length of data in bytes
 * \retval None
 */
static void push_work (struct file_table *ft, wid_t wid, const char *data, size_t len);


/**
 * \brief Finds a virtual file that matches the worker index and writer context file name. Writer is called to write the results to output file.
 *
 * "Pull" results from a worker's output buffers and send to the writer.
 * This function must be called with a lock on the shared process-control
 * structure since it modifies a worker's buffers (although the worker should
 * be halted at this point).
 *
 * \param[inout] controllers Pointer to controller context structure 
 * \param[in] wid Index of worker process
 * \retval None
 */
static void pull_worker_results (struct controller_ctx *controllers, wid_t wid);


/**
 * \brief Parse the string of file names and add each file to the virtual file table for all worker processes.
 *
 * \param[in] files String with file names to copy into streams
 * \param[in] ps_ctl Process control structure 
 * \param[in] size File size
 * \param[in] type Type of file
 * \retval * Pointer to a file set
 * \retval NULL An error occurred 
 *
 * \todo The user is responsible of freeing names 2D array.
 * \todo Files are created in shared memory with a size of HSP_SHM_SZ. What happens if input or output files are larger than that?
 */
static struct fileSet * create_stream_files (char *files, struct process_control *ps_ctl, size_t size, enum file_type type);


// Initialization of controller processes.
struct controller_ctx *
controller_init (int rank, int nworkers, char input_fmt)
{
  int rc;                  // Functions return code
  int len;                 // Length of working directory
  char *files = NULL;      // Temporary input and output files
  struct controller_ctx *controllers = NULL;  // Controller control structure
  char *outdir = NULL;     // Output directory
  char *shmSz = NULL;

  info("Call to controller_init()");

  // Allocate space for controller context structure
  controllers = malloc(sizeof(struct controller_ctx));
  if (!controllers) {
    ERROR("could not allocate space for controller context.");
    return NULL;
  }

  // Initialize controller context numeric/char members
  controllers->input_fmt = input_fmt;
  controllers->rank = rank;
  controllers->nworkers = nworkers;
  info("Initializing controller process (%d workers)", nworkers);

  // Get size of shared memory buffers from environment
  shmSz = getenv("HSP_SHM_SZ");
  if (!shmSz || (int)(*shmSz) == 0) {
    WARN("could not find shared memory size in environment, using default.");
    shmSz = NULL;
    controllers->shmSz = HSP_SHM_SZ; 
  }
  else
    controllers->shmSz = atoi(shmSz);
  info("Shared memory buffer size, %d", controllers->shmSz);

  // Get output directory from environment variable
  outdir = getenv("HSP_OUTDIR");
  if (!outdir || (int)(*outdir) == 0) {
    WARN("could not find output directory in environment, using default.");
    outdir = HSP_OUTDIR;
  }
  info("Creating output directory, %s", outdir);

  // Allocate space for full output directory name
  // 2 slashes, 2 char dir, 10 char rank, 1 NULL
  len = strlen(outdir) + 15;
  controllers->outdir = malloc(len * sizeof(*controllers->outdir));
  if (!controllers->outdir) {
    ERROR("could not allocate space for full output directory.");
    return NULL;
  }

  // Create full output directory string
  rc = snprintf(controllers->outdir, len, "%s/%02d/%02d", outdir, controllers->rank/100, controllers->rank%100);
  if (rc < 0) {
    ERROR("could not create output directory name.");
    return NULL;
  }

  // Create output directory
  rc = mkpath(controllers->outdir, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
  if (rc == EXIT_FAILURE) {
    ERROR("could not create output directory.");
    return NULL;
  }

  // Change to present output directory
  rc = chdir(controllers->outdir);
  if (rc == -1) {
    ERROR("could not change to output directory.");
    return NULL;
  }

  // Initialize shared process control structure
  controllers->ps_ctl = ps_ctl_init(controllers->nworkers, NULL);
  if (controllers->ps_ctl == NULL) {
    ERROR("could not initialize process control structure.");
    return NULL;
  }

  // Get input files from environment variable
  files = getenv("HSP_INFILES");
  if (!files || (int)(*files) == 0) {
    WARN("could not find input files in environment.");
    files = NULL;
    controllers->ifiles = NULL;
  }
  else {
    // Create input file mappings
    info("Creating input stream files.");
    controllers->ifiles = create_stream_files(files, controllers->ps_ctl, controllers->shmSz, FTE_INPUT);
    if (!controllers->ifiles) {
      ERROR("could not create stream of input files.");
      return NULL;
    }
  }

  // Get output file from environment variable
  files = getenv("HSP_OUTFILES");
  if (!files || (int)(*files) == 0) {
    WARN("could not find output files in environment.");
    files = NULL;
    controllers->ofiles = NULL;
  }
  else {
    // Create output files mappings
    info("Creating output stream files.");
    controllers->ofiles = create_stream_files(files, controllers->ps_ctl, controllers->shmSz, FTE_OUTPUT);
    if (!controllers->ofiles) {
      ERROR("could not create stream of input files.");
      return NULL;
    }
  }

  // Initialize remaining controller context members
  controllers->writers = NULL;
  

  return controllers;
}


// Controller processes receive shared database files from master process.
ssize_t
controller_broadcast_shared_file(struct process_control *ps_ctl, const char *path, size_t bcast_chunk_size)
{
  void  *shm = NULL;   // Pointer to shared memory of file
  size_t sz;    // Size of file
  int rc;       // Functions return code

  // Get file size from master process
  rc = MPI_Bcast(&sz, sizeof(sz), MPI_BYTE, MASTER, MPI_COMM_WORLD);
  if (rc != MPI_SUCCESS) {
    ERROR("failed to broadcast chunks of files.");
    return -1;
  }

  // Get shared memory added to process control structure virtual file table
  shm = ps_ctl_add_file(ps_ctl, -1, path, sz, FTE_SHARED);
  if (!shm) {
    ERROR("failed to add file to virtual table.");
    return -1;
  }

  // Receive shared file data and copy into shared memory of virtual file table
  trace("Controller receiving shared file (%s) of size %zu", strip_path(path), sz);
  rc = chunked_bcast(shm, sz, bcast_chunk_size, MASTER, MPI_COMM_WORLD);
  if (rc != MPI_SUCCESS) {
    ERROR("failed chunk broadcast shared file.");
    return -1; 
  }
  trace("Controller received shared file (%s) of size %zu", strip_path(path), sz);
  
  return sz;
} 


// Controller processes receive the program binary file from the master process.
ssize_t
controller_broadcast_work_file(const char *path, size_t bcast_chunk_size)
{
  void   *file;     // Pointer to mapped memory
  size_t  sz;       // Size of file
  int     fd;       // File descriptor
  int     rc;       // Functions return code

  // Get file size from master process
  rc = MPI_Bcast(&sz, sizeof(sz), MPI_BYTE, MASTER, MPI_COMM_WORLD);
  if (rc != MPI_SUCCESS) {
    ERROR("failed to broadcast file size.");
    return -1;
  }

  // Create file
  fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  if (fd == -1) {
    remove(path);
    fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    if (fd == -1) {
      ERROR("failed to create work file.");
      return -1;
    }
  }

  // Set size of file
  rc = ftruncate(fd, sz);
  if (rc == -1) {
    ERROR("failed to resize work file.");
    close(fd);
    return -1; 
  }

  // Advise kernel on file usage
  rc = posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL | POSIX_FADV_WILLNEED);
  if (rc != 0)
    WARN("failed file advise to kernel.");

  // Memory map the file 
  file = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, 0);
  if (file == MAP_FAILED) {
    ERROR("failed to mmap file.");
    close(fd);
    return -1; 
  }

  // Advise kernel on memory usage
  rc = posix_madvise(file, sz, MADV_SEQUENTIAL | MADV_WILLNEED);
  if (rc != 0)
    WARN("failed memory advise to kernel.");

  // Get file data from master process
  trace("Controller receiving work file (%s) of size %zu", path, sz);
  rc = chunked_bcast(file, sz, bcast_chunk_size, MASTER, MPI_COMM_WORLD);
  if (rc != MPI_SUCCESS) {
    ERROR("failed chunk broadcast work file.");
    munmap(file, sz);
    close(fd);
    return -1; 
  }
  trace("Controller received work file (%s) of size %zu", path, sz);

  // Unmap memory and close file
  munmap(file, sz);
  close(fd);

  return sz; 
}


// Main function for controller processes
int
controller_main (struct process_pool_ctl *pool_ctl, struct controller_ctx *controllers)
{
  struct cache_buffer *queue = NULL;   // Linked list of cache buffers
  struct timeval tv[2];         // Timing structure
  int no_work;                  // No work flag
  int i;                        // Iteration variable
  wid_t wid;                    // Index of worker process
  unsigned worker_iterations[controllers->nworkers];  // Tracks task iterations of worker processes
  struct process_control *ps_ctl = controllers->ps_ctl;  // Process control structure
  struct writer_ctx *writers = NULL;     // Writer context structure
  int rc;                     // Function return code
  int noutfiles;
  int ninfiles;
  int nprocesses;

  info("Call to controller_main()");

  printVirtualFilesTable(&ps_ctl->ft);

  nprocesses = ps_ctl->nprocesses;
  noutfiles = controllers->ofiles->nfiles;
  ninfiles = controllers->ifiles->nfiles;
  (void)ninfiles;

  // Allocate one writer per output file
  writers = malloc(noutfiles * sizeof(struct writer_ctx));
  if (!writers) {
    ERROR("failed to allocate writers context.");
    return EXIT_FAILURE;
  }

  // Set writers context to controller context
  controllers->writers = writers;

  // Spawn and initialize writer threads
  for (i = 0; i < noutfiles; ++i) {
    rc = writer_init(&writers[i], controllers->ofiles->fn[i], (controllers->shmSz * nprocesses), nprocesses);
    if (rc == EXIT_FAILURE) {
      ERROR("failed to start writer.");
      free(writers);
      return EXIT_FAILURE;
    }
  }

  // Iterate through worker processes
  for (wid = 0; wid < nprocesses; ++wid) {
    // Initialize input virtual files with empty data
    push_work(&ps_ctl->ft, wid, "", 0);

    // Set worker processes state as running 
    ps_ctl->process_state[wid] = RUNNING;
    ps_ctl->process_cmd[wid]   = RUN;
  }

  // Process control is setup, data is in place, spawn the worker processes
  info("Controller sends run signal to process pool.");
  process_pool_spawn(pool_ctl, controllers->outdir, nprocesses);

  // Initialize tracker for worker task iterations
  memset(worker_iterations, 0, sizeof(worker_iterations));

  // Get timestamp
  gettimeofday(tv+0, NULL);

  // Initialize flag
  no_work = 0;

  // Loop to service worker processes
  while (1) {

    // Lock shared process control structure
    pthread_mutex_lock(&ps_ctl->lock);
    
    // Check if all worker processes are done, if so exit loop
    if (ps_ctl_all_done(ps_ctl)) {
      trace("All processes done! exit loop.");
      pthread_mutex_unlock(&ps_ctl->lock);
      break;
    }

    // Wait for a worker process to need service
    while (ps_ctl_all_running(ps_ctl)) {
      trace("Waiting for customer...");
      pthread_cond_wait(&ps_ctl->need_service, &ps_ctl->lock);
    }
    
    // Print report with state of worker processes
/*#ifdef TRACE
    char *report=malloc(nprocesses+1);
    for (wid = 0; wid < nprocesses; ++wid) {
      switch (ps_ctl->process_state[wid]) {
      case EOD:     report[wid] = 'E'; break;
      case NOSPACE: report[wid] = 'N'; break;
      case FAILED:  report[wid] = 'F'; break;
      case DONE:    report[wid] = 'D'; break;
      case IDLE:    report[wid] = 'I'; break;
      case RUNNING: report[wid] = 'R'; break;
      default:      report[wid] = ' '; break;
      }
    }
    report[wid] = '\0';
    trace("Worker statuses = [%s]\n", report);
    free(report);
#endif // TRACE
*/
    // Service worker processes
    for (wid = 0; wid < nprocesses; ++wid) {

      // Check state of worker process
      switch (ps_ctl->process_state[wid]) {

      // End of data
      case EOD:
        // If work is left and no data available in cache buffers queue, then request work units to master.
        // Front buffer empty implies whole queue is empty
        if (!no_work && queue == NULL) {
          // No data in the buffer, must request now
          no_work = (request_work(&queue, nprocesses) == -1);
        }

        // Preemptively flush the results while the process is stopped
        // FIXME: Don't do this if this is the initial EOD (first request for data)
        pull_worker_results(controllers, wid);
 
        // If work unit was stored in queue successfully
        if (queue != NULL) {

          // Get a sequence from the queue data
          char *end  = iter_next(controllers->input_fmt, queue->data, queue->data+queue->size, queue->r_ptr);

          // Get size of query sequence string
          off_t len  = end - queue->r_ptr;

          // Copy data into virtual table
          push_work(&ps_ctl->ft, wid, queue->r_ptr, len);

          // Increment task iterations
          worker_iterations[wid]++;

          // Set state of worker process to running
          ps_ctl->process_cmd[wid] = RUN;
          ps_ctl->process_state[wid] = RUNNING;

          // Signal that worker process is ready
          pthread_cond_signal(&ps_ctl->process_ready[wid]);
          trace("Sent new data to worker %d", wid);

          // Advance the data iterator in current cache buffer of queue 
          queue->len -= len;
          queue->count--;
          queue->r_ptr += len;

          // If no more data left in current cache buffer, advance to next buffer
          if (queue->count == 0) {
            // Check that length of valid data should zero
            assert(queue->len == 0);

            // Get next cache buffer
            struct cache_buffer *n = queue->next;
            free(queue);
            queue = n;
          }

        // No more data left, tell worker process to quit
        } else if (no_work) {
          trace("Requesting worker %d to quit.", wid);

          // Set the process command to quit and signal that worker is ready
          ps_ctl->process_cmd[wid] = QUIT;
          ps_ctl->process_state[wid] = RUNNING;
          pthread_cond_signal(&ps_ctl->process_ready[wid]);
        }
        break;

      // Worker process does not have enough space for more data
      case NOSPACE:
        // We always handle NOSPACE, even if there is no work left
        // FIXME: This will result in fragmentation of the output data
        // we need to maintain an index so output can be pieced back in
        // proper order by the 'gather' script

        // Get results from worker and pass to writer for output
        pull_worker_results(controllers, wid);
        info("Buffer almost overflowed, result data is probably interleaved.");

        // Set worker process to the running state
        ps_ctl->process_cmd[wid] = RUN;
        ps_ctl->process_state[wid] = RUNNING;

        // Signal that worker process is ready
        pthread_cond_signal(&ps_ctl->process_ready[wid]);
        break;

      // If worker process failed, do not do anything 
      case FAILED:
        ERROR("!!!!!! PROCESS FAILED !!!!!!");
        break;

      // If worker process terminated, do not do anything
      case DONE:
        trace("PROCESS DONE");
        break;

      // If worker is idle, do not do anything
      // If worker is running, do not do anything
      case IDLE:
      case RUNNING:
        break;
      }
    }

    // Done modifying process states, unlock shared process control structure
    pthread_mutex_unlock(&ps_ctl->lock);

    // If work is left, prefetch data if needed
    if (!no_work) {
      no_work = (request_work(&queue, nprocesses) == -1);
    }
  }

  trace("Loop is exited.");

  // The loop exited, but master didn't tell us to. Post an abort request.
  if (!no_work) {
    request_abort();
  }

  // Synchronize master and slave processes
  MPI_Barrier(MPI_COMM_WORLD);

  // Get timestamp
  gettimeofday(tv+1, NULL);

  // Flush last bit of output data from all worker processes
  pthread_mutex_lock(&ps_ctl->lock);
  for (wid = 0; wid < nprocesses; ++wid) {
    pull_worker_results(controllers, wid);
  }
  pthread_mutex_unlock(&ps_ctl->lock);

  // Signal all writers that data is pending and to exit main loop
  for (i = 0; i < noutfiles; ++i) {
    writers[i].running = 0;
    pthread_cond_signal(&writers[i].data_pending);
  }
  
  // Join with writer threads
  for (i = 0; i < noutfiles; ++i) {
    pthread_join(writers[i].thread, NULL);
    trace("Writer thread %d successfully exited.", i);
  }

  // Compute time passed between timestamps
  long t = (tv[1].tv_sec - tv[0].tv_sec) * 1000000
           + (tv[1].tv_usec - tv[0].tv_usec);

  // Write out the number of task iterations per worker process
  putchar('\n');
  for (wid = 0; wid < nprocesses; ++wid) {
    info("Worker %2u: iterations: %5u", wid, worker_iterations[wid]);
  }

  info("Time taken: %lfs", ((double)t) / 1000000.0);

  return EXIT_SUCCESS;
}


// Slave requests work units from master process and stores them into a linked list of cache buffers.
static int
request_work (struct cache_buffer **queue, int nprocesses)
{
  struct cache_buffer *b;     // Cache buffer for work unit requested
  struct cache_buffer *tail;  // Tail cache buffer of linked list
  struct request  req;        // Request object sent to master process
  struct workunit wu;         // Work unit metadata structure
  int cnt;                    // Number of sequences in the linked list of cache buffers
  int rc;                     // Functions return code

  // Count total number of data from cache buffers in queue 
  for (cnt = 0, tail = NULL, b = *queue; b; b = b->next) {
    cnt += b->count;
    tail = b;
  }

  // If total number of data already in buffer is twice the workers, then we do not fetch more data right now.
  if (cnt > nprocesses/2) {
    return 0;
  }

  // More data will be requested from master process
  req.type  = REQ_WORKUNIT;

  // Request enough data to have 1 per worker process
  req.count = nprocesses - cnt;

  // Send work unit request to master process 
  trace("Requesting work...");
  rc = MPI_Send(&req, sizeof(struct request), MPI_BYTE, 0, TAG_REQUEST, MPI_COMM_WORLD);
  trace("Sent request %d", rc);

  // Receive  work unit metadata from master process
  trace("Receiving work unit...");
  rc = MPI_Recv(&wu, sizeof(struct workunit), MPI_BYTE, 0,
           TAG_WORKUNIT, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);

  trace("Received work unit (type: %d, size: %d) %d", wu.type, wu.len, rc);

  // Check type of work unit received to decide the action to perform
  switch (wu.type) {

  // If master sent exit message, then return (do not request more work)
  case WU_TYPE_EXIT:
    return -1;

  // If master is sending work unit data, prepare to receive it
  case WU_TYPE_DATA:

    // Allocate a cache buffer with space for the work unit data to receive
    b = malloc(sizeof(struct cache_buffer) + wu.len);

    // Set cache buffer attributes (data pointer, buffer size, number of sequences)
    b->r_ptr = b->data;
    b->size  = wu.len;
    b->count = wu.count;
    b->len   = wu.len;
    b->next  = NULL;

    // Receive work unit data from master process
    trace("Receiving work unit data...");
    rc = MPI_Recv(b->data, wu.len, MPI_BYTE, 0, TAG_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    trace("Received work unit data %d", rc);

    // If linked list has cache buffers, add to end of queue
    if (tail) {
      tail->next = b;
    // If linked list is empty, then this is the only cache buffer in queue
    } else {
      *queue = b;
    }

    // Return the number of sequences received in work unit from master process
    return b->count;

  // Unknown work unit type from master process, terminate execution 
  default:
    WARN("Controller: unknown work unit type from master. Exiting.");
    exit(EXIT_FAILURE);
  }

  (void)rc;
}


// Send a request with abort message to master process.
static int
request_abort ()
{
  struct request req;  // Request object
  int rc;              // Functions return code

  // Set an abort request
  req.type  = REQ_ABORT;
  req.count = 0;

  // Send request abort to master process
  trace("Sending abort notification...");
  rc = MPI_Send(&req, sizeof(struct request), MPI_BYTE, 0, TAG_REQUEST, MPI_COMM_WORLD);
  trace("Sent notification %d", rc);

  return rc;
}


// Searches the virtual file table for the virtual file corresponding to the worker index. The data passed is copied into the virtual file. 
static void
push_work (struct file_table *ft, wid_t wid, const char *data, size_t len)
{
  int i;                       // Iteration variable
  struct file_table_entry *f;  // Virtual file structure

  // Iterate through all the virtual files
  for (i = 0; i < ft->nfiles; ++i) {
    // Get a virtual file
    f = &ft->file[i];

    // Check ownership of virtual file and if its an input file
    if (f->wid == wid && f->type == FTE_INPUT) {
      // Copy data into shared memory
      memcpy(f->shm, data, len);

      // Set size of virtual file
      f->size = len;
    }
  }
}


// Finds a virtual file that matches the worker index and writer context file name. Writer is called to write the results to output file.
static void
pull_worker_results (struct controller_ctx *controllers, wid_t wid)
{
  int i;     // Iteration variable
  int j;     // Iteration variable
  int noutfiles;
  struct file_table_entry *f = NULL;   // Virtual file
  struct file_table *ft = NULL;        // Virtual file table
  struct writer_ctx *writers = NULL;       // Writer context structure

  ft = &controllers->ps_ctl->ft;        // Virtual file table
  writers = controllers->writers;       // Writer context structure
  noutfiles = controllers->ofiles->nfiles;
        
  // Iterate through all the virtual files
  for (i = 0; i < ft->nfiles; ++i) {
    // Get a virtual file
    f = &ft->file[i];

    // Check ownership of virtual file and if its an output file
    if (f->wid == wid && f->type == FTE_OUTPUT) {

      // Iterate through all the writers
      for (j = 0; j < noutfiles; ++j) {

        // Find writer with output file name that matches the virtual file name
        if (!strcmp(f->name, writers[j].name)) {

          // Write it out, it is an output file that belongs to us
          trace("worker %d: Writing %d/%d: '%s' vs. '%s' (size %zu)", wid, i, ft->nfiles, f->name, writers[j].name, f->size);	
          writer_write(&writers[j], wid, f->shm, f->size);

          // Data was retrieved so mark virtual file as empty
          f->size = 0;
        } else {
          info("there\n");
          trace("worker %d: Considering writing %d/%d: '%s' vs. '%s' (size %zu)",
                 wid, i, ft->nfiles, f->name, writers[j].name, f->size);	
	}
      }
    }
  }
}


// Parse the string of file names and add each file to the virtual file table for all worker processes.
static struct fileSet *
create_stream_files (char *files, struct process_control *ps_ctl, size_t size, enum file_type type) {
  int i;         // Iteration variable
  int wid;       // Worker iteration variable
  int nfiles;    // The number of files 
  char *lfiles;  // Copy of input files string
  char *f;       // Parsed file names
  char *p;       // Pointer character iterator
  struct fileSet *fs;  // File set

  // Allocate space for the file set
  fs = malloc(sizeof(struct fileSet));
  if (!fs) {
    ERROR("could not allocate space for file set.");
    return NULL;
  }
  
  // Iterate through the string of file names, count the number of files
  for (nfiles = 0, p = files; *p; ++p)
    if (*p == ':')
      ++nfiles;
  ++nfiles;

  fs->nfiles = nfiles;
  info("Count %d files.", nfiles);

  // Allocate space for file names in set
  fs->fn = malloc(nfiles * sizeof(char *));
  if (!fs->fn) {
    ERROR("could not allocate space for file names in file set.");
    return NULL;
  }
  for (i = 0; i < nfiles; ++i) {
    fs->fn[i] = malloc(MAX_FILE_PATH * sizeof(char));
    if (!fs->fn[i]) {
      ERROR("could not allocate space for file names #%d.", i);
      return NULL;
    }
  }
  
  // Duplicate string of files
  lfiles = strdup(files);
  if (!lfiles) {
    ERROR("could not duplicate file string.");
    free(fs);
    return NULL;
  }
  
  // Iterate through each file
  for (i = 0, f = strtok(lfiles, ":"); f; f = strtok(NULL, ":"), ++i) {
  
    // Iterate through each worker process
    for (wid = 0; wid < ps_ctl->nprocesses; ++wid) {

      // Add file to virtual file table
      if (!ps_ctl_add_file(ps_ctl, wid, f, size, type)) {
        ERROR("failed to add file to process control.");
        free(fs);
        free(lfiles);
        return NULL;
      }
    }
   
    // Copy file name to file set 
    strncpy(fs->fn[i], f, MAX_FILE_PATH);
  }

  // Free memory
  free(lfiles); 

  // Return the file set
  return fs;
}

