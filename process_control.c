/**
 * \file process_control.c
 * \author NICS/UTK 
 * \date June 2014
 *
 * \brief Contains HSP-Wrap functions for intra-process control and synchronization. The shared memory process control structure is created, shared mutex and condition variables are create.
 */


#include "process_control.h"


/**
 * \bug Memory leaks. Need to add exception handling to functions and create a single function to munmap process control and unlink file descriptor of shared memory.
 */
struct process_control *
ps_ctl_init (unsigned nprocesses, int *ps_ctl_fd)
{
  struct process_control *ps_ctl = NULL;  // Shared process control structure
  pthread_mutexattr_t mattr;       // Mutex attribute
  pthread_condattr_t  cattr;       // Condition variable attribute
  int fd;                          // File descriptor
  int i;                           // Iteration variable

  // Reset value
  ps_ctl_fd = NULL;

  // Create main shared process control structure
#ifdef HSP_SYSV_SHM
  ps_ctl = create_shm_sysv(0, sizeof(struct process_control), &fd);
#else
  char shmname[256];
  snprintf(shmname, 256, "/hspwrap.%d.%s", getpid(), PS_CTL_SHM_NAME);
  ps_ctl = create_shm_posix(shmname, sizeof(struct process_control), &fd);
  info("Controller created process control shared memory: %s\n", shmname);
#endif
  if (ps_ctl == NULL) {
    fprintf(stderr, "Error: failed to create shared memory for process control.\n");
    return NULL;
  }
  
  // Initialize intra-process mutex attributes object
  if (pthread_mutexattr_init(&mattr)) {
    fprintf(stderr, "Error: could not initialize mutex attributes\n");
    return NULL;
  }

  // Set mutex attribute
  // PTHREAD_PROCESS_SHARED - can use mutex to synchronize threads within separate processes that have access to the memory where the mutex is initialized.
  // PTHREAD_PROCESS_PRIVATE - can use mutex to synchronize thhreads within a single process.
  if (pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED)) {
    pthread_mutexattr_destroy(&mattr);
    fprintf(stderr, "Error: could not set mutex attributes\n");
    return NULL;
  }

  // Initialize shared mutex (lock)
  if (pthread_mutex_init(&ps_ctl->lock, &mattr)) {
    pthread_mutexattr_destroy(&mattr);
    fprintf(stderr, "Error: could not initialize shared mutex object\n");
    return NULL;
  }

  // Free mutex attributes object
  if (pthread_mutexattr_destroy(&mattr)) {
    fprintf(stderr, "Error: could not free mutex attributes\n");
    return NULL;
  }

  // Initialize intra-process condition variable attributes object
  if (pthread_condattr_init(&cattr)) {
    fprintf(stderr, "Error: could not initalize condition variable attributes\n");
    return NULL;
  }
     
  // Set condition variable attribute
  // PTHREAD_PRCESS_SHARED - can use condition variable to signal threads within separate processes that have access to the memory where the condition variable is initialized.
  // PTHREAD_PROCESS_PRIVATE - can use condition variable to synchronize thhreads within a single process.
  if (pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED)) {
    pthread_condattr_destroy(&cattr);
    fprintf(stderr, "Error: could not set condition variable attributes\n");
    return NULL;
  }

  // Initialize shared condition variable (need_service)
  if (pthread_cond_init(&ps_ctl->need_service, &cattr)) {
    pthread_condattr_destroy(&cattr);
    fprintf(stderr, "Error: could not initialize shared condition variable object\n");
    return NULL;
  }

  // Initialize process specific variables: shared condition variable (process_ready), process state, and process command
  for (i = 0; i < nprocesses; ++i) {
    if (pthread_cond_init(&ps_ctl->process_ready[i], &cattr)) {
      for (; i > 0; --i) {
        pthread_cond_destroy(&ps_ctl->process_ready[i-1]);
      }
      pthread_condattr_destroy(&cattr);
      fprintf(stderr, "Error: could not initialize shared condition variable object\n");
      return NULL;
    }
    ps_ctl->process_state[i] = DONE;
    ps_ctl->process_cmd[i] = QUIT;
  }

  // Free condition variable attributes object
  if (pthread_condattr_destroy(&cattr)) {
    fprintf(stderr, "Error: could not free condition variable attributes\n");
    return NULL;
  }

  // Set the number of worker processes 
  ps_ctl->nprocesses = nprocesses;

  // Reset virtual file table
  ps_ctl->ft.nfiles = 0;

  // Return file descriptor of shared memory via functin argument
  if (ps_ctl_fd) {
    *ps_ctl_fd = fd;
  }

  // Return shared process control structure
  return ps_ctl;
}


/**
 * \bug Memory leaks.
 */
void *
ps_ctl_add_file (struct process_control *ps_ctl, wid_t wid,
                 const char *name, size_t sz, enum file_type type)
{
    void *shm;     // Pointer to shared memory
    int   fd;      // File descriptor of shared memory
    int nfiles;    // Number of files in virtual table 

    nfiles = ps_ctl->ft.nfiles;

    // Create shared virtual file
#ifdef HSP_SYSV_SHM
    shm = create_shm_sysv(2 + nfiles, sz, &fd);
#else
    char  shmname[256];
    snprintf(shmname, 256, "/hspwrap.%d.%d", getpid(), nfiles);
    shm = create_shm_posix(shmname, sz, &fd);
#endif
    if (!shm) {
      fprintf(stderr, "Error: failed to create shared file.\n");
      return NULL;
    }

    // Add virtual file to process control table, if file limit has not been reached
    if (nfiles < MAX_DB_FILES) {
      ps_ctl->ft.file[nfiles].shm      = shm;
      ps_ctl->ft.file[nfiles].shm_fd   = fd;
      ps_ctl->ft.file[nfiles].shm_size = sz;
      ps_ctl->ft.file[nfiles].wid      = wid;
      ps_ctl->ft.file[nfiles].size     = (wid == -1) ? sz : 0;
      ps_ctl->ft.file[nfiles].type     = type;
      strncpy(ps_ctl->ft.file[nfiles].name, name, MAX_FILE_PATH);
      ps_ctl->ft.nfiles++;
      trace("Added a virtual file: %s", name);
    } else {
      fprintf(stderr, "Error: failed to add file to virtual table.\n");
      return NULL;
    }

    // Return pointer to shared virtual file
    return shm;
}


int
ps_ctl_all_done (struct process_control *ps_ctl)
{
  int i;  // Iteration variable
  
  // Loop through all process states
  for (i = 0; i < ps_ctl->nprocesses; ++i) {
    if (ps_ctl->process_state[i] != DONE) {
      return 0;
    }
  }
  return 1;
}


int
ps_ctl_all_running (struct process_control *ps_ctl)
{
  int i;  // Iteration variable

  // Loop through all process states
  for (i = 0; i < ps_ctl->nprocesses; ++i) {
    if (ps_ctl->process_state[i] != RUNNING) {
      return 0;
    }
  }
  return 1;
}


int
ps_ctl_all_waiting (struct process_control *ps_ctl)
{
  int i;  // Iteration variable

  // Loop through all process states
  for (i = 0; i < ps_ctl->nprocesses; ++i) {
    if (ps_ctl->process_state[i] != EOD) {
      return 0;
    }
  }
  return 1;
}


void
ps_ctl_print (struct process_control *ps_ctl, FILE *f)
{
  struct file_table_entry *fte;  // Virtual file structure 
  int i;    // Iteration variable

  // Loop through available virtual files and print info
  for (i=0; i < ps_ctl->ft.nfiles; ++i) {
    fte = &ps_ctl->ft.file[i];
    fprintf(f, "  file: %4d wid: %4d path: %30s size: %zu\n",
            i, fte->wid, fte->name, fte->shm_size);
  }
}


void
printVirtualFilesTable(struct file_table *ft)
{
  int i;
  struct file_table_entry *fte;

  info("Printing Virtual Files Table:");
  for (i = 0; i < ft->nfiles; ++i) {
    fte = &ft->file[i];
    info("  #%d, %s\n"
         "  wid = %d\n"
         "  type = %d\n"
         "  size = %zu\n"
         "  shm_fd = %d\n"
         "  shm_size = %zu\n",
         i+1, fte->name, fte->wid, fte->type, fte->size, fte->shm_fd, fte->shm_size);      
  }
}

