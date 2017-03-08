/**
 * \file process_pool.c
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Contains function for worker process pool.
 */


// Enable Linux sched support for cpu-affinity right now
//#define _GNU_SOURCE 

#include "process_pool.h"


/**
 * \brief Lookup process by worker-id
 *
 *  Search for the worker process identifier of the worker processes with a specific main process identifier.
 *
 * \param[in] worker_ps Worker process table 
 * \param[in] pid Process identifier
 * \param[in] nproc Number of worker processes 
 * \retval wid_t Worker process identifier
 */
static wid_t worker_for_pid (struct worker_process *worker_ps, pid_t pid, int nproc)
{
  int i;    // Iteration variable

  // Iterate through all the worker processes to search for matching process identifier.
  // Return the corresponding worker process identifier.
  for (i = 0; i < nproc; ++i) {
    if (worker_ps[i].pid == pid) {
      return worker_ps[i].wid;
    }
  }

  // If PID did not match, return invalid WID
  return BAD_WID;
}


/**
 * \brief Fork worker process
 *
 * The environment variables are copied and a child process is forked.
 * Uses the external "environ" variable to access key/value pairs.
 *
 * \param[in] hspwrap_pid HSP-Wrap process identifier
 * \param[in] wid Worker process identifier
 * \param[in] exe Executable to run
 * \param[in] argv Command line arguments
 * \retval Worker PID 
 * \retval -1 An error occurred
 */
static pid_t fork_worker (pid_t hspwrap_pid, wid_t wid, const char *exe, char **argv);


/**
 * \bug Uses environ external variable without declaring it. Strange that it compiles.
 * \bug Memory leak with new environment list. Need to copy strings into new list instead of copying pointers. Malloc each list entry based on strlen of environ element.
 *
 * \todo Change how strings are copied to environment list and env array.
 */
static pid_t 
fork_worker (pid_t hspwrap_pid, wid_t wid, const char *exe, char **argv)
{
#ifdef SET_TRACE
  char env[3][40];   // Array for environment key/value pairs
#else
  char env[2][40];   // Array for environment key/value pairs
#endif
  char **env_list;   // Environment list for worker processes
  int nenviron;      // Number of key/value pairs in environment
  int i;             // Iteration variable
  pid_t pid;         // PID for worker processes

  info("Call to fork_worker()");
  
  // Count environment variables
  for (nenviron=0; environ[nenviron]; ++nenviron) ;

  // Make new list (leak!)
  // Add 3 entries at the end for process and worker identifiers.
  // The last entry is flagged with a NULL value.
#ifdef SET_TRACE
  env_list = malloc((nenviron + 4) * sizeof(char *));
#else
  env_list = malloc((nenviron + 3) * sizeof(char *));
#endif

  // Copy environment pointers
  for (i=0; i<nenviron; ++i) {
    env_list[i] = environ[i];
  }
  env_list[i++]   = env[0];
  env_list[i++] = env[1];
#ifdef SET_TRACE
  env_list[i++] = env[2];
  env_list[i++] = NULL;
#else
  env_list[i++] = NULL;
#endif

  // Start child process
  pid = fork();

  // Child
  if (pid == 0) {

    // Get the worker PID
    pid = getpid();
    info("Forked worker %u", wid);

#ifdef HSP_GDB_TRACE
    // Use gdb to attach to worker process
    printf("Ready for attaching trace to worker process %d, sleeping 30 sec...\n", (int)pid);
    sleep(30);
#endif

    // Copy HSP-Wrap process identifier and worker identifier
    snprintf(env[0], ARRAY_SIZE(env[0]), PID_ENVVAR "=%d", (int)hspwrap_pid);
    snprintf(env[1], ARRAY_SIZE(env[1]), WORKER_ID_ENVVAR "=%u", wid);
#ifdef SET_TRACE
    snprintf(env[2], ARRAY_SIZE(env[2]), "HSPWRAP_RANK=%d", mpirank);
#endif

    // Execute program
    if (execve(exe, argv, env_list)) {
      ERROR("could not exec: %s", strerror(errno));
      kill(pid, SIGKILL);
    }
  }
  // Parent (process pool daemon)
  else if (pid > 0) {

#ifdef HSP_GDB_TRACE
    // Use gdb to attach to worker process
    printf("Ready for attaching trace to process pool %d, sleeping 30 sec...\n", (int)pid);
    sleep(30);
#endif

    return pid;
  }
  
  // Fork error
  ERROR("failed to fork worker process.");
  return -1;
}


// Initialize process pool control structure
struct process_pool_ctl *
process_pool_ctl_init (pid_t hspwrap_pid)
{
  pthread_mutexattr_t mattr;          // Mutex attribute
  pthread_condattr_t  cattr;          // Condition variable attribute
  int fd;                             // File descriptor of shared memory
  int rc;                             // Function return code
  struct process_pool_ctl *pool_ctl;

  info("Call to process_pool_ctl_init()");

  // Initialize inter-process mutex attributes object
  rc = pthread_mutexattr_init(&mattr);
  if (rc != 0) {
    ERROR("could not initalize mutex attributes.");
    return NULL;
  }

  // Set mutex attribute to shared
  rc = pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
  if (rc != 0) {
    ERROR("could not set mutex attributes.");
    return NULL;
  }

  // Initialize inter-process condition variables attributes object
  rc = pthread_condattr_init(&cattr);
  if (rc != 0) {
    ERROR("could not initalize condition variable attributes.");
    return NULL;
  }

  // Set condition variables attribute to shared
  rc = pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
  if (rc != 0) {
    ERROR("could not set condition variable attributes.");
    return NULL;
  }

  // Create shared memory and mapped memory 
#ifdef HSP_SYSV_SHM
  pool_ctl = create_shm_sysv(hspwrap_pid + 1, sizeof(struct process_pool_ctl), &fd); 
#else
  char shmname[256];                  // Shared memory name
  snprintf(shmname, 256, "/hspwrap.%d.%s", hspwrap_pid, POOL_CTL_SHM_NAME);
  pool_ctl = create_shm_posix(shmname, sizeof(struct process_pool_ctl), &fd); 
  info("Process pool created pool control shared memory: %s", shmname);
#endif
  
  // Validate shared memory handle
  if (pool_ctl == NULL) {
    ERROR("failed to create process pool control shared memory.");
    return NULL;
  }

  // Close file descriptor of shared memory
  close(fd);

  // Initialize process pool control structure, mutex, and condition variables
  pool_ctl->nprocesses = 0;
  pool_ctl->ready = 0;

  rc = pthread_mutex_init(&pool_ctl->lock, &mattr);
  if (rc != 0) {
    ERROR("could not initalize mutex.");
    return NULL;
  }
  rc = pthread_cond_init(&pool_ctl->wait, &cattr);
  if (rc != 0) {
    ERROR("could not initalize condition variable.");
    return NULL;
  }
  rc = pthread_cond_init(&pool_ctl->run, &cattr);
  if (rc != 0) {
    ERROR("could not initalize condition variable.");
    return NULL;
  }

  return pool_ctl;
}


// Fork process pool process
struct process_pool_ctl *
process_pool_fork ()
{
  int rc;                // Function return code
  pid_t hspwrap_pid;     // HSP-Wrap PID
  pid_t pool_pid;        // Forked process pool PID
  pid_t tmp_pid;         // Temporary process PID
  struct process_pool_ctl *pool_ctl;

  info("Call to process_pool_fork()");
 
  // Get the HSP-Wrap PID
  hspwrap_pid = getpid();
  info("HSP-Wrap PID: %d", (int)hspwrap_pid);

  // Create control structure
  pool_ctl = process_pool_ctl_init(hspwrap_pid);
  if (pool_ctl == NULL) {
    ERROR("failed to initialize process pool control.");
    return NULL;
  }

  // Daemonize: Fork the process pool control
  tmp_pid = fork();
 
  // Temporary child process
  if (tmp_pid == 0) {

    tmp_pid = getpid();
    info("Temporary pool PID: %d", (int)tmp_pid);
      
    // Fork process pool and make a daemon
    pool_pid = fork();

    // Process pool daemon
    if (pool_pid == 0) { 

      pool_pid = getpid();
      info("Process pool PID: %d", (int)pool_pid);

#ifndef HSP_GDB_TRACE
      // Change file-mode mask to no permissions
      umask((mode_t)0);

      // Run in a new session / process group
      if (setsid() == -1) {
        ERROR("could not create process pool session.");
        exit(EXIT_FAILURE);
      }

      // Close standard streams (allow stdout and stderr for debugging) 
      //fclose(stdin);
      //fclose(stdout);
      //fclose(stderr);

      // Remap standard streams (allow stdout and stderr for debugging)
      //FILE *fnull_in = freopen("/dev/null", "r", stdin);
      //FILE *fnull_out = freopen("/dev/null", "w", stdout);
      //FILE *fnull_err = freopen("/dev/null", "w", stderr);
#endif
      
      // Change to ready state to signal main process
      pthread_mutex_lock(&pool_ctl->lock);
      pool_ctl->ready = 1;
      pthread_cond_signal(&pool_ctl->wait);
      info("Process pool sends ready signal to controller.");
     
      // Start process pool operations
      rc = process_pool_start(hspwrap_pid, pool_pid, pool_ctl); 
      if (rc != EXIT_SUCCESS)
        ERROR("failed to start process pool.");

#ifndef HSP_GDB_TRACE
      // Close file streams
      //fclose(fnull_in);
      //fclose(fnull_out);
      //fclose(fnull_err);
#endif

      kill(pool_pid, SIGKILL);
    }
    // Temporary process, just exits to allow child to become a daemon
    else if (pool_pid > 0) {
      kill(tmp_pid, SIGKILL);
    }
    // Temporary process fork() error
    else {
      // Change to error state to signal main process
      ERROR("could not fork process pool.");
      pthread_mutex_lock(&pool_ctl->lock);
      pool_ctl->ready = -1;
      pthread_cond_signal(&pool_ctl->wait);
      pthread_mutex_unlock(&pool_ctl->lock);
      kill(tmp_pid, SIGKILL);
    } 
  }
  // Parent controller process
  else if (tmp_pid > 0) {

    // Wait until process pool is ready or an error occurs
    info("Controller waiting on ready signal from process pool.");
    pthread_mutex_lock(&pool_ctl->lock);
    while (pool_ctl->ready == 0) {
      pthread_cond_wait(&pool_ctl->wait, &pool_ctl->lock);
    }
    pthread_mutex_unlock(&pool_ctl->lock);
    info("Controller received ready signal from process pool.");

    // If an error occurs, return 
    if (pool_ctl->ready == -1)
      return NULL;

    return pool_ctl;
  }
  // Fork() error
  else {
    ERROR("could not fork temporary process for process pool.");
    return NULL;
  }

  return NULL;
}


// Parse command line arguments of wrapped program, attach to shared process control structure, fork worker processes, and wait for worker processes to complete.
int
process_pool_start (pid_t hspwrap_pid, pid_t pool_pid, struct process_pool_ctl *pool_ctl)
//, const char *workdir, int nproc)
{
  struct process_control *ps_ctl = NULL;   // Process pool control structure
  pid_t exit_pid;      // PID for exit worker process
  pid_t work_pid;      // PID for worker process
  wid_t wid;           // Worker ID
  int forked;          // Number of forked processes
  int status;          // Status flag
  char *fullCmdline = NULL;   // Complete command line for program (exefile + cmdline)
  char *fullExefile = NULL;       // Executable path
  char *exefile = NULL;       // Executable path
  size_t len;          // Length of full command line
  int i;               // Iteration variable
  char **argv = NULL;  // Parsed command line arguments for forked worker processes
  char *cmdline = NULL;
  const char *workdir = NULL;
  int nproc;
  struct worker_process *worker_ps = NULL;  // Worker process table

  info("Call to process_pool_start()");

  // Get the executable
  fullExefile = getenv("HSP_EXEFILE");
  if (!fullExefile) {
    ERROR("could not find executable file."); 
    return EXIT_FAILURE;
  }
  exefile = strip_path(fullExefile);

  // Read command line arguments for wrapped program
  cmdline = getenv("HSP_EXEARGS");
  if (!cmdline || (int)(*cmdline) == 0) {
    WARN("missing HSP_EXEARGS environment variable.");
    cmdline = NULL;
    len = strlen(exefile) + 1;
    fullCmdline = malloc(len * sizeof(*fullCmdline));
    strncpy(fullCmdline, exefile, len);
  }
  else {
    len = strlen(exefile) + strlen(cmdline) + 2;
    fullCmdline = malloc(len * sizeof(*fullCmdline));
    snprintf(fullCmdline, len, "%s %s", exefile, cmdline);

    // FIXME: leaks the duplicate, doesn't check for NULL
    argv = parseCmdline(fullCmdline);
  }
  info("Worker command line: %s", fullCmdline);

  // Wait until active processes are set by master and controllers.
  // This occurs in their respective main function via process_pool_spawn().
  info("Process pool waiting run signal from master/controllers.");
  while (pool_ctl->nprocesses == 0) {
    pthread_cond_wait(&pool_ctl->run, &pool_ctl->lock);
  }
  pthread_mutex_unlock(&pool_ctl->lock);
  info("Process pool received run signal from master/controllers.");

  // Validate process pool operations
  if (pool_ctl->nprocesses < 1) {
    WARN("invalid active processes, killing process pool daemon.");
    return EXIT_FAILURE;
  }
  else {
    nproc = pool_ctl->nprocesses;
    workdir = pool_ctl->workdir;
    info("Process pool PID %d under HSP-Wrap PID %d is starting...\n"
         "  cwd: %s\n"
         "  workdir:   %s\n"
         "  processes: %d",
         (int)pool_pid, hspwrap_pid, getcwd(NULL, 0), workdir, nproc);
 
  }
 
  // Change to working directory
  if (chdir(workdir))
     WARN("Process pool failed to chdir: %s", workdir);

  // Attach to shared process control structure
  // TODO: Refactor (we use this here and in stdiowrap)
#ifdef HSP_SYSV_SHM
  ps_ctl = mmap_shm_sysv(hspwrap_pid + 0, sizeof(struct process_control), NULL);
#else
  char shmname[256];
  snprintf(shmname, 256, "/hspwrap.%d.%s", hspwrap_pid, PS_CTL_SHM_NAME);
  ps_ctl = mmap_shm_posix(shmname, sizeof(struct process_control), NULL);
  info("Process pool attached to process control shared memory: %s", shmname);
#endif

  // Allocate worker process structure 
  worker_ps = malloc(nproc * sizeof(struct worker_process));
  if (!worker_ps) {
    ERROR("failed to allocate worker process structure.");
    return EXIT_FAILURE;
  }

  // Fork worker processes
  forked = 0;
  for (wid = 0; wid < nproc; ++wid) {
    worker_ps[wid].wid = wid;
    worker_ps[wid].pid = 0;
    worker_ps[wid].status = 0;

    work_pid = fork_worker(hspwrap_pid, wid, exefile, argv);
    if (work_pid == BAD_PID) {
      ERROR("failed to fork worker %" PRI_WID, wid);
    }
    else {
      info("WID:%" PRI_WID ", PID:%d started.", wid, (int)work_pid);
      worker_ps[wid].pid = work_pid;
      worker_ps[wid].status = 1;
      forked++;
    }
  }

  // Wait on worker processes if terminated or stopped/resumed by a signal 
  for (i = 0; i < forked; ++i) {
    exit_pid = wait(&status);

    wid = worker_for_pid(worker_ps, exit_pid, nproc);
    if (wid == BAD_WID)
      continue;

    // Check if the child terminated normally
    if (WIFEXITED(status))
      trace("Worker %u (pid %d) exited with status %d", wid, (int)exit_pid, WEXITSTATUS(status));
    else if (WIFSIGNALED(status))
      trace("Worker %u (pid %d) exited with signal %d", wid, (int)exit_pid, WTERMSIG(status));

    // Update process control structure to worker done and no command
    pthread_mutex_lock(&ps_ctl->lock);
    ps_ctl->process_state[wid] = DONE;
    ps_ctl->process_cmd[wid]   = NO_CMD;

    // Signal that worker process needs service
    pthread_cond_signal(&ps_ctl->need_service);
    pthread_mutex_unlock(&ps_ctl->lock);
  }

  info("Process pool PID %d under HSP-Wrap PID %d terminated.", pool_pid, hspwrap_pid);

  return EXIT_SUCCESS;
}

