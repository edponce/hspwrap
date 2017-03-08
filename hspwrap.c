/**
 * \file hspwrap.c
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Contains main HSP-Wrap functions for master and slave processes
 *
 * Contains main HSP-Wrap function for master and slave processes
 *
 * \todo Use fputs instead of fprintf whenever possible.
 * \todo Remember to flush stdout, stderr before terminating program.
 */

#include "hspwrap.h"
#include "process_pool.h"
#include "master.h"
#include "controller.h"

#define LOCAL_EXEFILE "exefile"

// Set tracing option global MPI rank
#ifdef SET_TRACE
pid_t PID = BAD_WID;
int mpirank;
#endif


/**
 * \brief Master distributes shared files to controllers
 *
 * \param[inout] controllers Controller context structure
 * \param[in] rank MPI process rank
 * \param[in] bcast_chunk_size Chunk size for database broadcast
 * \retval EXIT_SUCCESS Successful completion
 * \retval EXIT_FAILURE An error occurred
 */
static int distributeSharedFiles(struct controller_ctx *controllers, int rank, int bcast_chunk_size);


/**
 * \brief Find minimum value of size_t objects
 *
 * \param[in] a First value
 * \param[in] b Second value
 * \retval Minimum value
 */
size_t MIN (size_t a, size_t b)
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
size_t MAX (size_t a, size_t b)
{
  return (a < b) ? b : a;
}


/**
 * \brief Place holder for cleanup function called by all MPI processes when exiting program, either normal or abnormal termination.
 *
 * \retval None
 */
static void fnExitAllMPIprocs (void);


// Place holder for cleanup function called by all MPI processes when exiting program, either normal or abnormal termination.
static void fnExitAllMPIprocs (void)
{
  int rc;

  info("HSP-Wrap terminated. Cleaning...");

  rc = hspclean_main();
  if (rc != EXIT_SUCCESS)
    info("Cleaning had problems.");
  else
    info("Cleaning successful.");
}


// Launch wrapped program in standard mode, that is, without HSP-Wrap intervention.
int
launchStdMode (void)
{
  char *outdir = NULL;       // Output directory
  char *fullExefile = NULL;  // Program to execute (full path)
  char *exefile = NULL;      // Program to execute (binary name, used for argv[0])
  char *cmdline = NULL;      // Command line for program
  char *fullCmdline = NULL;    // Complete command line for program (exefile + cmdline)
  char **argvStd = NULL;     // Parse command line for program
  char **envList = NULL;     // Environment list for launch program
  size_t len;                // Length of buffers for malloc
  int nenv;                  // Number of key/value pairs in environment
  int i;                     // Iteration variable
  int rc;                    // Function return code

  // Get output directory
  outdir = getenv("HSP_OUTDIR");
  if (!outdir || (int)(*outdir) == 0)
  {
    WARN("could not find output directory in environment, using default.");
    outdir = HSP_OUTDIR;
  }

  // Create the output directory, if does not exist
  rc = mkpath(outdir, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
  if (rc == EXIT_FAILURE)
  {
    ERROR("could not create output directory.");
    return EXIT_FAILURE;
  }

  // Get the executable full path
  fullExefile = getenv("HSP_EXEFILE_STD");
  if (!fullExefile || (int)(*fullExefile) == 0)
  {
    ERROR("could not find executable file in environment.");
    fullExefile = NULL;
    return EXIT_FAILURE;
  }

  // Get only the name of executable
  exefile = strip_path(fullExefile);
  if (!exefile)
    exefile = fullExefile;

  // Get command line arguments
  cmdline = getenv("HSP_EXEARGS_STD");
  if (!cmdline || (int)(*cmdline) == 0)
  {
    WARN("could not find command line arguments in environment.");
    cmdline = NULL; 
    len = strlen(exefile) + 1;
    fullCmdline = malloc(len * sizeof(*fullCmdline));
    strncpy(fullCmdline, exefile, len);
  }
  else
  {
    len = strlen(exefile) + strlen(cmdline) + 2;
    fullCmdline = malloc(len * sizeof(*fullCmdline));
    snprintf(fullCmdline, len, "%s %s", exefile, cmdline);

    // Parse command line
    argvStd = parseCmdline(fullCmdline);
  }
  info("Standard mode command line: %s", fullCmdline);

  // Count environment variables and copy to list for program
  // The last entry is flagged with NULL.
  for (nenv = 0; environ[nenv]; ++nenv);
  envList = malloc((nenv + 1) * sizeof(*envList));
  if (!envList)
  {
    ERROR("could not allocate space for environment list.");
    free(argvStd);
    return EXIT_FAILURE;
  }
  for (i = 0; i < nenv; ++i)
    envList[i] = environ[i];
  envList[i] = NULL;

/*
  // Change to present output directory
  rc = chdir(outdir);
  if (rc == -1)  
  {
    ERROR("could not change to output directory.");
    return EXIT_FAILURE;
  }
*/

  // Execute program
  // This function does not return
  rc = execve(fullExefile, argvStd, envList);
  if (rc == -1)
  {
    ERROR("could not execute program, errno %s.", strerror(errno));
    free(argvStd);
    free(envList);
    return EXIT_FAILURE;
  }

  free(argvStd);
  free(envList);
  return EXIT_SUCCESS;
}


/**
 * \brief Entry point of HSP-Wrap code.
 *
 * In main, the following operations are performed:
 * 1. Use environment variables to set broadcast chunk size
 * 2. Set the input format
 * 3. Pre-fork process pool
 * 4. Initialize master/slave states
 * 5. Distribute executable
 * 6. Distribute database files
 * 7. Call master/slave main functions
 *
 * \param[in] argc Number of command line arguments
 * \param[in] argv String array of command line arguments
 * \retval EXIT_FAILURE An error occurred
 * \retval EXIT_SUCCESS Program completed successfully
 *
 * \todo Validate all environment and inputs used at beginning. Should we stat files?
 */
int
main (int argc, char **argv)
{
  char *ch = NULL;     // Get environment variables
  char input_fmt;       // Format of input data
  int nworkers;        // Number of worker processes
  int rank = -1;            // MPI rank of current process
  int nranks;           // Number of MPI processes
  int rc;              // Function return value
  size_t bcast_chunk_size; // Chunk size for broadcast messages
  struct process_pool_ctl *pool_ctl = NULL;  //Process pool control structure
  struct controller_ctx *controllers = NULL; //Controller context structure
  struct master_ctx *master = NULL;           // Master context 
  char *exe = NULL;
  int stdMode;
   
#ifdef SET_TRACE
  PID = getpid();
  mpirank = rank;
#endif

  ftrace("HSP-Wrap starts");

  // Validate number of command line arguments
  if (argc != 1) {
    ERROR("invalid number of arguments.");
    ERROR("Usage: hspwrap\n"
          "HSP-Wrap configuration uses environment variables.");
    return EXIT_FAILURE;
  }

  // Register exit function, called by all MPI processes
  atexit(fnExitAllMPIprocs);

  // Pre-fork process pool
  info("Pre-forking process pool."); 
  pool_ctl = process_pool_fork();
  if (pool_ctl == NULL) {
    ERROR("failed to pre-fork process pool.");
    return EXIT_FAILURE;
  }

  // Initialize MPI
  rc = MPI_Init(NULL, NULL);
  if (rc != MPI_SUCCESS) {
    ERROR("failed to initialize MPI.");
    return EXIT_FAILURE;
  }

  // Get total number of MPI processes
  rc = MPI_Comm_size(MPI_COMM_WORLD, &nranks);
  if (rc != MPI_SUCCESS) {
    ERROR("could not get MPI ranks.");
    MPI_Finalize();
    return EXIT_FAILURE;
  }
 
  // Get MPI rank 
  rc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  if (rc != MPI_SUCCESS) {
    ERROR("could not get MPI rank.");
    MPI_Finalize();
    return EXIT_FAILURE;
  }
  
#ifdef SET_TRACE
  mpirank = rank;
#endif

  // Print banner 
  if (rank == MASTER) 
    print_banner_slant(stdout);
  
  minfo("MPI ranks: %d", nranks);
  
  // Get HSP-Wrap run mode
  ch = getenv("HSP_STD_MODE");
  if (ch == NULL || (int)(*ch) == 0) {
    WARN("could not find HSP-Wrap run mode in environment, using default.");
    ch = HSP_STD_MODE;
    ch = NULL;
  }
  else
    stdMode = atoi(ch);
  
  // Validate run mode 
  if (stdMode < 0 || stdMode > 1) {
    ERROR("invalid HSP-Wrap run mode.");
    MPI_Finalize();
    return EXIT_FAILURE;
  }

  // Run standard application
  if (stdMode == 1) {
    minfo("HSP-Wrap running in STANDARD MODE");
  
    // Terminate process pools
    pool_ctl->nprocesses = -1;
    pthread_cond_signal(&pool_ctl->run);
    
    // Terminate additional MPI processes
    if (nranks > 1 && rank != MASTER) {
      MPI_Finalize();
      return EXIT_SUCCESS;
    }

    // Prepare environment and launch standard application 
    rc = launchStdMode();
    if (rc != EXIT_SUCCESS ) { 
      ERROR("STANDARD MODE terminated abnormally.");
      MPI_Finalize();
      return EXIT_FAILURE;
    }

    MPI_Finalize();
    return EXIT_SUCCESS;
  }
  else
    minfo("HSP-Wrap running in WRAPPED MODE");

  // Get and set the number of worker processes
  ch = getenv("HSP_NWORKERS");
  if (ch == NULL || (int)(*ch) == 0) {
    WARN("could not find number of workers in environment, using default.");
    nworkers = HSP_NWORKERS;
    ch = NULL;
  } 
  else
    nworkers = atoi(ch);

  // Validate number of workers 
  if (nworkers < 1) {
    ERROR("invalid number of workers.");
    MPI_Finalize();
    return EXIT_FAILURE;
  }
  minfo("Number of workers: %d", nworkers);

  // Get and set chunk size for database broadcast
  ch = getenv("HSP_BCAST_CHUNK_SIZE");
  if (ch == NULL || (int)(*ch) == 0) {
    WARN("could not find communication chunk size in environment, using default.");
    bcast_chunk_size = HSP_BCAST_CHUNK_SIZE;
    ch = NULL;
  }
  else
    sscanf(ch, "%zu", &bcast_chunk_size);

  // Validate communication chunk size
  if (bcast_chunk_size < 1) {
    ERROR("invalid communication chunk size.");
    MPI_Finalize();
    return EXIT_FAILURE;
  }
  minfo("MPI bcast size: %zu", bcast_chunk_size);

  // Get and set input format (Line/line or FASTA/fasta)
  // Considers only the first letter of the format string
  ch = getenv("HSP_INPUT_FORMAT");
  if (ch == NULL || (int)(*ch) == 0) {
    WARN("could not find input format in environment, using default.");
    input_fmt = (char)tolower((int)HSP_INPUT_FORMAT);
    ch = NULL;
  }
  else
    input_fmt = (char)tolower((int)(*ch));

  if (input_fmt != 'l' && input_fmt != 'f') {
    ERROR("invalid input format.");
    MPI_Finalize();
    return EXIT_FAILURE; 
  }
  minfo("Input format: %c", input_fmt);
  
#ifdef HSP_SYSV_SHM
  minfo("Using System V shared memory.");
#else
  minfo("Using POSIX shared memory.");
#endif
#ifdef HSP_TMP_SHM
  minfo("Shared memory location: /tmp.");
#else
  minfo("Shared memory location: /dev/shm (system-dependent).");
#endif

  // Initialize master and controller states
  if (rank != MASTER) {
    controllers = controller_init(rank, nworkers, input_fmt);
    if (!controllers)
      ERROR("failed to initialize controller.");
  }
  else {
    master = master_init(nranks - 1, nworkers, input_fmt);
    if (!master)
      ERROR("failed to initialize master."); 
  }

  // Broadcast binary files first
  exe = getenv("HSP_EXEFILE");  
  if (!exe || (int)(*exe) == 0) {
    ERROR("could not find executable file in environment.");
    exe = NULL;
  }
  else {
    if (rank != MASTER) {
      rc = controller_broadcast_work_file(strip_path(exe), bcast_chunk_size);
      if (rc == -1)
        ERROR("controller failed to broadcast work file.");
    }
    else {
      rc = master_broadcast_file(exe, bcast_chunk_size);
      if (rc == -1)
        ERROR("master failed to broadcast work file.");
    }
  }

  // Distribute database files
  rc = distributeSharedFiles(controllers, rank, bcast_chunk_size);
  if (rc == EXIT_FAILURE)
    ERROR("failed to distribute shared files.");

  // Call master and slave main functions
  if (rank != MASTER)
    rc = controller_main(pool_ctl, controllers);
  else
    rc = master_main(pool_ctl, master);
  
  if (rc == EXIT_FAILURE)
    ERROR("HSP-Wrap main function failed.");

  MPI_Finalize();

  return EXIT_SUCCESS;
}


// Master distributes shared files to controllers
static int distributeSharedFiles(struct controller_ctx *controllers, int rank, int bcast_chunk_size)
{
  char *sharedDir = NULL;
  char *sharedFiles = NULL;
  char *fn = NULL;
  char path[MAX_FILE_PATH];
  int nfiles = 0;
  int rc;

  // Distribute shared files
  if (rank == MASTER) {
    sharedDir = getenv("HSP_SHARED_DIR");
    if (!sharedDir || (int)(*sharedDir) == 0) {
      WARN("could not find shared directory in environment, using default.");
      sharedDir = ".";
    }
  }

  sharedFiles = strdup(getenv("HSP_SHARED_FILES"));
  if (!sharedFiles || (int)(*sharedFiles) == 0) {
    WARN("could not find shared files in environment.");
    sharedFiles = NULL;
    return EXIT_SUCCESS;
  }

  // Iterate through the shared files, construct their full path, and broadcast
  for (fn = strtok(sharedFiles, ":"); fn; fn = strtok(NULL, ":")) {
    nfiles++;

    if (rank == MASTER) {
      rc = snprintf(path, sizeof(path), "%s/%s", sharedDir, fn);
      if (rc < 0) {
        ERROR("could not construct shared file path.");
        free(sharedFiles);
        return EXIT_FAILURE;
      }
    }

    trace("Broadcasting shared file: %s", fn);
    if (rank != MASTER)
      rc = controller_broadcast_shared_file(controllers->ps_ctl, fn, bcast_chunk_size);
    else
      rc = master_broadcast_file(path, bcast_chunk_size);

    if (rc == -1) {
      ERROR("failed to broadcast shared files.");
      free(sharedFiles);
      return EXIT_FAILURE;
    }
    trace("Completed broadcasting shared file: %s", fn);
  }

  free(sharedFiles);

  return EXIT_SUCCESS;
}


// Perform HSP-Wrap cleanup
int
hspclean_main (void)
{
  char *dirn;
  DIR  *dirp;
  int   cleaned, failed;
  struct dirent *dp;

  dirn = "/dev/shm";

  cleaned = failed = 0;
  dirp = opendir(dirn);

  if (!dirp) {
    ERROR("hspclean: could not open: %s\n", dirn);
    return EXIT_FAILURE;
  }

  chdir(dirn);

  while (1) {
    errno = 0;
    if ((dp = readdir(dirp)) != NULL) {
      if (strncmp(dp->d_name, "hspwrap.", 8) == 0) {
        // Found one, remove it
        if (unlink(dp->d_name) == -1) {
          WARN("hspclean: could not remove file: %s\n", dp->d_name);
          failed++;
        } else {
          cleaned++;
        }
      }
    } else if (errno == 0) {
      // End of list
      closedir(dirp);
      break;
    } else {
      // Error 
      ERROR("hspclean: could not read directory: %s\n", dirn);
      closedir(dirp);
      return EXIT_FAILURE;
    }
  }

  info("hspclean: Removed %d files, %d failed.\n", cleaned, failed);

  return EXIT_SUCCESS;
}


// Print title banner
void
print_banner (FILE *f)
{
  fprintf(f, "  _  _ ___ ___\n"
             " | || / __| _ \\__ __ ___ _ __ _ _ __\n"
             " | __ \\__ \\  _/\\ \\V  \\V / '_/ _` | '_ \\\n"
             " |_||_|___/_|   \\_/\\_/|_| \\__,_| .__/\n"
             "   HSPwrap version %2.1f       |_|\n\n", atof(HSP_VERSION));
}


// Print title banner
void
print_banner_slant (FILE *f)
{
  fprintf(f, "    __ _________\n"
             "   / // / __/ _ \\_    _________ ____\n"
             "  / _  /\\ \\/ ___/ |/|/ / __/ _ `/ _ \\\n"
             " /_//_/___/_/   |__,__/_/  \\_,_/ .__/\n"
             "   HSPwrap version %2.1f      /_/\n\n", atof(HSP_VERSION));
}

