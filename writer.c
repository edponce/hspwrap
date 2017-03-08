/**
 * \file writer.c
 * \author NICS/ORNL
 * \date June 2014
 *
 * \brief Contains functions for writer threads
 */


#include "writer.h"


/**
 * \brief Main function for writer threads.
 *
 * This function is called indirectly by the slave main method once per output file per slave process.
 * The argument provided is a writer context structure which is already initialized. The writer context contains a lock, condition variables, and buffers to communicate with the slave process and manage output files.
 * The output and index files are opened and then writer starts service loop. The writer context structure contains buffers metadata and data needed to be written to file. When the writer is signaled that data to be flush is pending in the first buffer, the writer swaps the second and first buffer; and uses the seond buffer to write to file. This way the buffer lock can be released while writer flushes and the slave process copies worker output to the first buffer.
 *
 * param[in] arg Initialized writer context structure
static void *
 * \retval 0 Successful termination
 *
 * \todo Consider nonblocking approach.
 * \todo Fix writer waiting condition for flushing.
 * \todo Add loop for writing to output file if bytes written does not match.
 * \todo Do not flush to filesystem after every write operation.
 */
static void * writer_main (void *arg);


// Main functin for writer threads. Flush shared output buffers to output file.
static void *
writer_main (void *arg)
{
  struct writer_ctx *ctx = arg;  // Get writer context structure from argument
  int   fd;                      // File descriptor to output file
  int idx;                       // File descriptor to index file
  char *idx_name;                // Name of index file

  // Open output file and get file descriptor into writer context
  fd = open(ctx->name, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (fd == -1) {
    remove(ctx->name);
    fd = open(ctx->name, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
      ERROR("writer: Could not open output file.");
      return ctx;
    }
  }
  ctx->fd = fd;

  // Allocate and create name of index file
  // name + 4 file extension + 1 NULL
  idx_name = malloc(strlen(ctx->name) + 4 + 1);
  if (idx_name == 0) {
    ERROR("writer: Could not allocate index filename.");
    return ctx;
  }
  strcpy(idx_name, ctx->name);
  strcat(idx_name, ".idx");

  // Open index file and get file descriptor into writer context
  idx = open(idx_name, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR|S_IWUSR | S_IRGRP|S_IROTH);
  if (idx == -1) {
    remove(idx_name);
    idx = open(idx_name, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR|S_IWUSR | S_IRGRP|S_IROTH);
    if (idx == -1) {
      ERROR("writer: Could not open index file.");
      return ctx;
    }
  }
  ctx->idx = idx;
  free(idx_name);

  // Provide service until slave signals writer to exit
  // 1: keep running, 0: exit main loop
  while (ctx->running) {

    // Wait until slave signals to flush output and buffer is at least half full
    pthread_mutex_lock(&ctx->lock);
    while (ctx->running && ctx->avail > ctx->size/2) {
      pthread_cond_wait(&ctx->data_pending, &ctx->lock);
    }

    info("Writer flushing data\n");

    // Create a temporary buffer and swap with writer output buffer
    char *tmp     = ctx->buf;
    ctx->buf      = ctx->back_buf;
    ctx->back_buf = tmp;

    // Compute the length of valid data
    ctx->back_len = ctx->size - ctx->avail;

    // Reset the available space (we are flushing) 
    ctx->avail    = ctx->size;

    // Set buffer pointer to swapped second output buffer
    ctx->ptr      = ctx->buf;

    // Done with public state, unlock and signal slave that  
    pthread_mutex_unlock(&ctx->lock);
    pthread_cond_signal(&ctx->space_avail);

    // Write contents of second output buffer into the output file
    ssize_t bytes = write(fd, ctx->back_buf, ctx->back_len);
    if (bytes == -1) {
      ERROR("Couldn't write output: %s", strerror(errno));
    } else {
      trace("Wrote %zd of %zu bytes\n", bytes, ctx->back_len);
    }

    // Flush file contents to filesystem
    fsync(fd);
    fsync(idx);

    // Reset length of valid data
    ctx->back_len = 0;
  }

  // Close output file descriptor
  close(fd);

  return ctx;
}


// Initialize a writer context structure and spawn a writer thread. This function is called by the slave process once per output file.
int
writer_init (struct writer_ctx *ctx, const char *name, size_t buff_size, int nstreams)
{
  int rc;   // Function return code

  // Initialize writer context using default and argument values
  ctx->name  = name;
  ctx->size  = buff_size;
  ctx->avail = ctx->size;
  ctx->buf   = malloc(ctx->size);
  if (!ctx->buf) {
    fprintf(stderr, "Error: could not allocate for writer buffer.\n");
    return EXIT_FAILURE;
  }
  ctx->ptr   = ctx->buf;
  ctx->back_buf = malloc(ctx->size);
  if (!ctx->back_buf) {
    fprintf(stderr, "Error: could not allocate for writer back buffer.\n");
    free(ctx->buf);
    return EXIT_FAILURE;
  }
  ctx->back_len = 0;
  ctx->running  = 1;
  ctx->nstreams = nstreams;
  ctx->voffsets = malloc(nstreams * sizeof(uint64_t));
  if (!ctx->voffsets) {
    fprintf(stderr, "Error: could not allocate for writer voffsets.\n");
    free(ctx->buf);
    free(ctx->back_buf);
    return EXIT_FAILURE;
  }
  ctx->poffset  = 0;

  // Reset the virtual offsets of worker streams
  memset(ctx->voffsets, 0, nstreams * sizeof(uint64_t));

  // Initialize writer context lock and condition variables
  rc = pthread_mutex_init(&ctx->lock, NULL);
  if (rc != 0) {
    fprintf(stderr, "Error: could not initalize mutex\n");
    free(ctx->buf);
    free(ctx->back_buf);
    free(ctx->voffsets);
    return EXIT_FAILURE;
  }

  rc = pthread_cond_init(&ctx->data_pending, NULL);
  if (rc != 0) {
    fprintf(stderr, "Error: could not initalize condition variable\n");
    free(ctx->buf);
    free(ctx->back_buf);
    free(ctx->voffsets);
    return EXIT_FAILURE;
  }

  rc = pthread_cond_init(&ctx->space_avail, NULL);
  if (rc != 0) {
    fprintf(stderr, "Error: could not initalize condition variable\n");
    free(ctx->buf);
    free(ctx->back_buf);
    free(ctx->voffsets);
    return EXIT_FAILURE;
  }

  // Spawn a writer thread
  rc = pthread_create(&ctx->thread, NULL, writer_main, ctx);
  if (rc != 0) {
    fprintf(stderr, "Error: failed to create writer thread.\n");
    free(ctx->buf);
    free(ctx->back_buf);
    free(ctx->voffsets);
    return EXIT_FAILURE;
  } 

  return EXIT_SUCCESS;
}


// Write function for writer threads, data from worker process is copied into the writer context buffer. Metadata is stored in the index file. This function is called indirectly by the slave process.
void
writer_write (struct writer_ctx *ctx, uint16_t stream, const void *buf, size_t count)
{
  char crc[] = {'C', 'R', 'C', '!'};  // Delimiter symbols for metadata in index file
  uint64_t cnt = count;               // Size of data buffer provided

  // If no data was provided, exit
  if (count == 0) {
    return;
  }

  // If not enough space in buffer, signal writer thread to flush to file 
  pthread_mutex_lock(&ctx->lock);
  if (ctx->avail < count) {
    pthread_cond_signal(&ctx->data_pending);
  }

  // Wait until writer flushes and space in buffer is available
  while (ctx->avail < count) {
    pthread_cond_wait(&ctx->space_avail, &ctx->lock);
  }

  // Write worker process output to writer buffer
  memcpy(ctx->ptr, buf, count);

  // Update writer pointer based on the data written
  ctx->ptr     += count;

  // Update the available space in buffer
  ctx->avail   -= count;

  // Write metadata to index file
  write(ctx->idx, &stream, sizeof(stream));
  write(ctx->idx, &ctx->voffsets[stream], sizeof(uint64_t));
  write(ctx->idx, &ctx->poffset, sizeof(uint64_t));
  write(ctx->idx, &cnt, sizeof(cnt));
  write(ctx->idx, &crc, sizeof(crc));

  // Adjust offsets 
  ctx->poffset  += count;
  ctx->voffsets[stream] += count;

  // Release writer context lock
  pthread_mutex_unlock(&ctx->lock);
}

