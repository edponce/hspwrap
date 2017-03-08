# Makefile for master/controller model 
#
# target : prerequisite
# <tab>action
# <tab>...
#
# Include header files in dependencies of corresponding object files
# for automatic updates when a header file is modified.

# Compiler options
CC := mpicc
#CFLAGS := -Wall -pedantic -std=c99
CFLAGS := -Wall -std=c99

# Macro definitions
DEFS:= -D_GNU_SOURCE

# Include options
INCL := -I/usr/lib/openmpi/include

# Linker options
LFLAGS := -lm

# Name of executable
PROG := hspwrap

all: $(PROG) 

master.o: master.c master.h
	$(CC) $(CFLAGS) $(INCL) $(DEFS) -c $<

controller.o: controller.c controller.h
	$(CC) $(CFLAGS) $(INCL) $(DEFS) -c $<

writer.o: writer.c writer.h
	$(CC) $(CFLAGS) $(INCL) $(DEFS) -c $<

process_control.o: process_control.c process_control.h
	$(CC) $(CFLAGS) $(INCL) $(DEFS) -c $<

process_pool.o: process_pool.c process_pool.h
	$(CC) $(CFLAGS) $(INCL) $(DEFS) -c $<

util.o: util.c util.h
	$(CC) $(CFLAGS) $(INCL) $(DEFS) -c $<

hspwrap.o: hspwrap.c hspwrap.h
	$(CC) $(CFLAGS) $(INCL) $(DEFS) -c $<

$(PROG): master.o controller.o writer.o process_pool.o process_control.o util.o hspwrap.o
	$(CC) $(CFLAGS) $(INCL) $(DEFS) $^ -o $@ $(LFLAGS)

clean:
	rm *.o $(PROG) 

# Targets that are not real files to create
.PHONY: all clean

