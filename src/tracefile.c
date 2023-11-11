/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include "./tracefile.h"

#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>

int tracefile_open(struct smog_tracefile *tracefile, const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "%s: ", path);
        perror("open");
        return 1;
    }

    struct stat st;
    int res = fstat(fd, &st);
    if (res != 0) {
        fprintf(stderr, "%s: ", path);
        perror("fstat");
        close(fd);
        return 1;
    }

    char *buffer = mmap(0, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (buffer == MAP_FAILED) {
        fprintf(stderr, "%s: ", path);
        perror("mmap");
        close(fd);
        return 1;
    }

    close(fd);

    tracefile->buffer = buffer;
    tracefile->length = st.st_size;

    tracefile->frame_offsets = NULL;
    tracefile->num_frames = 0;

    return 0;
}

void tracefile_close(struct smog_tracefile *tracefile) {
    munmap(tracefile->buffer, tracefile->length);
    tracefile->buffer = NULL;
    tracefile->length = 0;

    free(tracefile->frame_offsets);
    tracefile->frame_offsets = NULL;
    tracefile->num_frames = 0;
}

int tracefile_index_frames(struct smog_tracefile *tracefile) {
    off_t *offsets = NULL;
    size_t n = 0;

    size_t index = 0;
    while (index < tracefile->length) {
        // add the current index to the offsets array
        off_t *new_offsets = realloc(offsets, sizeof(*offsets) * (n + 1));
        if (!new_offsets) {
            perror("realloc");
            free(offsets);
            return 1;
        }
        offsets = new_offsets;
        offsets[n++] = index;

        // advance the index over the frame
        // skip timestamp
        index += 8;

        // get number of VMAs
        uint32_t num_vmas = *(uint32_t*)(tracefile->buffer + index);
        index += 4;

        // advance the index over each VMA
        for (uint32_t i = 0; i < num_vmas; ++i) {
            // advance the index over VMA start / end
            index += 16;

            // get number of pages
            uint32_t pages = *(uint32_t*)(tracefile->buffer + index);
            index += 4;

            // advance the index over the pages
            size_t words = (pages * 2 + (32 - 1)) / 32;
            index += words * 4;
        }
    }

    tracefile->frame_offsets = offsets;
    tracefile->num_frames = n;

    return 0;
}
