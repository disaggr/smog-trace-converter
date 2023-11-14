
AUTOMAKE_OPTIONS = subdir-objects

bin_PROGRAMS = smog-trace-converter

smog_trace_converter_CPPFLAGS = -Isrc/ -Wall -Wextra -Werror
smog_trace_converter_CFLAGS = @libparquet_CFLAGS@ @OPENMP_CFLAGS@ @libpng_CFLAGS@
smog_trace_converter_CXXFLAGS = $(smog_trace_converter_CFLAGS)
smog_trace_converter_LDADD = @libparquet_LIBS@ @libpng_LIBS@

smog_trace_converter_SOURCES = src/smog-trace-converter.c src/smog-trace-converter.h \
                               src/args.c src/args.h \
                               src/util.c src/util.h \
                               src/tracefile.c src/tracefile.h \
                               src/backends/parquet.cpp src/backends/parquet.h \
                               src/backends/png.c src/backends/png.h \
                               src/backends/png-frames.cpp src/backends/png-frames.h
