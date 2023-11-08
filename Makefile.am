
AUTOMAKE_OPTIONS = subdir-objects

bin_PROGRAMS = smog-trace-converter smog-trace-bitmap

smog_trace_converter_CPPFLAGS = -Isrc/ -Wall -Wextra
smog_trace_converter_CXXFLAGS = @libparquet_CFLAGS@ @OPENMP_CFLAGS@
smog_trace_converter_LDADD = @libparquet_LIBS@

smog_trace_converter_SOURCES = src/smog-trace-converter.cpp

smog_trace_bitmap_CPPFLAGS = -Isrc/ -Wall -Wextra
smog_trace_bitmap_CXXFLAGS = @libpng_CFLAGS@
smog_trace_bitmap_LDADD = @libpng_LIBS@

smog_trace_bitmap_SOURCES = src/smog-trace-bitmap.cpp
