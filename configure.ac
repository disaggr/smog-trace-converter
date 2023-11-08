
AC_INIT([smog-trace-converter], [0.1])

AC_CONFIG_SRCDIR([src/smog-trace-converter.cpp])
AC_CONFIG_AUX_DIR([build-aux])

AM_INIT_AUTOMAKE([-Wall -Werror foreign])

AC_PROG_CXX

PKG_CHECK_MODULES(libparquet, [ arrow, parquet ])
AC_SUBST([libparquet_CFLAGS])
AC_SUBST([libparquet_LIBS])

PKG_CHECK_MODULES(libpng, [ libpng ])
AC_SUBST([libpng_CFLAGS])
AC_SUBST([libpng_LIBS])

AC_OPENMP
AC_SUBST([OPENMP_CFLAGS])

AC_CONFIG_FILES([Makefile])

AC_OUTPUT
