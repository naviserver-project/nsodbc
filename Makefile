#
# Stub for building a generic ODBC driver
#

all:	generic

generic:
	$(MAKE) -f Makefile.generic

install:	all
	$(MAKE) -f Makefile.generic install

clean:
	$(MAKE) -f Makefile.generic clean

clobber:
	$(MAKE) -f Makefile.generic clobber

