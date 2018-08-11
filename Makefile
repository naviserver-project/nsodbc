ifndef NAVISERVER
    NAVISERVER  = /usr/local/ns
endif

#
# Place, from where odbc library and includes can be found
#
ODBC=/opt/local

#
# Module name
#
MOD      =  nsodbc.so

#
# Objects to build.
#
MODOBJS  = nsodbc.o

MODLIBS  = -lnsdb -L$(ODBC)/lib -lodbc
CPPFLAGS = -I$(NAVISERVER)/include -I$(ODBC)/include 

include  $(NAVISERVER)/include/Makefile.module

