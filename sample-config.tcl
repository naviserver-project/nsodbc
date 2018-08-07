
#
# Red Brick ODBC Driver sample configuration
#
#  The Red Brick ODBC driver is specified here.  It is an internal
#  driver.  The driver relies on two environment variables:
#    export LD_LIBRARY_PATH=/path/to/rb/client/lib/dir
#    export RB_CONFIG=/path/to/rb/client/dir/with/ini/file
#
ns_section "ns/db/drivers"
ns_param   nsrbodbc        nsrbodbc.so ;# Use the module for an internal driver
ns_param   defaultpool     mypool    ;# Optionally specify default pool
ns_param   pools           mypool    ;# Optionally specify list of pools

#
# The "param" config item serves no purpose.  It is a placeholder.
#
ns_section "ns/db/driver/nsrbodbc"
ns_param   param           ""

# Specify the name of the database pool here.
ns_section "ns/db/pools"
ns_param   mypool          "Red Brick database pool"

# Describe the pool in detail here.  This section depends on the db driver.
ns_section "ns/db/pool/mypool"
ns_param   driver          nsrbodbc    ;# From "ns/db/drivers" list
ns_param   datasource      "whatever"  ;# I hope *you* know what this should be
ns_param   user            "username"  ;# Username for database
ns_param   password        "userpass"  ;# Password for database
ns_param   connections     1         ;# No. of connections to open
ns_param   logsqlerrors    true      ;# Verbose SQL query error logging
ns_param   maxidle         600       ;# Max time to keep idle db conn open
ns_param   maxopen         3600      ;# Max time to keep active db conn open
ns_param   verbose         true      ;# Verbose error logging


# Tell the virtual server about the pools it can use.
ns_section "ns/server/${servername}/db"
ns_param   pools *

