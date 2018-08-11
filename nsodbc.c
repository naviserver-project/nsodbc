/*
 * The contents of this file are subject to the AOLserver Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://aolserver.com/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is AOLserver Code and related documentation
 * distributed by AOL.
 * 
 * The Initial Developer of the Original Code is America Online,
 * Inc. Portions created by AOL are Copyright (C) 1999 America Online,
 * Inc. All Rights Reserved.
 *
 * Alternatively, the contents of this file may be used under the terms
 * of the GNU General Public License (the "GPL"), in which case the
 * provisions of GPL are applicable instead of those above.  If you wish
 * to allow use of your version of this file only under the terms of the
 * GPL and not to allow others to use your version of this file under the
 * License, indicate your decision by deleting the provisions above and
 * replace them with the notice and other provisions required by the GPL.
 * If you do not delete the provisions above, a recipient may use your
 * version of this file under either the License or the GPL.
 */


#include "ns.h"
#include "nsdb.h"
#include "nsodbc.h"
#define DONT_TD_VOID 1

#ifndef REDBRICK
#include <sql.h>
#include <sqlext.h>
#else
#include <rbsql.h>
#include <rbsqlext.h>
#endif

#define RC_OK(rc) (!((rc)>>1))
#define MAX_ERROR_MSG 500
#define MAX_IDENTIFIER 256

static const char *ODBCName(void);
static int         ODBCServerInit(const char *server, const char *hModule, const char *driver);
static Ns_ShutdownProc ODBCShutdown;
static int         ODBCOpenDb(Ns_DbHandle *handle);
static int         ODBCCloseDb(Ns_DbHandle *handle); 
static int         ODBCGetRow(Ns_DbHandle *handle, Ns_Set *row);
static int         ODBCCancel(Ns_DbHandle *handle);
static int         ODBCExec(Ns_DbHandle *handle, char *sql); 
static Ns_Set     *ODBCBindRow(Ns_DbHandle *handle); 
static int	   ODBCFreeStmt(Ns_DbHandle *handle);
static void        ODBCLog(RETCODE rc, Ns_DbHandle *handle);
static const char *odbcName = "ODBC";
static HENV        odbcenv; 

static Tcl_CmdProc ODBCCmd;
static Tcl_CmdProc ODBCBindCmd;
static Ns_TclTraceProc AddCmds;

NS_EXPORT NsDb_DriverInitProc Ns_DbDriverInit;


static Ns_DbProc odbcProcs[] = {
    {DbFn_Name,       (Ns_Callback *)ODBCName},
    {DbFn_ServerInit, (Ns_Callback *)ODBCServerInit},
    {DbFn_OpenDb,     (Ns_Callback *)ODBCOpenDb},
    {DbFn_CloseDb,    (Ns_Callback *)ODBCCloseDb},
    {DbFn_GetRow,     (Ns_Callback *)ODBCGetRow}, 
    {DbFn_Flush,      (Ns_Callback *)ODBCCancel},
    {DbFn_Cancel,     (Ns_Callback *)ODBCCancel},
    {DbFn_Exec,       (Ns_Callback *)ODBCExec}, 
    {DbFn_BindRow,    (Ns_Callback *)ODBCBindRow}, 
    {0, NULL}
};


/*
 *----------------------------------------------------------------------
 *
 * Ns_DbDriverInit -
 *
 *	ODBC module load routine.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Initializes ODBC.
 *
 *----------------------------------------------------------------------
 */

NS_EXPORT int   Ns_ModuleVersion = 1;

NS_EXPORT Ns_ReturnCode
Ns_DbDriverInit(const char *driver, const char *UNUSED(configPath))
{
    if (SQLAllocEnv(&odbcenv) != SQL_SUCCESS) {
        Ns_Log(Error, "%s: failed to allocate odbc", driver);
        return NS_ERROR;
    }
    if (Ns_DbRegisterDriver(driver, odbcProcs) != NS_OK) {
        Ns_Log(Error, "%s: failed to register driver", driver);
        return NS_ERROR;
    }
    Ns_RegisterAtShutdown(ODBCShutdown, odbcenv);
    return NS_OK;
}




/*
 *----------------------------------------------------------------------
 * BadArgs --
 *
 *      Common routine that creates bad arguments message.
 *
 * Results:
 *      Return TCL_ERROR and set bad argument message as Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
BadArgs(Tcl_Interp *interp, const char **argv, const char *args)
{
    Tcl_AppendResult(interp, "wrong # args: should be \"",
        argv[0], " ", argv[1], NULL);
    if (args != NULL) {
        Tcl_AppendResult(interp, " ", args, NULL);
    }
    Tcl_AppendResult(interp, "\"", NULL);

    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 * DbFail --
 *
 *      Common routine that creates database failure message.
 *
 * Results:
 *      Return TCL_ERROR and set database failure message as Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
DbFail(Tcl_Interp *interp, Ns_DbHandle *handle, const char *cmd, char* sql)
{

  Tcl_AppendResult(interp, "Database operation \"", cmd, "\" failed", NULL);
  if (handle->cExceptionCode[0] != '\0') {
    Tcl_AppendResult(interp, " (exception ", handle->cExceptionCode,
                     NULL);
    if (handle->dsExceptionMsg.length > 0) {
      Tcl_AppendResult(interp, ", \"", handle->dsExceptionMsg.string,
                       "\"", NULL);
    }
    Tcl_AppendResult(interp, ")", NULL);
  }

    Tcl_AppendResult(interp, "\n talk to Brendan", NULL);
  Tcl_AppendResult(interp, "\nSQL: ", sql, NULL);

  ns_free(sql);

  return TCL_ERROR;
}





/* 
 * utility functions for dealing with string lists 
 */

static string_list_elt_t *
string_list_elt_new(char *string)
{
  string_list_elt_t *elt = 
    (string_list_elt_t *) ns_malloc(sizeof(string_list_elt_t));
  elt->string = string;
  elt->next = 0;

  return elt;

} /* string_list_elt_new */



static int 
string_list_len (string_list_elt_t *head)
{
  int i = 0;

  while (head != NULL) {
    i++;
    head = head->next;
  }

  return i; 

} /* string_list_len */



/* Free the whole list and the strings in it. */

static void 
string_list_free_list (string_list_elt_t *head)
{
  string_list_elt_t *elt;

  while (head) {
    ns_free(head->string);
    elt = head->next;
    ns_free(head);
    head = elt;
  }

} /* string_list_free_list */










/* Parse a SQL string and return a list of all
 * the bind variables found in it.
 */

static void
parse_odbc_bind_variables(char *input, 
                     string_list_elt_t **bind_variables, 
                     string_list_elt_t **fragments)
{
  char *p, lastchar;
  enum { base, instr, bind } state;
  char *bindbuf, *bp;
  char *fragbuf, *fp;
  string_list_elt_t *elt, *head=0, *tail=0;
  string_list_elt_t *felt, *fhead=0, *ftail=0;
  int current_string_length = 0;

  fragbuf = (char*)ns_malloc((strlen(input)+1)*sizeof(char));
  fp = fragbuf;
  bindbuf = (char*)ns_malloc((strlen(input)+1)*sizeof(char));
  bp = bindbuf;

  for (p = input, state=base, lastchar='\0'; *p != '\0'; lastchar = *p, p++) {

    switch (state) {
    case base:
      if (*p == '\'') {
	state = instr;
        current_string_length = 0;
        *fp++ = *p;
      } else if ((*p == ':') && (*(p + 1) != ':') && (lastchar != ':')) {
	bp = bindbuf;
	state = bind;
        *fp = '\0';
        felt = string_list_elt_new(ns_strdup(fragbuf));
        if(ftail == 0) {
          fhead = ftail = felt;
        } else {
          ftail->next = felt;
          ftail = felt;
        }
      } else {
        *fp++ = *p;
      }
      break;

    case instr:
      if (*p == '\'' && (lastchar != '\'' || current_string_length == 0)) {
	state = base;
      }
      current_string_length++;
      *fp++ = *p;
      break;

    case bind:
      if (*p == '=') {
        state = base;
        bp = bindbuf;
        fp = fragbuf;
      } else if (!(*p == '_' || *p == '$' || *p == '#' || isalnum((int)*p))) {
	*bp = '\0';
	elt = string_list_elt_new(ns_strdup(bindbuf));
	if (tail == 0) {
	  head = tail = elt;
	} else {
	  tail->next = elt;
	  tail = elt;
	}
	state = base;
        fp = fragbuf;
	p--;
      } else {
	*bp++ = *p;
      }
      break;
    }
  }

  if (state == bind) {
    *bp = '\0';
    elt = string_list_elt_new(ns_strdup(bindbuf));
    if (tail == 0) {
      head = tail = elt;
    } else {
      tail->next = elt;
      tail = elt;
    }
  } else {
    *fp = '\0';
    felt = string_list_elt_new(ns_strdup(fragbuf));
    if (ftail == 0) {
      fhead = ftail = felt;
    } else {
      ftail->next = felt;
      ftail = felt;
    }
  }
  
  ns_free(fragbuf);
  ns_free(bindbuf);
  *bind_variables = head;
  *fragments      = fhead;  

  return;

} /* parse_odbc_bind_variables */


/*
 * ODBCBindCMD - This function implements the "ns_odbc_bind" Tcl command 
 * installed into each interpreter of each virtual server.  It provides 
 * for the parsing and substitution of bind variables into the original 
 * sql query.  This is an emulation only. Postgresql doesn't currently 
 * support true bind variables yet.
 */

static int
ODBCBindCmd(ClientData UNUSED(clientData), Tcl_Interp *interp, int argc, const char *argv[]) {

  string_list_elt_t *bind_variables;
  string_list_elt_t *var_p;
  string_list_elt_t *sql_fragments;
  string_list_elt_t *frag_p;
  Ns_DString         ds;
  Ns_DbHandle       *handle;
  Ns_Set            *rowPtr;
  Ns_Set            *set   = NULL; 
  const char        *cmd;
  char              *sql;
  const char        *value = NULL, *p;

  if (argc < 4 || (!STREQ("-bind", argv[3]) && (argc != 4)) || 
       (STREQ("-bind", argv[3]) && (argc != 6))) {
    return BadArgs(interp, argv, "dbId sql");
  }

  if (Ns_TclDbGetHandle(interp, argv[2], &handle) != TCL_OK) {
    return TCL_ERROR;
  }

  Ns_DStringFree(&handle->dsExceptionMsg);
  handle->cExceptionCode[0] = '\0';

  /*
   * Make sure this is a PostgreSQL handle before accessing
   * handle->connection as an NsPgConn.
   */
  if (Ns_DbDriverName(handle) != odbcName) {
    Tcl_AppendResult(interp, "handle \"", argv[1], "\" is not of type \"",
		     odbcName, "\"", NULL);
    return TCL_ERROR;
			    }
  cmd = argv[1];

  if (STREQ("-bind", argv[3])) {
    set = Ns_TclGetSet(interp, argv[4]);
    if (set == NULL) {
      Tcl_AppendResult (interp, "invalid set id `", argv[4], "'", NULL);
      return TCL_ERROR;	      
    }
    sql = ns_strdup(argv[5]);
  } else {
    sql = ns_strdup(argv[3]);
  }

  /*
   * Parse the query string and find the bind variables.  Return
   * the sql fragments so that the query can be rebuilt with the 
   * bind variable values interpolated into the original query.
   */

  parse_odbc_bind_variables(sql, &bind_variables, &sql_fragments);  

  if (string_list_len(bind_variables) > 0) {

    Ns_DStringInit(&ds);

    /*
     * Rebuild the query and substitute the actual tcl variable values
     * for the bind variables.
     */

    for (var_p = bind_variables, frag_p = sql_fragments; 
         var_p != NULL || frag_p != NULL;) {
    
      if (frag_p != NULL) {
        Ns_DStringAppend(&ds, frag_p->string);
        frag_p = frag_p->next;
      }
   
      if (var_p != NULL) {
        if (set == NULL) {
          value = Tcl_GetVar(interp, var_p->string, 0);
        } else {
          value = Ns_SetGet(set, var_p->string);
        }
        if (value == NULL) {
          Tcl_AppendResult (interp, "undefined variable `", var_p->string,
                            "'", NULL);
          Ns_DStringFree(&ds);
          string_list_free_list(bind_variables);
          string_list_free_list(sql_fragments);
          ns_free(sql);
          return TCL_ERROR;
        }

        if ( strlen(value) == 0 ) {
            /*
             * DRB: If the Tcl variable contains the empty string, pass a NULL
             * as the value.
             */
            Ns_DStringAppend(&ds, "NULL");
        } else {
            /*
             * DRB: We really only need to quote strings, but there is one benefit
             * to quoting numeric values as well.  A value like '35 union select...'
             * substituted for a legitimate value in a URL to "smuggle" SQL into a
             * script will cause a string-to-integer conversion error within Postgres.
             * This conversion is done before optimization of the query, so indices are
             * still used when appropriate.
             */
            Ns_DStringAppend(&ds, "'");       

            /*
             * DRB: Unfortunately, we need to double-quote quotes as well ... and
             * escape backslashes
             */ 
            for (p = value; *p; p++) {
                if (*p == '\'') {
                    if (p > value) {
                        Ns_DStringNAppend(&ds, value, (int)(p-value));
                    }
                    value = p;
                    Ns_DStringAppend(&ds, "'");
                } else if (*p == '\\') {
                    if (p > value) {
		        Ns_DStringNAppend(&ds, value, (int)(p-value));
                    }
                    value = p;
                    Ns_DStringAppend(&ds, "\\");
                }
            }

            if (p > value) {
                Ns_DStringAppend(&ds, value);
            }

            Ns_DStringAppend(&ds, "'");       
        }
        var_p = var_p->next;
      }
    }
  
    ns_free(sql);
    sql = Ns_DStringExport(&ds);
    Ns_DStringFree(&ds);
  }

  string_list_free_list(bind_variables);
  string_list_free_list(sql_fragments);

  if (STREQ(cmd, "dml")) {
    if (Ns_DbDML(handle, sql) != NS_OK) {
      return DbFail(interp, handle, cmd, sql);
    }
  } else if (STREQ(cmd, "1row")) {
    rowPtr = Ns_Db1Row(handle, sql);
    if (rowPtr == NULL) {
      return DbFail(interp, handle, cmd, sql);
    }
    Ns_TclEnterSet(interp, rowPtr, 1);

  } else if (STREQ(cmd, "0or1row")) {
    int nrows;

    rowPtr = Ns_Db0or1Row(handle, sql, &nrows);
    if (rowPtr == NULL) {
      return DbFail(interp, handle, cmd, sql);
    }
    if (nrows == 0) {
      Ns_SetFree(rowPtr);
    } else {
      Ns_TclEnterSet(interp, rowPtr, 1);
    }

  } else if (STREQ(cmd, "select")) {
    rowPtr = Ns_DbSelect(handle, sql);
    if (rowPtr == NULL) {
      return DbFail(interp, handle, cmd, sql);
    }
    Ns_TclEnterSet(interp, rowPtr, 0);

  } else if (STREQ(cmd, "exec")) {
    switch (Ns_DbExec(handle, sql)) {
    case NS_DML:
      Tcl_SetObjResult(interp, Tcl_NewStringObj("NS_DML", 6));
      break;
    case NS_ROWS:
      Tcl_SetObjResult(interp, Tcl_NewStringObj("NS_ROWS", 7));
      break;
    default:
      return DbFail(interp, handle, cmd, sql);
      break;
    }

  } else {
    return DbFail(interp, handle, cmd, sql);    
  } 
  ns_free(sql);

  return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * ODBCServerInit -
 *
 *	Initialize the server when nsdb is ready.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Adds ns_odbc command.
 *
 *----------------------------------------------------------------------
 */

static Ns_ReturnCode
AddCmds(Tcl_Interp *interp, const void *UNUSED(arg))
{
    Tcl_CreateCommand(interp, "ns_odbc", ODBCCmd, NULL, NULL);
    Tcl_CreateCommand(interp, "ns_odbc_bind", ODBCBindCmd, NULL, NULL);
    return NS_OK;
}

Ns_ReturnCode
ODBCServerInit(const char *server, const char *UNUSED(module), const char *UNUSED(driver))
{
    return Ns_TclRegisterTrace(server, AddCmds, NULL, NS_TCL_TRACE_CREATE);
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCShutdown -
 *
 *	Callback to clean up driver on server shutdown.
 *
 * Results:
 *	Resources are freed.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static void
ODBCShutdown(const Ns_Time *UNUSED(timeoutPtr), void *arg)
{
    HENV henv = arg;
    RETCODE         rc;

    rc = SQLFreeEnv(henv);
    /*return RC_OK(rc) ? NS_OK : NS_ERROR;*/
}



/*
 *----------------------------------------------------------------------
 *
 * ODBCName -
 *
 *	Return the ODBC driver name.
 *
 * Results:
 *	Pointer to static string name.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static const char *
ODBCName(void)
{
    return odbcName;
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCOpenDb -
 *
 *	Open an ODBC datasource.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Performs both SQLAllocConnect and SQLConnect.
 *
 *----------------------------------------------------------------------
 */

static int
ODBCOpenDb(Ns_DbHandle *handle)
{
    SQLHDBC         hdbc;
    RETCODE         rc;

    assert(handle != NULL);
    assert(handle->datasource != NULL);

    handle->connected = NS_FALSE;
    handle->connection = NULL;
    handle->statement = NULL;

    rc = SQLAllocConnect(odbcenv, &hdbc);
    handle->connection = (void *) hdbc;
    ODBCLog(rc, handle);
    if (!RC_OK(rc)) {
        handle->connection = NULL;
        SQLFreeConnect(hdbc);
        return NS_ERROR;
    }
    Ns_Log(Notice, "%s[%s]: attemping to open '%s'",
	   handle->driver, handle->poolname, handle->datasource);
    rc = SQLConnect(hdbc,
                    (SQLCHAR *)handle->datasource, SQL_NTS,
                    (SQLCHAR *)handle->user, SQL_NTS,
                    (SQLCHAR *)handle->password, SQL_NTS);
    ODBCLog(rc, handle);
    if (!SQL_SUCCEEDED(rc)) {
        handle->connection = NULL;
        SQLFreeConnect(hdbc);
        return NS_ERROR;
    }
    handle->connection = (void *) hdbc;
    handle->connected = NS_TRUE;
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCCloseDb -
 *
 *	Close an ODBC datasource.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Performs both SQLDisconnect and SQLFreeConnect.
 *
 *----------------------------------------------------------------------
 */

static int
ODBCCloseDb(Ns_DbHandle *handle)
{
    RETCODE         rc;
    SQLHDBC         hdbc;

    hdbc = (SQLHDBC) handle->connection;
    handle->connection = NULL;
    handle->connected = NS_FALSE;

    rc = SQLDisconnect(hdbc);
    if (!RC_OK(rc)) {
        return NS_ERROR;
    }
    rc = SQLFreeConnect(hdbc);
    if (!RC_OK(rc)) {
        return NS_ERROR;
    }
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCExec -
 *
 *	Send an SQL statement.
 *
 * Results:
 *	NS_DML, NS_ROWS, or NS_ERROR.
 *
 * Side effects:
 *	Database may be modified or rows may be waiting.
 *
 *----------------------------------------------------------------------
 */

static int
ODBCExec(Ns_DbHandle *handle, char *sql)
{
    HSTMT           hstmt;
    RETCODE         rc;
    int             status;
    short           numcols;

    /*
     * Allocate a new statement.
     */

    rc = SQLAllocStmt((SQLHDBC) handle->connection, &hstmt);
    ODBCLog(rc, handle);
    if (!RC_OK(rc)) {
        return NS_ERROR;
    }

    /*
     * Send the SQL and determine if rows are available.
     */

    handle->statement = hstmt;
    rc = SQLExecDirect(hstmt, (SQLCHAR *)sql, SQL_NTS);
    ODBCLog(rc, handle);
    if (RC_OK(rc)) {
        rc = SQLNumResultCols(hstmt, &numcols);
        ODBCLog(rc, handle);
        if (RC_OK(rc)) {
            if (numcols != 0) {
                handle->fetchingRows = 1;
                status = NS_ROWS;
            } else {
                status = NS_DML;
            }
        }
    }

    /* 
     * Free the statement unless rows are waiting.
     */

    if (!RC_OK(rc)) {
        status = NS_ERROR;
    }
    if (status != NS_ROWS && ODBCFreeStmt(handle) != NS_OK) {
        status = NS_ERROR;
    }
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCBindRow -
 *
 *	Bind row after an ODBCExec which returned NS_ROWS.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Given Ns_Set is modified with column names.
 *
 *----------------------------------------------------------------------
 */

static Ns_Set  *
ODBCBindRow(Ns_DbHandle *handle)
{
    HSTMT           hstmt;
    RETCODE         rc;
    Ns_Set         *row;
    short           numcols;
    SQLUSMALLINT    i;
    char            colname[100];
    short           cbcolname;
    SWORD           sqltype, ibscale, nullable;
    SQLULEN         cbcoldef;

    if (!handle->fetchingRows) {
        Ns_Log(Error, "%s[%s]: no waiting rows",
	       handle->driver, handle->poolname);
        return NULL;
    }
    row = handle->row;
    hstmt = (HSTMT) handle->statement;
    rc = SQLNumResultCols(hstmt, &numcols);
    for (i = 1; RC_OK(rc) && i <= numcols; i++) {
        rc = SQLDescribeCol(hstmt, i,
                            (SQLCHAR *)colname, sizeof(colname),
                            &cbcolname, &sqltype, &cbcoldef, &ibscale, &nullable);
        ODBCLog(rc, handle);
	if (RC_OK(rc)) {
	    Ns_SetPut(row, colname, NULL);
	}
    }
    if (!RC_OK(rc)) {
	ODBCFreeStmt(handle);
	row = NULL;
    }
    return row;
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCGetRow -
 *
 *	Fetch the next row.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Given Ns_Set is modified with new values.
 *
 *----------------------------------------------------------------------
 */

static int
ODBCGetRow(Ns_DbHandle *handle, Ns_Set *row)
{
    SQLRETURN         	rc1;
    SQLRETURN 		rc2 = 0;
    SQLCHAR            *datum;
    SQLUSMALLINT      	i;
    SQLHSTMT           	hstmt;
    SQLSMALLINT     	mdknumcols;
    SQLLEN		cbvalue;

    if (!handle->fetchingRows) {
        Ns_Log(Error, "%s[%s]: no waiting rows",
	       handle->driver, handle->poolname);
        return NS_ERROR;
    }
    hstmt = (SQLHSTMT) handle->statement;
    rc1 = SQLNumResultCols(hstmt, &mdknumcols);
    ODBCLog(rc1, handle);    
    datum = (SQLCHAR*)ns_malloc(4096);
    if (!RC_OK(rc1)) {
	goto error;
    }
    if (mdknumcols != (SQLSMALLINT)Ns_SetSize(row)) {
	Ns_Log(Error, "%s[%s]: mismatched number of rows",
	       handle->driver, handle->poolname);
	goto error;
    }
    rc2 = SQLFetch(hstmt);
    ODBCLog(rc2, handle);
    if (rc2 == SQL_NO_DATA_FOUND) {
	ODBCFreeStmt(handle);
	return NS_END_DATA;
    }
    for (i = 1; RC_OK(rc2) && i <= mdknumcols; i++) {

	rc2 = SQLGetData(hstmt, i, SQL_C_CHAR, (SQLPOINTER)datum, 4096, &cbvalue);
	ODBCLog(rc2, handle);
	if (RC_OK(rc2)) {
	    Ns_SetPutValue(row, i - 1, cbvalue == SQL_NULL_DATA ? "" : (char*)datum);
	}
    }
    if (!RC_OK(rc2)) {
error:
	ODBCFreeStmt(handle);
	ns_free(datum);
	return NS_ERROR;
    }
	ns_free(datum);
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCCancel -
 *
 *	Cancel the active select.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Rows are flushed.
 *
 *----------------------------------------------------------------------
 */

static int
ODBCCancel(Ns_DbHandle *handle)
{
    RETCODE         rc;
    int		    status;

    status = NS_OK;
    if (handle->fetchingRows) {
        rc = SQLCancel((HSTMT) handle->statement);
        ODBCLog(rc, handle);
	status = ODBCFreeStmt(handle);
    }
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCCmd -
 *
 *	Process the ns_odbc command.
 *
 * Results:
 *	Standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int
ODBCCmd(ClientData UNUSED(clientData), Tcl_Interp *interp, int argc, const char *argv[])
{
    Ns_DbHandle    *handle;
    RETCODE         rc;
    char            buf[MAX_IDENTIFIER];
    SWORD FAR       cbInfoValue;

    if (argc != 3) {
        Tcl_AppendResult(interp, "wrong # args: should be \"",
            argv[0], " cmd handle\"", NULL);
        return TCL_ERROR;
    }
    if (Ns_TclDbGetHandle(interp, argv[2], &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * Make sure this is an open ODBC handle.
     */

    if (Ns_DbDriverName(handle) != odbcName) {
        Tcl_AppendResult(interp, "handle \"", argv[1], "\" is not of type \"",
            odbcName, "\"", NULL);
        return TCL_ERROR;
    }

    if (handle->connection == NULL) {
        Tcl_AppendResult(interp, "handle \"", argv[1], "\" not connected", NULL);
        return TCL_ERROR;
    }

    if (STREQ(argv[1], "dbmsname")) {
        rc = SQLGetInfo((SQLHDBC) handle->connection, SQL_DBMS_NAME, buf, sizeof(buf), &cbInfoValue);
        ODBCLog(rc, handle);
        if (!RC_OK(rc)) {
            Tcl_SetObjResult(interp, Tcl_NewStringObj("could not determine dbmsname", -1));
            return TCL_ERROR;
        }
    } else if (STREQ(argv[1], "dbmsver")) {
        rc = SQLGetInfo((SQLHDBC) handle->connection, SQL_DBMS_VER, buf, sizeof(buf), &cbInfoValue);
        ODBCLog(rc, handle);
        if (!RC_OK(rc)) {
            Tcl_SetObjResult(interp, Tcl_NewStringObj("could not determine dbmsver", -1));
            return TCL_ERROR;
        }
    } else {
        Tcl_AppendResult(interp, "unknown command \"", argv[1],
            "\": should be dbmsname or dbmsver.", NULL);
        return TCL_ERROR;
    }

    Tcl_SetResult(interp, buf, TCL_VOLATILE);
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ODBCLog -
 *
 *	Log a possible error.
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	May add log entry.
 *
 *----------------------------------------------------------------------
 */

static void
ODBCLog(RETCODE rc, Ns_DbHandle *handle)
{
    SQLHDBC         hdbc;
    SQLHSTMT        hstmt;
    Ns_LogSeverity  severity;
    SQLCHAR         szSQLSTATE[6];
    SDWORD          nErr;
    SQLCHAR         msg[MAX_ERROR_MSG + 1];
    SWORD           cbmsg;

    if (rc == SQL_SUCCESS_WITH_INFO) {
        severity = Warning;
    } else if (rc == SQL_ERROR) {
        severity = Error;
    } else {
        return;
    }
    hdbc = (SQLHDBC) handle->connection;
    hstmt = (SQLHSTMT) handle->statement;
    while (SQLError(odbcenv, hdbc, hstmt, szSQLSTATE, &nErr, msg,
            sizeof(msg), &cbmsg) == SQL_SUCCESS) {
        Ns_Log(severity, "%s[%s]: odbc message: "
	       "SQLSTATE = %s, Native err = %d, msg = '%s'",
	       handle->driver, handle->poolname, szSQLSTATE, nErr, msg);
        strcpy(handle->cExceptionCode, (const char *)szSQLSTATE);
        Ns_DStringFree(&(handle->dsExceptionMsg));
        Ns_DStringAppend(&(handle->dsExceptionMsg), (const char *)msg);
    }
}

/*
 *----------------------------------------------------------------------
 *
 * ODBCFreeStmt -
 *
 *	Free the handle's current statement.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int
ODBCFreeStmt(Ns_DbHandle *handle)
{
    RETCODE rc;

    rc = SQLFreeStmt((SQLHSTMT) handle->statement, SQL_DROP);
    handle->statement = NULL;
    handle->fetchingRows = 0;
    if (!RC_OK(rc)) {
	return NS_ERROR;
    }
    return NS_OK;
}


/*
 * Local Variables:
 * mode: c
 * c-basic-offset: 4
 * fill-column: 78
 * indent-tabs-mode: nil
 * End:
 */
