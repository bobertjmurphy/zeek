// See the file "COPYING" in the main distribution directory for copyright.

#include <broker/data.hh>

#include "util.h"
#include "threading/SerialTypes.h"

#include "Manager.h"
#include "WriterBackend.h"
#include "WriterFrontend.h"


logging::WriterBackend::WriterBackend(WriterFrontend* arg_frontend) : BaseWriterBackend(arg_frontend)
{
}


bool logging::WriterBackend::WriteLogs(int num_writes, threading::Value*** vals)
{
    // Exit early if nothing is to be written
    if (num_writes == 0) {
        return 0;
    }
    
    // Repeatedly call DoWrite()
    bool no_fatal_errors = true;
    int num_fields = this->NumFields();
    const threading::Field* const *fields = this->Fields();
    for ( int j = 0; j < num_writes && no_fatal_errors; j++ )
        {
        // Try to write to the normal destination
        bool success = DoWrite(num_fields, fields, vals[j]);
        if (!success)
            {
            no_fatal_errors = false;
            }
        }
        
    // This has been moved from the caller in the superclass
    DeleteVals(num_writes, vals);

    return no_fatal_errors;
}

