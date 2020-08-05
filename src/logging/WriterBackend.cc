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


int logging::WriterBackend::WriteLogs(int num_writes, threading::Value*** vals)
{
    // Exit early if nothing is to be written
    if (num_writes == 0) {
        return 0;
    }
    
    // Repeatedly call DoWrite()
    int result = 0;
    bool success = true;
    int num_fields = this->NumFields();
    const threading::Field* const *fields = this->Fields();
    for ( int j = 0; j < num_writes && success; j++ )
        {
        // Try to write to the normal destination
        success = DoWrite(num_fields, fields, vals[j]);
        if (success)
            {
            result++;
            }
        }
    return result;
}

