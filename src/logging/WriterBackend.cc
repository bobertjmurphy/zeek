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

bool logging::WriterBackend::OnFinish(double network_time)
{
if ( Failed() )
	return true;

return DoFinish(network_time);		// Implemented by the writers
}

bool logging::WriterBackend::WriteLogs(size_t num_writes, threading::Value*** vals)
{
    // Exit early if nothing is to be written
    if (num_writes == 0) {
        return true;		// No fatal errors
    }

    // Get necessary values
    int num_fields = this->NumFields();
    const threading::Field* const *fields = this->Fields();
    assert(num_fields > 0 && fields != nullptr);
    
    // Repeatedly call DoWrite()
    int num_written = 0;
    bool success = true;
    for ( int j = 0; j < num_writes && success; j++ )
        {
        // Try to write to the normal destination
        success = DoWrite(num_fields, fields, vals[j]);

        if ( ! success )
            break;

        num_written++;
        }
    
    // Delete vals
    DeleteVals(num_writes, vals);
    
	bool no_fatal_errors = (num_writes == num_written);
    return no_fatal_errors;
}

bool logging::WriterBackend::RunHeartbeat(double network_time, double current_time)
{
	return true;
}

void logging::WriterBackend::SendStats() const
	{
	/// \todo Fill me in
	}
