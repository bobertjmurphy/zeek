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
        return true;		// No fatal errors
    }

    int num_fields = this->NumFields();
    const threading::Field* const *fields = this->Fields();
	int num_written = this->DoWriteLogs(num_fields, num_writes,
									    fields, vals);
	bool no_fatal_errors = (num_writes == num_written);
    return no_fatal_errors;
}

int logging::WriterBackend::DoWriteLogs(int num_fields, int num_writes,
                               const threading::Field* const* fields,
                               threading::Value*** vals)
{
    // Validate the arguments
    assert(num_fields > 0 && num_writes >= 0);
    
    // Exit early if nothing is to be written
    if (num_writes == 0) {
        return 0;
    }
    
    // Repeatedly call DoWrite()
    int result = 0;
    bool success = true;
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

bool logging::WriterBackend::RunHeartbeat(double network_time, double current_time)
{
	return true;
}

void logging::WriterBackend::SendStats() const
	{
	/// \todo Fill me in
	}
