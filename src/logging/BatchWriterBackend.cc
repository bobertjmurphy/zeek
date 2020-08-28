// See the file "COPYING" in the main distribution directory for copyright.

#include "BatchWriterBackend.h"

logging::BatchWriterBackend::BatchWriterBackend(WriterFrontend* arg_frontend) : BaseWriterBackend(arg_frontend)
{
}

bool logging::BatchWriterBackend::WriteLogs(int num_writes, threading::Value*** vals)
{
#if BOBERT
    // Exit early if nothing is to be written
    if (num_writes == 0) {
        return true;        // No fatal errors
    }

    int num_fields = this->NumFields();
    const threading::Field* const *fields = this->Fields();
    int num_written = this->DoWriteLogs(num_fields, num_writes,
                                        fields, vals);
    bool no_fatal_errors = (num_writes == num_written);
    return no_fatal_errors;
#else
    return true;
#endif
}

bool logging::BatchWriterBackend::RunHeartbeat(double network_time, double current_time)
{
    return true;
}

void logging::BatchWriterBackend::SendStats() const
    {
    /// \todo Fill me in
    }
