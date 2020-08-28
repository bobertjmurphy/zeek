// See the file "COPYING" in the main distribution directory for copyright.

#include "BatchWriterBackend.h"

logging::BatchWriterBackend::BatchWriterBackend(WriterFrontend* arg_frontend) : BaseWriterBackend(arg_frontend)
{
}

bool logging::BatchWriterBackend::WriteLogs(size_t num_writes, threading::Value*** vals)
{
    // Attempt the write
    WriteErrorInfoVector errors = this->DoWrite(num_writes, vals);
    
    // Handle any problems, and recognize any fatal errors
    size_t fatal_error_count = 0;
    for (const WriteErrorInfo& this_error : errors) {
        /// \todo Use first_record_index, description, and description to report problems
        
        // Keep track of fatal errors
        bool this_error_is_fatal = this_error.is_fatal;
        if (this_error_is_fatal) {
            fatal_error_count += 1;
        }
    }
    
    bool no_fatal_errors = (fatal_error_count == 0);
    return no_fatal_errors;
}

size_t logging::BatchWriterBackend::DoWriteLogs(size_t num_writes, threading::Value*** vals)
{
    
}

bool logging::BatchWriterBackend::RunHeartbeat(double network_time, double current_time)
{
    return true;
}

void logging::BatchWriterBackend::SendStats() const
    {
    /// \todo Fill me in
    }
