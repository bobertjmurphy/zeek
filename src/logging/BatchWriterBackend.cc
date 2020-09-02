// See the file "COPYING" in the main distribution directory for copyright.

#include "BatchWriterBackend.h"

logging::BatchWriterBackend::BatchWriterBackend(WriterFrontend* arg_frontend) :
BaseWriterBackend(arg_frontend), m_no_fatal_errors(true)
{
    
}

logging::BatchWriterBackend::~BatchWriterBackend()
{
    // Deallocate any batched values
    UNIMPLEMENTED
}

bool logging::BatchWriterBackend::WriteLogs(size_t num_writes, threading::Value*** vals)
{
    if (m_no_fatal_errors) {
		// Cache the underlying log records, and delete the top level of vals
		if (num_writes > 0) {
			m_cached_log_records.insert(m_cached_log_records.end(), vals, vals + num_writes);
			delete [] vals;
		}
		
		// If needed, write a batch
		WriteBatchIfNeeded();
    }
    
	return m_no_fatal_errors;
    
#if OLD
    // Do the write
    WriteErrorInfoVector errors = this->BatchWrite(num_writes, vals);
    
    // Delete vals
    DeleteVals(num_writes, vals);
    
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
#endif
}

bool logging::BatchWriterBackend::RunHeartbeat(double network_time, double current_time)
    {
    if (m_no_fatal_errors)
		{
		WriteBatchIfNeeded();
		}
		
	return m_no_fatal_errors;
    }

void logging::BatchWriterBackend::SendStats() const
    {
    /// \todo Fill me in
    }

void logging::BatchWriterBackend::WriteBatchIfNeeded()
{
    UNIMPLEMENTED
}
