// See the file "COPYING" in the main distribution directory for copyright.

#include "BatchWriterBackend.h"

logging::BatchWriterBackend::BatchWriterBackend(WriterFrontend* arg_frontend) :
BaseWriterBackend(arg_frontend), m_no_fatal_errors(true)
{
	// Determine configuration values
	std::string scratch;
	
	// Get the maximum number of records
	scratch = this->GetConfigString("batch:max_records");
	m_max_batch_records = std::stoull(scratch);
	
	// Get the maximum number of seconds between flushes, and make sure it's not negative
	scratch = this->GetConfigString("batch:max_delay_secs");
	m_max_batch_delay_seconds = std::max(std::stod(scratch), 0.0);
}

logging::BatchWriterBackend::~BatchWriterBackend()
{
    // Deallocate any cached records
	size_t cached_record_count = m_cached_log_records.size();
	DeleteCachedLogRecords(0, cached_record_count);
}

logging::BaseWriterBackend::WriterInfo::config_map logging::BatchWriterBackend::GetDefaultConfigMap() const
	{
		// Start off with this class's default values
		BaseWriterBackend::WriterInfo::config_map result =
			{
				{   "batch:max_records",          "0"     },	// Indefinite
				{   "batch:max_delay_secs",       "1"     },
			};
		
		// Merge in values from the superclass, but in a way that doesn't overwrite any key/value
		// pairs for which the key is already in result
		BaseWriterBackend::WriterInfo::config_map superclass_map = BaseWriterBackend::GetDefaultConfigMap();
		result.merge(superclass_map);
		
		return result;
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
		 
void logging::BatchWriterBackend::DeleteCachedLogRecords(size_t first_index, size_t n_records)
	{
		// Clamp the values to reasonable numbers
		first_index = std::min(first_index, m_cached_log_records.size());
		size_t after_last_index = std::min(first_index + n_records, m_cached_log_records.size());
		n_records = after_last_index - first_index;
		if (n_records == 0) {
			return;
		}
		
		// Delete the vals associated with the pointers to the records
		int num_fields = this->NumFields();
		for (size_t j = first_index; j <= after_last_index; ++j) {
			for ( int i = 0; i < num_fields; i++ )
				delete m_cached_log_records[j][i];

			delete [] m_cached_log_records[j];
		}
		
		// Remove the pointers from m_cached_log_records
		m_cached_log_records.erase(m_cached_log_records.begin() + first_index,
								   m_cached_log_records.begin() + after_last_index);
	}


void logging::BatchWriterBackend::WriteBatchIfNeeded()
	{
		assert(m_no_fatal_errors);
		UNIMPLEMENTED
	}
