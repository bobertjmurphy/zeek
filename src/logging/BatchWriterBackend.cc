// See the file "COPYING" in the main distribution directory for copyright.

#include "BatchWriterBackend.h"

logging::BatchWriterBackend::BatchWriterBackend(WriterFrontend* arg_frontend) :
BaseWriterBackend(arg_frontend), m_first_record_wallclock_time(0)
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
	if (num_writes > 0) {
		// Will this put the first record into the cache?
		if (m_cached_log_records.empty()) {
			m_first_record_wallclock_time = current_time(true);
		}
		
		// Cache the underlying log records, and delete the top level of vals
		m_cached_log_records.insert(m_cached_log_records.end(), vals, vals + num_writes);
		delete [] vals;
	}
	
	// If needed, write a batch
	bool no_fatal_errors = WriteBatchIfNeeded();
	return no_fatal_errors;
}

bool logging::BatchWriterBackend::RunHeartbeat(double network_time, double current_time)
    {
	// If needed, write a batch
	bool no_fatal_errors = WriteBatchIfNeeded();
	return no_fatal_errors;
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


bool logging::BatchWriterBackend::WriteBatchIfNeeded()
	{
		// Don't write anything if nothing is cached
		if (m_cached_log_records.empty()) {
			return true;		// No fatal errors
		}
		
		// Don't write if the write criteria haven't been met
		bool write_batch = false;
		if (m_max_batch_records != 0) {
			size_t record_count = m_cached_log_records.size();
			write_batch = (record_count >= m_max_batch_records);
		}
		if (!write_batch && (m_max_batch_delay_seconds > 0)) {
			double current_wallclock_time = current_time(true);
			double delay = current_wallclock_time - m_first_record_wallclock_time;
			write_batch = (delay >= m_max_batch_delay_seconds);
		}
		if (!write_batch) {
			return true;		// No fatal errors
		}
		
		// Do the write
		WriteErrorInfoVector errors = BatchWrite(m_cached_log_records);
		
		// Analyze any reported errors
		bool has_fatal_errors = false;
		for (const WriteErrorInfo& this_error: errors) {
			// Report any errors
			/// \todo Report the record count and description
			
			// Note any fatal errors
			has_fatal_errors |= this_error.is_fatal;
		}
		
		// Clear the cache and return
		size_t cached_record_count = m_cached_log_records.size();
		DeleteCachedLogRecords(0, cached_record_count);
		return !has_fatal_errors;
	}
