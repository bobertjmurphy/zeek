// See the file "COPYING" in the main distribution directory for copyright.
//
// Bridge class between main process and writer threads.

#pragma once

#include "BaseWriterBackend.h"

#include <vector>

namespace logging
{

class WriterFrontend;


/**
 * Base class for writer implementation. When the logging::Manager creates a
 * new logging filter, it instantiates a WriterFrontend. That then in turn
 * creates a WriterBackend of the right type. The frontend then forwards
 * messages over the backend as its methods are called.
 *
 * All of this methods must be called only from the corresponding child
 * thread (the constructor and destructor are the exceptions.)
 */
class BatchWriterBackend : public BaseWriterBackend
	{
	public:
		/**
		 * Constructor.
		 *
		 * @param frontend The frontend writer that created this backend. The
		 * *only* purpose of this value is to be passed back via messages as
		 * a argument to callbacks. One must not otherwise access the
		 * frontend, it's running in a different thread.
		 *
		 * @param name A descriptive name for writer's type (e.g., \c Ascii).
		 *
		 */
		explicit BatchWriterBackend(WriterFrontend* frontend);

		/**
		 * Destructor.
		 */
		~BatchWriterBackend() override;

		/**
		 * Indicates the thread should finish its operations. Calls DoFinish() in the subclasses
		 */
		bool OnFinish(double network_time) override;


	protected:

		/**
		 * Writer-specific output method implementing recording of zero or more log
		 * entry.
		 *
		 * A batching writer implementation must override this method.
		 *
		 * @param records_to_write: A queue of records to be written
		 *
		 * @return A vector of WriteErrorInfo structs, describing any write failures.
		 * If all writes succeeded, this must be an empty vector. Indices in the
		 * struct are relative to the beginning of records_to_write.
		 */
		virtual WriteErrorInfoVector BatchWrite(const LogRecordBatch &records_to_write) = 0;

		/**
		 * This class's batching implementation of WriteLogs. See the prototype in the
		 * BaseWriterBackend implementation for details.
		 */
		virtual WriteLogsResult WriteLogs(size_t num_writes, threading::Value*** vals) override final;

		/**
		 * Get a map from expected configuration variables to default values for those variables.
		 */
		virtual BaseWriterBackend::WriterInfo::config_map GetDefaultConfigMap() const override;


		/**
		 * Convenience exception and inline functions for errors that should shut down the writer
		 */
		class fatal_writer_error : public runtime_error
			{
			public:
				explicit fatal_writer_error(const string& __s) : runtime_error(__s) {}
				explicit fatal_writer_error(const char* __s)   : runtime_error(__s) {}
			};
		inline void throw_fatal_writer_error(const string& __s)
			{
			throw fatal_writer_error(__s);
			}
		inline void throw_fatal_writer_error_if(bool cond, const string& __s)
			{
			if (cond) throw fatal_writer_error(__s);
			}

		/**
		 * Convenience exception and inline functions for errors that shouldn't shut down the writer
		 */
		class non_fatal_writer_error : public runtime_error
			{
			public:
				explicit non_fatal_writer_error(const string& __s) : runtime_error(__s) {}
				explicit non_fatal_writer_error(const char* __s)   : runtime_error(__s) {}
			};
		inline void throw_non_fatal_writer_error(const string& __s)
			{
			throw non_fatal_writer_error(__s);
			}
		inline void throw_non_fatal_writer_error_if(bool cond, const string& __s)
			{
			if (cond) throw non_fatal_writer_error(__s);
			}

		/**
		 * Limits on how much the batching system can cache before flushing.
		 *
		 *	 These values are initialized from Zeek's standard configuration system
		 *	 during the BatchWriterBackend constructor, using the keys in the succeeding
		 *	 comments.
		 *
		 *	 A child class of BatchWriterBackend can control its own configuration by
		 *	 overwriting these values, either in its constructor, or later on.
		 */
		size_t  m_max_batch_records;      	// "batch:max_records", 0 indicates no limit
		double  m_max_batch_delay_seconds;  // "batch:max_delay_secs", 0 indicates no limit

	private:

		/**
		 * A place to cache records until they can be sent in a batch
		 */
		LogRecordBatch  m_cached_log_records;

		/**
		 * Remove records from the cache
		 */
		void DeleteCachedLogRecords(size_t first_index, size_t n_records);

		/**
		 * If the batch transmission criteria have been met, send all cached records at once
		 */
		bool WriteBatchIfNeeded(bool force_write, double current_wallclock_time);

		/**
		 * The time the first record currently in the cache was added.
		 */
		double m_first_cached_record_time;

	};


}
