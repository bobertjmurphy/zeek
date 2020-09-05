// See the file "COPYING" in the main distribution directory for copyright.
//
// Bridge class between main process and writer threads.

#pragma once

#include "threading/MsgThread.h"

#include "Component.h"

// Some help with development
#ifndef NDEBUG
#define UNIMPLEMENTED assert(false);
#else
#define UNIMPLEMENTED
#endif

namespace broker
{
class data;
}

namespace logging
{

class WriterFrontend;

/**
 * Base class for writer implementation. When the logging::Manager creates a
 * new logging filter, it instantiates a WriterFrontend. That then in turn
 * creates a BaseWriterBackend of the right type. The frontend then forwards
 * messages over the backend as its methods are called.
 *
 * All of this methods must be called only from the corresponding child
 * thread (the constructor and destructor are the exceptions.)
 */
class BaseWriterBackend : public threading::MsgThread
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
		explicit BaseWriterBackend(WriterFrontend* frontend);

		/**
		 * Destructor.
		 */
		~BaseWriterBackend() override;

		/**
		 * A struct passing information to the writer at initialization time.
		 */
		struct WriterInfo
			{
				// Structure takes ownership of these strings.
				typedef std::map<const char*, const char*, CompareString> config_map;

				/**
				 * A string left to the interpretation of the writer
				 * implementation; it corresponds to the 'path' value configured
				 * on the script-level for the logging filter.
				 *
				 * Structure takes ownership of string.
				 */
				const char* path;

				/**
				 * The rotation interval as configured for this writer.
				 */
				double rotation_interval;

				/**
				 * The parsed value of log_rotate_base_time in seconds.
				 */
				double rotation_base;

				/**
				 * The network time when the writer is created.
				 */
				double network_time;

				/**
				 * A map of key/value pairs corresponding to the relevant
				 * filter's "config" table.
				 */
				config_map config;

				WriterInfo() : path(0), rotation_interval(0.0), rotation_base(0.0),
					network_time(0.0)
					{
					}

				WriterInfo(const WriterInfo& other)
					{
					path = other.path ? copy_string(other.path) : 0;
					rotation_interval = other.rotation_interval;
					rotation_base = other.rotation_base;
					network_time = other.network_time;

					for ( config_map::const_iterator i = other.config.begin(); i != other.config.end(); i++ )
						config.insert(std::make_pair(copy_string(i->first), copy_string(i->second)));
					}

				~WriterInfo()
					{
					delete [] path;

					for ( config_map::iterator i = config.begin(); i != config.end(); i++ )
						{
						delete [] i->first;
						delete [] i->second;
						}
					}

				// Note, these need to be adapted when changing the struct's
				// fields. They serialize/deserialize the struct.
				broker::data ToBroker() const;
				bool FromBroker(broker::data d);

			private:
				const WriterInfo& operator=(const WriterInfo& other); // Disable.
			};

		/**
		 * One-time initialization of the writer to define the logged fields.
		 *
		 * @param num_fields The number of log fields for this stream
		 *
		 * @param fields An array of size \a num_fields with the log fields.
		 * The methods takes ownership of the array.
		 *
		 * @param frontend_name The name of the front-end writer implementation.
		 *
		 * @return False if an error occured.
		 */
		bool Init(int num_fields, const threading::Field* const* fields);

		/**
		 * Writes one or more log entries.
		 *
		 * @param num_fields: The number of log fields for this stream. The
		 * value must match what was passed to Init().
		 *
		 * @param num_writes: The number of log records to be written.
		 *
		 * @param vals: An array of size  \a num_writes * \a num_fields
		 * with the log values. The types of each group of \a num_fields
		 * values must match with the fields passed to Init(). The method
		 * takes ownership of \a vals..
		 *
		 * @return True if the writer should continue. False if a fatal error
		 * occurred, in which case the writer must not be used any further.
		 */
		bool Write(int num_fields, int num_writes, threading::Value*** vals);

		/**
		 * Sets the buffering status for the writer, assuming the writer
		 * supports that. (If not, it will be ignored).
		 *
		 * @param enabled False if buffering is to be disabled (by default
		 * it's on).
		 *
		 * @return False if an error occured.
		 */
		bool SetBuf(bool enabled);

		/**
		 * Flushes any currently buffered output, assuming the writer
		 * supports that. (If not, it will be ignored).
		 *
		 * @param network_time The network time when the flush was triggered.
		 *
		 * @return False if an error occured.
		 */
		bool Flush(double network_time);

		/**
		 * Triggers rotation, if the writer supports that. (If not, it will
		 * be ignored).
		 *
		 * @return False if an error occured.
		 */
		bool Rotate(const char* rotated_path, double open, double close, bool terminating);

		/**
		 * Disables the frontend that has instantiated this backend. Once
		 * disabled,the frontend will not send any further message over.
		 *
		 * TODO: Do we still need this method (and the corresponding message)?
		 */
		void DisableFrontend();

		/**
		 * Returns the additional writer information passed into the constructor.
		 */
		const WriterInfo& Info() const
			{
			return *info;
			}

		/**
		 * Returns the number of log fields as passed into the constructor.
		 */
		int NumFields() const
			{
			return num_fields;
			}

		/**
		 * Returns the log fields as passed into the constructor.
		 */
		const threading::Field* const * Fields() const
			{
			return fields;
			}

		/**
		 * Returns the current buffering state.
		 *
		 * @return True if buffering is enabled.
		 */
		bool IsBuf()
			{
			return buffering;
			}

		/**
		 * Signals that a file has been successfully rotated and any
		 * potential post-processor can now run.
		 *
		 * Most of the parameters should be passed through from DoRotate().
		 *
		 * Note: Exactly one of the two FinishedRotation() methods must be
		 * called by a writer's implementation of DoRotate() once rotation
		 * has finished.
		 *
		 * @param new_name The filename of the rotated file.
		 *
		 * @param old_name The filename of the original file.
		 *
		 * @param open: The timestamp when the original file was opened.
		 *
		 * @param close: The timestamp when the origina file was closed.
		 *
		 * @param terminating: True if the original rotation request occured
		 * due to the main Bro process shutting down.
		 */
		bool FinishedRotation(const char* new_name, const char* old_name,
		                      double open, double close, bool terminating);

		/**
		 * Signals that a file rotation request has been processed, but no
		 * further post-processing needs to be performed (either because
		 * there was an error, or there was nothing to rotate to begin with
		 * with this writer).
		 *
		 * Note: Exactly one of the two FinishedRotation() methods must be
		 * called by a writer's implementation of DoRotate() once rotation
		 * has finished.
		 */
		bool FinishedRotation();

		/**
		 * Overridden from MsgThread - calls DoHeartbeat() in the sublcasses.
		 */
		bool OnHeartbeat(double network_time, double current_time) override final;

		/**
		 * Gets a configuration string, using this order of precedence in case
		 * of overrides:
		 *  1. Value for a reader-writer combination, set in a Zeek script
		 *  2. Value for a reader independent of the writer, set in a Zeek script
		 *     May also be a global config value
		 *  3. Default value from the code
		 */
		std::string GetConfigString(const std::string& key) const;

		/**
		 * Gets a recognizable name for the frontend. For example, for
		 * packet_filter/Log::WRITER_ASCII, that would be "packet_filter".
		 */
		std::string GetFrontendName() const;

		/**
		 * Gets a recognizable name for the backend. For example, for
		 * packet_filter/Log::WRITER_ASCII, that would be "ascii".
		 */
		std::string GetBackendName() const;

		/**
		 * Gets a recognizable name for the frontend and backend. For example, for
		 * for packet_filter/Log::WRITER_ASCII, that would be
		 * "packet_filter:ascii".
		 */
		std::string FullName() const;

		// Let the compiler know that we are aware that there is a virtual
		// info function in the base.
		using MsgThread::Info;

	protected:
		friend class FinishMessage;

		/**
		 * Writer-specific intialization method.
		 *
		 * A writer implementation must override this method. If it returns
		 * false, it will be assumed that a fatal error has occured that
		 * prevents the writer from further operation; it will then be
		 * disabled and eventually deleted. When returning false, an
		 * implementation should also call Error() to indicate what happened.
		 */
		virtual bool DoInit(const WriterInfo& info, int num_fields,
		                    const threading::Field* const*  fields) = 0;

		/**
		 * Writer-specific method implementing a change of fthe buffering
		 * state.  If buffering is disabled, the writer should attempt to
		 * write out information as quickly as possible even if doing so may
		 * have a performance impact. If enabled (which is the default), it
		 * may buffer data as helpful and write it out later in a way
		 * optimized for performance. The current buffering state can be
		 * queried via IsBuf().
		 *
		 * A writer implementation must override this method but it can just
		 * ignore calls if buffering doesn't align with its semantics.
		 *
		 * If the method returns false, it will be assumed that a fatal error
		 * has occured that prevents the writer from further operation; it
		 * will then be disabled and eventually deleted. When returning
		 * false, an implementation should also call Error() to indicate what
		 * happened.
		 */
		virtual bool DoSetBuf(bool enabled) = 0;

		/**
		 * Writer-specific method implementing flushing of its output.
		 *
		 * A writer implementation must override this method but it can just
		 * ignore calls if flushing doesn't align with its semantics.
		 *
		 * If the method returns false, it will be assumed that a fatal error
		 * has occured that prevents the writer from further operation; it
		 * will then be disabled and eventually deleted. When returning
		 * false, an implementation should also call Error() to indicate what
		 * happened.
		 *
		 * @param network_time The network time when the flush was triggered.
		 */
		virtual bool DoFlush(double network_time) = 0;

		/**
		 * Writer-specific method implementing log rotation.  Most directly
		 * this only applies to writers writing into files, which should then
		 * close the current file and open a new one.  However, a writer may
		 * also trigger other apppropiate actions if semantics are similar.
		 * Once rotation has finished, the implementation *must* call
		 * FinishedRotation() to signal the log manager that potential
		 * postprocessors can now run.
		 *
		 * A writer implementation must override this method but it can just
		 * ignore calls if flushing doesn't align with its semantics. It
		 * still needs to call FinishedRotation() though.
		 *
		 * If the method returns false, it will be assumed that a fatal error
		 * has occured that prevents the writer from further operation; it
		 * will then be disabled and eventually deleted. When returning
		 * false, an implementation should also call Error() to indicate what
		 * happened.
		 *
		 * @param rotate_path Reflects the path to where the rotated output
		 * is to be moved, with specifics depending on the writer. It should
		 * generally be interpreted in a way consistent with that of \c path
		 * as passed into DoInit(). As an example, for file-based output, \c
		 * rotate_path could be the original filename extended with a
		 * timestamp indicating the time of the rotation.
		 *
		 * @param open The network time when the *current* file was opened.
		 *
		 * @param close The network time when the *current* file was closed.
		 *
		 * @param terminating Indicates whether the rotation request occurs
		 * due the main Bro prcoess terminating (and not because we've
		 * reached a regularly scheduled time for rotation).
		 */
		virtual bool DoRotate(const char* rotated_path, double open, double close,
		                      bool terminating) = 0;

		/**
		 * Writer-specific method called just before the threading system is
		 * going to shutdown. It is assumed that once this messages returns,
		 * the thread can be safely terminated.
		 *
		 * @param network_time The network time when the finish is triggered.
		 */
		virtual bool DoFinish(double network_time) = 0;
		/**
		 * Triggered by regular heartbeat messages from the main thread.
		 *
		 * This method can be overridden. Default implementation does
		 * nothing.
		 */
		virtual bool DoHeartbeat(double network_time, double current_time) = 0;

		/**
		 * Perform a "low-level" write request that actually tries to write one or
		 * more log records to the target.
		 *
		 * It must be implemented, and only implemented, by direct
		 * child classes of BaseWriterBackend, and its implementation is at the
		 * heart of the difference between a batching and non-batching writer.
		 *
		 * @param num_writes: The number of log records to be written with
		 * this call.
		 *
		 * @param vals: An array of size \a num_fields *  \a num_writes with the
		 * log values. Within each group of \a num_fields values, their types
		 * must match with the field passed to Init(). The method takes ownership
		 * of \a vals.
		 *
		 * @return true on no fatal errors, false on a fatal error. If there
		 * were any fatal errors, an implementation should also call Error() to
		 * indicate what happened, and the writer and its thread will eventually
		 * be terminated.
		 */
		virtual bool WriteLogs(size_t num_writes, threading::Value*** vals) = 0;

		/**
		 * Deletes the values as passed into Write().
		 */
		void DeleteVals(int num_writes, threading::Value*** vals);

		/**
		 * Sends statistics wherever they need to go.
		 */
		virtual void SendStats() const;


		/**
		 * Regularly triggered for execution in the child thread.
		 *
		 * network_time: The network_time when the heartbeat was trigger by
		 * the main thread.
		 *
		 * current_time: Wall clock when the heartbeat was trigger by the
		 * main thread.
		 *
		 * @return true if the thread should continue, false if it should terminate.
		 */
		virtual bool RunHeartbeat(double network_time, double current_time) = 0;

		virtual BaseWriterBackend::WriterInfo::config_map GetDefaultConfigMap() const;

		/**
		 * A FIFO queue for caching and transmitting a sequence of log records.
		 */
		typedef std::vector<threading::Value**> LogRecordBatch;

		/**
		 * Batch writers use this struct to report a problem that prevented sending a contiguous range
		 * of log records.
		 */
		struct WriteErrorInfo
			{
			/**
			 * Constructor for creating a WriteErrorInfo in one line
			 */
			WriteErrorInfo(size_t idx, size_t cnt, const std::string& desc, bool fatal) :
				first_record_index(idx), record_count(cnt), description(desc), is_fatal(fatal)
				{
				}

			/**
			 * The index of the first record in the range to which the description applies.
			 */
			size_t first_record_index;

			/**
			 * The number of the reecords in the range to which the description applies.
			 */
			size_t record_count;

			/**
			 * A text description of the problem. This may be logged, reported to an administrator,
			 * etc.
			 */
			std::string description;

			/**
			 * If this is false, the writer should continue running. If this is true, the writer
			 * will be shut down.
			 */
			bool is_fatal;
			};

		/**
		 * Batch writers use this to report problems sending zero or more ranges of log records.
		 */
		typedef std::vector<WriteErrorInfo> WriteErrorInfoVector;

		/**
		 * Non-batch writers call this to report errors when writing log records.
		 *
		 * @return true on no fatal errors, false on a fatal error.
		 */
		bool HandleWriteErrors(size_t error_log_index, size_t num_writes, threading::Value*** vals) const;

		/**
		 * Batch writers call this to report errors when writing log records.
		 *
		 * @return true on no fatal errors, false on a fatal error.
		 */
		bool HandleWriteErrors(const LogRecordBatch& records, const WriteErrorInfoVector& errors) const;

		/**
		 * Method allowing a writer to send a specified Zeek event. Vals must
		 * match the values expected by the Zeek event.
		 *
		 * @param event_name name of the Zeek event to send
		 *
		 * @param num_vals number of entries in \a vals
		 *
		 * @param vals the values to be given to the event
		 */
		void SendEvent(const char* event_name, const int num_vals, threading::Value* *vals);

		/**
		 * Varargs version of SendEvent
		 *
		 * @param event_name name of the Zeek event to send
		 *
		 * @param num_vals number of entries in list
		 *
		 * @param ... the values to be given to the event
		 */
		void VaSendEvent(const char* event_name, const int num_vals, ...);

	private:
		friend class Manager;

		// Frontend that instantiated us. This object must not be access from
		// this class, it's running in a different thread!
		WriterFrontend* frontend;

		const WriterInfo* info;    // Meta information.
		int num_fields;    // Number of log fields.
		const threading::Field* const*  fields;    // Log fields.
		bool buffering;    // True if buffering is enabled.

		int rotation_counter; // Tracks FinishedRotation() calls.

		std::string m_backend_name;

		// The default configuration map can be initialized inside GetConfigString(),
		// which is const
		mutable BaseWriterBackend::WriterInfo::config_map m_default_config_map;
		mutable bool m_default_config_map_inited;


		// Statistics
		size_t logs_successfully_written = 0;
	};


}
