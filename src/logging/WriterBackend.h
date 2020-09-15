// See the file "COPYING" in the main distribution directory for copyright.
//
// Bridge class between main process and writer threads.

#pragma once

#include "BaseWriterBackend.h"

namespace logging
{


/**
 * Base class for non-batch writer implementation. When the logging::Manager creates
 * a new logging filter, it instantiates a WriterFrontend. That then in turn
 * creates a WriterBackend of the right type. The frontend then forwards
 * messages over the backend as its methods are called.
 *
 * All of this methods must be called only from the corresponding child
 * thread (the constructor and destructor are the exceptions.)
 */
class WriterBackend : public BaseWriterBackend
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
		explicit WriterBackend(WriterFrontend* frontend);

		/**
		 * Indicates the thread should finish its operations. Calls DoFinish() in the subclasses
		 */
		bool OnFinish(double network_time) override;

	protected:

		/**
		 * Writer-specific output method implementing recording of one log
		 * entry.
		 *
		 * A non-batching writer implementation must override this method.
		 *
		 * Unlike previous Zeek versions, if this returns false, Zeek will NOT always
		 * assume that a fatal error has occured, and WON'T always disable and delete
		 * the log writer.
		 *
		 * Instead, if this returns false, the log writer will report that via the stats
		 * system, and keep running. Zeek components plug-ins may monitor those reports,
		 * and shut down the writer based on their own criteria.
		 */
		virtual bool DoWrite(int num_fields, const threading::Field* const*  fields,
		                     threading::Value** vals) = 0;

		/**
		 * This class's non-batching implementation of WriteLogs. See the prototype in the
		 * BaseWriterBackend implementation for details.
		 */
		virtual WriteLogsResult WriteLogs(size_t num_writes, threading::Value*** vals) override final;
	};

}
