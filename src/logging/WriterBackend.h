// See the file "COPYING" in the main distribution directory for copyright.
//
// Bridge class between main process and writer threads.

#pragma once

#include "BaseWriterBackend.h"

namespace broker { class data; }

namespace logging  {

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

        
    protected:
    
    /**
     * Writer-specific output method implementing recording of one log
     * entry.
     *
     * A non-batching writer implementation must override this method. If it
     * returns false, Zeek will assume that a fatal error has occured that
     * prevents the writer from further operation; the writer will then be
     * disabled and eventually deleted. When returning false, an
     * implementation should also call Error() to indicate what happened.
     */
    virtual bool DoWrite(int num_fields, const threading::Field* const*  fields,
                 threading::Value** vals) = 0;

    /**
     * This class's non-batching implementation of WriteLogs
     *
     * @param num_writes: The number of log records to be written with
     * this call.
     *
     * @param vals: An array of size \a num_fields *  \a num_writes with the
     * log values. Within each group of \a num_fields values, their types
     * must match with the field passed to Init(). The method takes ownership
     * of \a vals.
     *
     * @return The number of log records written. If this is not the same
     * as num_writes, an implementation should also call Error() to
     * indicate what happened.
     */
    virtual int WriteLogs(int num_writes, threading::Value*** vals) override final;
};


}
