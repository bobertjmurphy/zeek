// See the file "COPYING" in the main distribution directory for copyright.
//
// Bridge class between main process and writer threads.

#pragma once

#include "BaseWriterBackend.h"

#include <vector>
#include <deque>

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

        
protected:
    
    /**
     * A FIFO queue for caching and transmitting a sequence of log records. 
     */
    typedef std::deque<threading::Value**> LogRecordBatch;
    
    /**
     * Batch writers use this struct to report a problem that prevented sending a contiguous range
     * of log records.
     */
    struct WriteErrorInfo
        {
        /**
         * Constructor for creating a WriteErrorInfo in one line
         */
        WriteErrorInfo(size_t idx, size_t cnt, const std::string& desc, bool fatal = false) :
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
     * This class's batching implementation of WriteLogs
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
    virtual bool WriteLogs(size_t num_writes, threading::Value*** vals) override final;
    
    /**
     * Regulatly triggered for execution in the child thread.
     *
     * network_time: The network_time when the heartbeat was trigger by
     * the main thread.
     *
     * current_time: Wall clock when the heartbeat was trigger by the
     * main thread.
     *  
     * @return true if the thread should continue, false if it should terminate.
     */
    virtual bool RunHeartbeat(double network_time, double current_time) override final;
    
    /**
     * Sends statistics wherever they need to go.
     */
    virtual void SendStats() const override;
    
private:
    
    /** 
     * A place to cache records until they can be sent in a batch
     */
    LogRecordBatch  m_cached_log_records;
    
    /** 
     * If the batch transmission criteria have been met, send all cached records at once
     */
    void WriteBatchIfNeeded();
    
    /** 
     * Since requests to write are not synchronous with the calls that determine whether
     * a fatal error has occurred, this keeps track of that.
     */
    bool m_no_fatal_errors;
};


}
