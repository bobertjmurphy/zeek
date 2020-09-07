// See the file "COPYING" in the main distribution directory for copyright.

#include <broker/data.hh>

#include "util.h"
#include "threading/SerialTypes.h"

#include "Manager.h"
#include "WriterBackend.h"
#include "WriterFrontend.h"


logging::WriterBackend::WriterBackend(WriterFrontend* arg_frontend) : BaseWriterBackend(arg_frontend)
	{
	}

bool logging::WriterBackend::OnFinish(double network_time)
	{
	if ( Failed() )
		return true;

	// Report final statistics
	SendStats();

	return DoFinish(network_time);		// Implemented by the writers
	}

bool logging::WriterBackend::WriteLogs(size_t num_writes, threading::Value*** vals)
	{
#if OLD
	// Exit early if nothing is to be written
	if (num_writes == 0)
		{
		return true;		// No fatal errors
		}

	size_t num_successful_writes = DoWriteLogs(num_writes, vals);
#else
		// Get necessary values
		int num_fields = this->NumFields();
		const threading::Field* const *fields = this->Fields();
		assert(num_fields > 0 && fields != nullptr);

		// Repeatedly call DoWrite()
		int num_successful_writes = 0;
		bool success = true;
		for ( size_t j = 0; j < num_writes && success; j++ )
			{
			// Try to write to the normal destination
			success = DoWrite(num_fields, fields, vals[j]);
			if (success)
				num_successful_writes++;
			}
#endif

	bool no_fatal_errors = true;
	if (num_successful_writes < num_writes)
		no_fatal_errors = HandleWriteErrors(num_successful_writes, num_writes, vals);

	// Delete vals
	DeleteVals(num_writes, vals);

	// Report statistics
	ReportWriteStatistics(num_writes, num_successful_writes);

	return no_fatal_errors;
	}

#if OLD
size_t logging::WriterBackend::DoWriteLogs(size_t num_writes, threading::Value*** vals)
	{
	// Get necessary values
	int num_fields = this->NumFields();
	const threading::Field* const *fields = this->Fields();
	assert(num_fields > 0 && fields != nullptr);

	// Repeatedly call DoWrite()
	int result = 0;
	bool success = true;
	for ( size_t j = 0; j < num_writes && success; j++ )
		{
		// Try to write to the normal destination
		success = DoWrite(num_fields, fields, vals[j]);
		if (success)
			result++;
		}
	return result;
	}
#endif

bool logging::WriterBackend::RunHeartbeat(double network_time, double current_time)
	{
	return true;
	}
