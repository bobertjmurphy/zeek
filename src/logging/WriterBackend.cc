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

	return DoFinish(network_time);		// Implemented by the writers
	}

bool logging::WriterBackend::WriteLogs(size_t num_writes, threading::Value*** vals)
	{
	// Exit early if nothing is to be written
	if (num_writes == 0)
		{
		return true;		// No fatal errors
		}

	// Get necessary values
	int num_fields = this->NumFields();
	const threading::Field* const *fields = this->Fields();
	assert(num_fields > 0 && fields != nullptr);

	// Repeatedly call DoWrite()
	bool no_fatal_errors = true;
	int j = 0;
	for ( ; j < num_writes; j++ )
		{
		// Try to write to the normal destination
		bool success = DoWrite(num_fields, fields, vals[j]);

		// Handle any failures
		if ( ! success )
			{
			no_fatal_errors = HandleWriteErrors(j, num_writes, vals);
			break;
			}
		}

	// Delete vals
	DeleteVals(num_writes, vals);

	// Report statistics
	ReportWriteStatistics(num_writes, j);

	return no_fatal_errors;
	}

bool logging::WriterBackend::RunHeartbeat(double network_time, double current_time)
	{
	return true;
	}
