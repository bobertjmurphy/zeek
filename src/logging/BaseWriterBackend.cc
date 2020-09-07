// See the file "COPYING" in the main distribution directory for copyright.

#include <broker/data.hh>

#include "util.h"
#include "threading/SerialTypes.h"

#include "Manager.h"
#include "BaseWriterBackend.h"
#include "WriterFrontend.h"

// Messages sent from backend to frontend (i.e., "OutputMessages").

using threading::Value;
using threading::Field;

namespace logging
{

#pragma mark - Messages

class RotationFinishedMessage : public threading::OutputMessage<WriterFrontend>
	{
	public:
		RotationFinishedMessage(WriterFrontend* writer, const char* new_name, const char* old_name,
		                        double open, double close, bool success, bool terminating)
			: threading::OutputMessage<WriterFrontend>("RotationFinished", writer),
			  new_name(copy_string(new_name)), old_name(copy_string(old_name)), open(open),
			  close(close), success(success), terminating(terminating)    { }

		virtual ~RotationFinishedMessage()
			{
			delete [] new_name;
			delete [] old_name;
			}

		virtual bool Process()
			{
			return log_mgr->FinishedRotation(Object(), new_name, old_name, open, close, success, terminating);
			}

	private:
		const char* new_name;
		const char* old_name;
		double open;
		double close;
		bool success;
		bool terminating;
	};

class FlushWriteBufferMessage : public threading::OutputMessage<WriterFrontend>
	{
	public:
		FlushWriteBufferMessage(WriterFrontend* writer)
			: threading::OutputMessage<WriterFrontend>("FlushWriteBuffer", writer)    {}

		virtual bool Process()
			{
			Object()->FlushWriteBuffer();
			return true;
			}
	};

class DisableMessage : public threading::OutputMessage<WriterFrontend>
	{
	public:
		DisableMessage(WriterFrontend* writer)
			: threading::OutputMessage<WriterFrontend>("Disable", writer)    {}

		virtual bool Process()
			{
			Object()->SetDisable();
			return true;
			}
	};

class WriterEventMessage : public threading::OutputMessage<BaseWriterBackend>
	{
	public:
		WriterEventMessage(BaseWriterBackend* writer, const char* arg_event_name, const ValPtrVector& arg_vals)
			: threading::OutputMessage<BaseWriterBackend>("WriterEvent", writer),
			  event_name(arg_event_name), vals(arg_vals) {}

		virtual ~WriterEventMessage() { }

		virtual bool Process()
			{
			bool success = log_mgr->SendEvent(Object(), event_name, vals);

			if ( ! success )
				reporter->Error("Sending WriterEvent for event %s failed", event_name.c_str());

			return true; // We do not want to die if sendEvent fails because the event did not return.
			}

	private:
		std::string event_name;
		ValPtrVector vals;
	};

}

// Backend methods.

#pragma mark - BaseWriterBackend

using namespace logging;

broker::data BaseWriterBackend::WriterInfo::ToBroker() const
	{
	auto t = broker::table();

	for ( config_map::const_iterator i = config.begin(); i != config.end(); ++i )
		{
		auto key = std::string(i->first);
		auto value = std::string(i->second);
		t.insert(std::make_pair(key, value));
		}

	return broker::vector({path, rotation_base, rotation_interval, network_time, std::move(t)});
	}

bool BaseWriterBackend::WriterInfo::FromBroker(broker::data d)
	{
	if ( ! caf::holds_alternative<broker::vector>(d) )
		return false;

	auto v = caf::get<broker::vector>(d);
	auto bpath = caf::get_if<std::string>(&v[0]);
	auto brotation_base = caf::get_if<double>(&v[1]);
	auto brotation_interval = caf::get_if<double>(&v[2]);
	auto bnetwork_time = caf::get_if<double>(&v[3]);
	auto bconfig = caf::get_if<broker::table>(&v[4]);

	if ( ! (bpath && brotation_base && brotation_interval && bnetwork_time && bconfig) )
		return false;

	path = copy_string(bpath->c_str());
	rotation_base = *brotation_base;
	rotation_interval = *brotation_interval;
	network_time = *bnetwork_time;

	for ( auto i : *bconfig )
		{
		auto k = caf::get_if<std::string>(&i.first);
		auto v = caf::get_if<std::string>(&i.second);

		if ( ! (k && v) )
			return false;

		auto p = std::make_pair(copy_string(k->c_str()), copy_string(v->c_str()));
		config.insert(p);
		}

	return true;
	}

BaseWriterBackend::BaseWriterBackend(WriterFrontend* arg_frontend) :
	MsgThread(), m_default_config_map_inited(false)
	{
	num_fields = 0;
	fields = 0;
	buffering = true;
	frontend = arg_frontend;
	info = new WriterInfo(frontend->Info());
	rotation_counter = 0;
	m_logs_received = 0;
	m_log_writes_attempted = 0;
	m_log_writes_succeeded = 0;
	m_send_stats_interval_secs = -1;			// Uninited
	m_next_stats_send_clock_time_secs = -1;		// Uninited

	SetName(frontend->Name());

	// Determine the backend name
	std::string back_end_description = frontend->Name();
	for (char &c : back_end_description)
		c = std::tolower(c);
	const std::string match_string = "log::writer_";
	size_t match_loc = back_end_description.rfind(match_string);
	if (match_loc != std::string::npos)
		{
		size_t loc = match_loc + match_string.length();
		back_end_description = back_end_description.substr(loc);
		}
	m_backend_name = back_end_description;
	}

BaseWriterBackend::~BaseWriterBackend()
	{
	if ( fields )
		{
		for(int i = 0; i < num_fields; ++i)
			delete fields[i];

		delete [] fields;
		}

	delete info;
	}

void BaseWriterBackend::DeleteVals(int num_writes, Value*** vals)
	{
	for ( int j = 0; j < num_writes; ++j )
		{
		// Note this code is duplicated in Manager::DeleteVals().
		for ( int i = 0; i < num_fields; i++ )
			delete vals[j][i];

		delete [] vals[j];
		}

	delete [] vals;
	}

void BaseWriterBackend::SendEvent(const char* event_name, ValPtrVector& vals)
	{
	SendOut(new WriterEventMessage(this, event_name, vals));
	}

bool BaseWriterBackend::FinishedRotation(const char* new_name, const char* old_name,
        double open, double close, bool terminating)
	{
	--rotation_counter;
	SendOut(new RotationFinishedMessage(frontend, new_name, old_name, open, close, true, terminating));
	return true;
	}

bool BaseWriterBackend::FinishedRotation()
	{
	--rotation_counter;
	SendOut(new RotationFinishedMessage(frontend, 0, 0, 0, 0, false, false));
	return true;
	}

void BaseWriterBackend::DisableFrontend()
	{
	SendOut(new DisableMessage(frontend));
	}

bool BaseWriterBackend::Init(int arg_num_fields, const Field* const* arg_fields)
	{
	SetOSName(Fmt("zk.%s", Name()));
	num_fields = arg_num_fields;
	fields = arg_fields;

	if ( Failed() )
		return true;

	if ( ! DoInit(*info, arg_num_fields, arg_fields) )
		{
		DisableFrontend();
		return false;
		}

	return true;
	}

bool BaseWriterBackend::Write(int arg_num_fields, int num_writes, Value*** vals)
	{
	assert(num_writes >= 0);

	// Double-check that the arguments match. If we get this from remote,
	// something might be mixed up.
	if ( num_fields != arg_num_fields )
		{

#ifdef DEBUG
		const char* msg = Fmt("Number of fields don't match in BaseWriterBackend::Write() (%d vs. %d)",
		                      arg_num_fields, num_fields);
		Debug(DBG_LOGGING, msg);
#endif

		DeleteVals(num_writes, vals);
		DisableFrontend();
		return false;
		}

	// Double-check all the types match.
	for ( int j = 0; j < num_writes; j++ )
		{
		for ( int i = 0; i < num_fields; ++i )
			{
			if ( vals[j][i]->type != fields[i]->type )
				{
#ifdef DEBUG
				const char* msg = Fmt("Field #%d type doesn't match in BaseWriterBackend::Write() (%d vs. %d)",
				                      i, vals[j][i]->type, fields[i]->type);
				Debug(DBG_LOGGING, msg);
#endif
				DisableFrontend();
				DeleteVals(num_writes, vals);
				return false;
				}
			}
		}

	size_t logs_to_write = std::max(num_writes, 0);
	m_logs_received += logs_to_write;
	bool success = this->InternalWrite(logs_to_write, vals);

	// Don't call DeleteVals() here - BaseWriterBackend caches vals, and
	// accesses it after this function returns, so deleting vals here will
	// cause a dangling reference.

	if ( ! success )
		DisableFrontend();

	return success;
	}

bool BaseWriterBackend::InternalWrite(size_t num_writes, threading::Value*** vals)
	{
	// Exit early if nothing is to be written
	if (num_writes == 0)
		{
		return true;		// No fatal errors
		}

	bool no_fatal_errors = this->WriteLogs(num_writes, vals);

	// Don't delete vals - batching writers may be caching them

	return no_fatal_errors;
	}

bool BaseWriterBackend::SetBuf(bool enabled)
	{
	if ( enabled == buffering )
		// No change.
		return true;

	if ( Failed() )
		return true;

	buffering = enabled;

	if ( ! DoSetBuf(enabled) )
		{
		DisableFrontend();
		return false;
		}

	return true;
	}

bool BaseWriterBackend::Rotate(const char* rotated_path, double open,
                               double close, bool terminating)
	{
	if ( Failed() )
		return true;

	rotation_counter = 1;

	if ( ! DoRotate(rotated_path, open, close, terminating) )
		{
		DisableFrontend();
		return false;
		}

	// Insurance against broken writers.
	if ( rotation_counter > 0 )
		InternalError(Fmt("writer %s did not call FinishedRotation() in DoRotation()", Name()));

	if ( rotation_counter < 0 )
		InternalError(Fmt("writer %s called FinishedRotation() more than once in DoRotation()", Name()));

	return true;
	}

bool BaseWriterBackend::Flush(double network_time)
	{
	if ( Failed() )
		return true;

	if ( ! DoFlush(network_time) )
		{
		DisableFrontend();
		return false;
		}

	return true;
	}

bool BaseWriterBackend::OnHeartbeat(double network_time, double current_time)
	{
	if ( Failed() )
		return true;

	SendOut(new FlushWriteBufferMessage(frontend));

	// If needed, init the stats timing member variables
	if (m_send_stats_interval_secs <= 0)
		{
		// Get the stats interval - it must be between one millisecond and 24 hours
		std::string scratch = this->GetConfigString("statistics_interval_seconds");
		m_send_stats_interval_secs = std::stod(scratch);
		m_send_stats_interval_secs = std::max(m_send_stats_interval_secs, 0.001);
		m_send_stats_interval_secs = std::min(m_send_stats_interval_secs, 86400.0);

		// Determine the next time to send statistics
		m_next_stats_send_clock_time_secs = current_time + m_send_stats_interval_secs;
		}

	// If the time is right, send stats on this writer
	if (current_time >= m_next_stats_send_clock_time_secs)
		{
		this->SendStats();
		m_next_stats_send_clock_time_secs = current_time + m_send_stats_interval_secs;
		}

	// Pass the heartbeat request on to subclasses
	bool result = this->DoHeartbeat(network_time, current_time);
	return result;
	}

std::string BaseWriterBackend::GetConfigString(const std::string& key) const
	{
	// If needed, set up the default config map
	if (!m_default_config_map_inited)
		{
		m_default_config_map = GetDefaultConfigMap();
		m_default_config_map_inited = true;
		}

	// Find the key in the default config map
	BaseWriterBackend::WriterInfo::config_map::const_iterator itr = m_default_config_map.find(key.c_str());
	assert(itr != m_default_config_map.end());
	std::string result = itr->second;

	// If present in the config, use that value. If this exists, it can be inited as a global
	// config value in a bro script, and overridden in a bro script for the frontend.
	itr = info->config.find(key.c_str());
	if (itr != info->config.end())
		result = itr->second;


	// If present, override with a bro script writer/frontend combination value. For example,
	// to override "foo" for the ASCII writer, use "ascii:foo"
	std::string override_key = this->GetBackendName() + ":" + key;
	itr = info->config.find(override_key.c_str());
	if (itr != info->config.end())
		result = itr->second;

	return result;
	}

BaseWriterBackend::WriterInfo::config_map BaseWriterBackend::GetDefaultConfigMap() const
	{
	const static BaseWriterBackend::WriterInfo::config_map c_default_config =
		{
			{   "statistics_interval_seconds",	"10"     },
		};
	return c_default_config;
	}

std::string BaseWriterBackend::GetFrontendName() const
	{
	return info->path;
	}

std::string BaseWriterBackend::GetBackendName() const
	{
	std::string result = this->m_backend_name;
	return result;
	}

std::string BaseWriterBackend::FullName() const
	{
	std::string result = GetFrontendName() + ":" + GetBackendName();
	return result;
	}

void BaseWriterBackend::SendStats()
	{
	ValPtrVector error_vals =
		{
		val_mgr->GetCount(m_logs_received),
		val_mgr->GetCount(m_log_writes_attempted),
		val_mgr->GetCount(m_log_writes_succeeded)
		};
	SendEvent("Log::statistics", error_vals);
	}

logging::BaseWriterBackend::HandleWriteErrorsResult BaseWriterBackend::HandleWriteErrors(const LogRecordBatch& records,
        const WriteErrorInfoVector& errors)
	{
	bool has_fatal_errors = false;
	size_t error_record_count = 0;
	for (const WriteErrorInfo& this_error: errors)
		{
		// Report any errors. The columns are:
		// 1. Description
		// 2. Whether the error is fatal
		// 3. The number of records involved
		ValPtrVector error_vals =
			{
			new StringVal(this_error.description),
			val_mgr->GetBool(this_error.is_fatal),
			val_mgr->GetCount(this_error.record_count)
			};
		SendEvent("Log::write_error", error_vals);

		error_record_count += this_error.record_count;
		has_fatal_errors |= this_error.is_fatal;
		}

	HandleWriteErrorsResult result = { error_record_count, !has_fatal_errors };
	return result;
	}

bool BaseWriterBackend::HandleWriteErrors(size_t error_log_index, size_t num_writes,
        threading::Value*** vals)
	{
	if (num_writes == 0) 		// No fatal errors
		return true;

	LogRecordBatch record_batch(vals, vals + num_writes);

	WriteErrorInfoVector errors;
	errors.emplace_back(error_log_index, 1, "Unspecified", false);
	size_t next_record_index = error_log_index + 1;
	if (next_record_index < num_writes)
		{
		errors.emplace_back(next_record_index, num_writes - next_record_index,
		                    "Not written due to previous error", false);
		}

	HandleWriteErrorsResult r = HandleWriteErrors(record_batch, errors);
	bool no_fatal_errors = r.no_fatal_errors;
	return no_fatal_errors;
	}

void BaseWriterBackend::ReportWriteStatistics(size_t log_writes_attempted,
        size_t log_writes_succeeded)
	{
	m_log_writes_attempted += log_writes_attempted;
	m_log_writes_succeeded += log_writes_succeeded;
	}
