// See the file "COPYING" in the main distribution directory for copyright.

#include <string>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "threading/SerialTypes.h"

#include "Ascii_Batch.h"
#include "ascii_batch.bif.h"

using namespace logging::writer;
using namespace threading;
using threading::Value;
using threading::Field;

Ascii_Batch::Ascii_Batch(WriterFrontend* frontend) : BatchWriterBackend(frontend)
	{
	fd = 0;
	ascii_done = false;
	output_to_stdout = false;
	include_meta = false;
	tsv = false;
	use_json = false;
	enable_utf_8 = false;
	formatter = 0;
	gzip_level = 0;
	gzfile = nullptr;

	InitConfigOptions();
	init_options = InitFilterOptions();
	}

void Ascii_Batch::InitConfigOptions()
	{
	output_to_stdout = BifConst::LogAsciiBatch::output_to_stdout;
	include_meta = BifConst::LogAsciiBatch::include_meta;
	use_json = BifConst::LogAsciiBatch::use_json;
	enable_utf_8 = BifConst::LogAsciiBatch::enable_utf_8;
	gzip_level = BifConst::LogAsciiBatch::gzip_level;

	separator.assign(
			(const char*) BifConst::LogAsciiBatch::separator->Bytes(),
			BifConst::LogAsciiBatch::separator->Len()
			);

	set_separator.assign(
			(const char*) BifConst::LogAsciiBatch::set_separator->Bytes(),
			BifConst::LogAsciiBatch::set_separator->Len()
			);

	empty_field.assign(
			(const char*) BifConst::LogAsciiBatch::empty_field->Bytes(),
			BifConst::LogAsciiBatch::empty_field->Len()
			);

	unset_field.assign(
			(const char*) BifConst::LogAsciiBatch::unset_field->Bytes(),
			BifConst::LogAsciiBatch::unset_field->Len()
			);

	meta_prefix.assign(
			(const char*) BifConst::LogAsciiBatch::meta_prefix->Bytes(),
			BifConst::LogAsciiBatch::meta_prefix->Len()
			);

	ODesc tsfmt;
	BifConst::LogAsciiBatch::json_timestamps->Describe(&tsfmt);
	json_timestamps.assign(
			(const char*) tsfmt.Bytes(),
			tsfmt.Len()
			);

	gzip_file_extension.assign(
		(const char*) BifConst::LogAsciiBatch::gzip_file_extension->Bytes(),
		BifConst::LogAsciiBatch::gzip_file_extension->Len()
		);
	}

bool Ascii_Batch::InitFilterOptions()
	{
	const WriterInfo& info = Info();

	// Set per-filter configuration options.
	for ( WriterInfo::config_map::const_iterator i = info.config.begin();
	      i != info.config.end(); ++i )
		{
		if ( strcmp(i->first, "tsv") == 0 )
			{
			if ( strcmp(i->second, "T") == 0 )
				tsv = true;
			else if ( strcmp(i->second, "F") == 0 )
				tsv = false;
			else
				{
				Error("invalid value for 'tsv', must be a string and either \"T\" or \"F\"");
				return false;
				}
			}

		else if ( strcmp(i->first, "gzip_level" ) == 0 )
			{
			gzip_level = atoi(i->second);

			if ( gzip_level < 0 || gzip_level > 9 )
				{
				Error("invalid value for 'gzip_level', must be a number between 0 and 9.");
				return false;
				}
			}
		else if ( strcmp(i->first, "use_json") == 0 )
			{
			if ( strcmp(i->second, "T") == 0 )
				use_json = true;
			else if ( strcmp(i->second, "F") == 0 )
				use_json = false;
			else
				{
				Error("invalid value for 'use_json', must be a string and either \"T\" or \"F\"");
				return false;
				}
			}

		else if ( strcmp(i->first, "enable_utf_8") == 0 )
			{
			if ( strcmp(i->second, "T") == 0 )
				enable_utf_8 = true;
			else if ( strcmp(i->second, "F") == 0 )
				enable_utf_8 = false;
			else
				{
				Error("invalid value for 'enable_utf_8', must be a string and either \"T\" or \"F\"");
				return false;
				}
			}

		else if ( strcmp(i->first, "output_to_stdout") == 0 )
			{
			if ( strcmp(i->second, "T") == 0 )
				output_to_stdout = true;
			else if ( strcmp(i->second, "F") == 0 )
				output_to_stdout = false;
			else
				{
				Error("invalid value for 'output_to_stdout', must be a string and either \"T\" or \"F\"");
				return false;
				}
			}

		else if ( strcmp(i->first, "separator") == 0 )
			separator.assign(i->second);

		else if ( strcmp(i->first, "set_separator") == 0 )
			set_separator.assign(i->second);

		else if ( strcmp(i->first, "empty_field") == 0 )
			empty_field.assign(i->second);

		else if ( strcmp(i->first, "unset_field") == 0 )
			unset_field.assign(i->second);

		else if ( strcmp(i->first, "meta_prefix") == 0 )
			meta_prefix.assign(i->second);

		else if ( strcmp(i->first, "json_timestamps") == 0 )
			json_timestamps.assign(i->second);

		else if ( strcmp(i->first, "gzip_file_extension") == 0 )
			gzip_file_extension.assign(i->second);
		}

	if ( ! InitFormatter() )
		return false;

	return true;
	}

bool Ascii_Batch::InitFormatter()
	{
	delete formatter;
	formatter = 0;

	if ( use_json )
		{
		formatter::JSON::TimeFormat tf = formatter::JSON::TS_EPOCH;

		// Write out JSON formatted logs.
		if ( strcmp(json_timestamps.c_str(), "JSON::TS_EPOCH") == 0 )
			tf = formatter::JSON::TS_EPOCH;
		else if ( strcmp(json_timestamps.c_str(), "JSON::TS_MILLIS") == 0 )
			tf = formatter::JSON::TS_MILLIS;
		else if ( strcmp(json_timestamps.c_str(), "JSON::TS_ISO8601") == 0 )
			tf = formatter::JSON::TS_ISO8601;
		else
			{
			Error(Fmt("Invalid JSON timestamp format: %s", json_timestamps.c_str()));
			return false;
			}

		formatter = new formatter::JSON(this, tf);
		// Using JSON implicitly turns off the header meta fields.
		include_meta = false;
		}
	else
		{
		// Enable utf-8 if needed
		if ( enable_utf_8 )
			desc.EnableUTF8();

		// Use the default "Bro logs" format.
		desc.EnableEscaping();
		desc.AddEscapeSequence(separator);
		formatter::Ascii::SeparatorInfo sep_info(separator, set_separator, unset_field, empty_field);
		formatter = new formatter::Ascii(this, sep_info);
		}

	return true;
	}

Ascii_Batch::~Ascii_Batch()
	{
	if ( ! ascii_done )
		// In case of errors aborting the logging altogether,
		// DoFinish() may not have been called.
		CloseFile(network_time);

	delete formatter;
	}

void Ascii_Batch::WriteHeaderField(const string& key, const string& val)
	{
	string str = meta_prefix + key + separator + val + "\n";

	InternalWrite(fd, str.c_str(), str.length());
	}

void Ascii_Batch::CloseFile(double t)
	{
	if ( ! fd )
		return;

	if ( include_meta && ! tsv )
		WriteHeaderField("close", Timestamp(0));

	InternalClose(fd);
	fd = 0;
	gzfile = nullptr;
	}

bool Ascii_Batch::DoInit(const WriterInfo& info, int num_fields, const Field* const * fields)
	{
	assert(! fd);

	if ( ! init_options )
		return false;

	string path = info.path;

	if ( output_to_stdout )
		path = "/dev/stdout";

	fname = IsSpecial(path) ? path : path + "." + LogExt();

	if ( gzip_level > 0 )
		{
		fname += ".";
		fname += gzip_file_extension.empty() ? "gz" : gzip_file_extension;
		}

	fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);

	if ( fd < 0 )
		{
		Error(Fmt("cannot open %s: %s", fname.c_str(),
			  Strerror(errno)));
		fd = 0;
		return false;
		}

	if ( gzip_level > 0 )
		{
		if ( gzip_level < 0 || gzip_level > 9 )
			{
			Error("invalid value for 'gzip_level', must be a number between 0 and 9.");
			return false;
			}

		char mode[4];
		snprintf(mode, sizeof(mode), "wb%d", gzip_level);
		errno = 0; // errno will only be set under certain circumstances by gzdopen.
		gzfile = gzdopen(fd, mode);

		if ( gzfile == nullptr )
			{
			Error(Fmt("cannot gzip %s: %s", fname.c_str(),
			                                Strerror(errno)));
			return false;
			}
		}
	else
		{
		gzfile = nullptr;
		}

#if OLD
	if ( ! WriteHeader(path) )
		{
		Error(Fmt("error writing to %s: %s", fname.c_str(), Strerror(errno)));
		return false;
		}
#else
		try {
			WriteHeader(path);
		}
		catch (...) {
			Error(Fmt("error writing to %s: %s", fname.c_str(), Strerror(errno)));
			return false;
		}
#endif

	return true;
	}

void Ascii_Batch::WriteHeader(const string& path)
	{
	if ( ! include_meta )
		return;

	string names;
	string types;

	for ( int i = 0; i < NumFields(); ++i )
		{
		if ( i > 0 )
			{
			names += separator;
			types += separator;
			}

		names += string(Fields()[i]->name);
		types += Fields()[i]->TypeName().c_str();
		}

	if ( tsv )
		{
		// A single TSV-style line is all we need.
		string str = names + "\n";
#if OLD
		if ( ! InternalWrite(fd, str.c_str(), str.length()) )
			return false;

		return true;
#else
			InternalWrite(fd, str.c_str(), str.length());
			return;
#endif
		}

	string str = meta_prefix
		+ "separator " // Always use space as separator here.
		+ get_escaped_string(separator, false)
		+ "\n";

#if OLD
	if ( ! InternalWrite(fd, str.c_str(), str.length()) )
		return false;

    if ( ! (WriteHeaderField("writer", this->GetBackendName())) )
            return false;

	if ( ! (WriteHeaderField("set_separator", get_escaped_string(set_separator, false)) &&
	        WriteHeaderField("empty_field", get_escaped_string(empty_field, false)) &&
	        WriteHeaderField("unset_field", get_escaped_string(unset_field, false)) &&
	        WriteHeaderField("path", get_escaped_string(path, false)) &&
	        WriteHeaderField("open", Timestamp(0))) )
		return false;

	if ( ! (WriteHeaderField("fields", names) &&
	        WriteHeaderField("types", types)) )
		return false;
#else
		InternalWrite(fd, str.c_str(), str.length());

		WriteHeaderField("writer", this->GetBackendName());

		WriteHeaderField("set_separator", get_escaped_string(set_separator, false));
		WriteHeaderField("empty_field", get_escaped_string(empty_field, false));
		WriteHeaderField("unset_field", get_escaped_string(unset_field, false));
		WriteHeaderField("path", get_escaped_string(path, false));
		WriteHeaderField("open", Timestamp(0));

		WriteHeaderField("fields", names);
		WriteHeaderField("types", types);
#endif
	}

bool Ascii_Batch::DoFlush(double network_time)
	{
	fsync(fd);
	return true;
	}

bool Ascii_Batch::DoFinish(double network_time)
	{
	if ( ascii_done )
		{
		fprintf(stderr, "internal error: duplicate finish\n");
		abort();
		}

	DoFlush(network_time);

	ascii_done = true;

	CloseFile(network_time);

	return true;
	}

void Ascii_Batch::WriteOneRecord(threading::Value** vals)
	{
	desc.Clear();

#if OLD
	if ( ! formatter->Describe(&desc, NumFields(), Fields(), vals) )
		return false;
#else
		if ( ! formatter->Describe(&desc, NumFields(), Fields(), vals) )
			throw_non_fatal_writer_error("Couldn't format the log record's values");
#endif

	desc.AddRaw("\n", 1);

	const char* bytes = (const char*)desc.Bytes();
	int len = desc.Len();

	if ( strncmp(bytes, meta_prefix.data(), meta_prefix.size()) == 0 )
		{
		// It would so escape the first character.
		char hex[4] = {'\\', 'x', '0', '0'};
		bytetohex(bytes[0], hex + 2);

#if OLD
		if ( ! InternalWrite(fd, hex, 4) )
			goto write_error;
#else
			InternalWrite(fd, hex, 4);
#endif

		++bytes;
		--len;
		}

#if OLD
	if ( ! InternalWrite(fd, bytes, len) )
		goto write_error;
#else
		InternalWrite(fd, bytes, len);
#endif

#if OLD
write_error:
	Error(Fmt("error writing to %s: %s", fname.c_str(), Strerror(errno)));
	return false;
#endif
	}


logging::BatchWriterBackend::WriteErrorInfoVector Ascii_Batch::BatchWrite(const LogRecordBatch &records_to_write)
{
    
    // Initialize the write for this batch
	if ( ! fd )
		DoInit(Info(), NumFields(), Fields());
    
    // Write the log records in this batch
	WriteErrorInfoVector errors;
	size_t record_count = records_to_write.size();
	LogRecordBatch::const_iterator itr = records_to_write.begin();
	for (size_t i = 0; i < record_count; ++i, ++itr) {
		// Try to write this log record, and record any fatal or non-fatal errors
		try {
			WriteOneRecord(*itr);
		}
		catch (fatal_writer_error e) {
			errors.emplace_back(i, 1, e.what(), true);
		}
		catch (non_fatal_writer_error e) {
			errors.emplace_back(i, 1, e.what(), false);
		}
		
		// If there was an error, don't write any remaining records
		if (!errors.empty()) {
			size_t next_record_index = i + 1;
			if (next_record_index < record_count) {
				errors.emplace_back(next_record_index, record_count - next_record_index,
									"Not written due to previous error", false);
			}
		}
	}
    
    // Wrap up and report any errors
	if ( ! IsBuf() )
		fsync(fd);
    
    return errors;
}


bool Ascii_Batch::DoRotate(const char* rotated_path, double open, double close, bool terminating)
	{
	// Don't rotate special files or if there's not one currently open.
	if ( ! fd || IsSpecial(Info().path) )
		{
		FinishedRotation();
		return true;
		}

	CloseFile(close);

	string nname = string(rotated_path) + "." + LogExt();

	if ( gzip_level > 0 )
		{
		nname += ".";
		nname += gzip_file_extension.empty() ? "gz" : gzip_file_extension;
		}

	if ( rename(fname.c_str(), nname.c_str()) != 0 )
		{
		char buf[256];
		bro_strerror_r(errno, buf, sizeof(buf));
		Error(Fmt("failed to rename %s to %s: %s", fname.c_str(),
		          nname.c_str(), buf));
		FinishedRotation();
		return false;
		}

	if ( ! FinishedRotation(nname.c_str(), fname.c_str(), open, close, terminating) )
		{
		Error(Fmt("error rotating %s to %s", fname.c_str(), nname.c_str()));
		return false;
		}

	return true;
	}

bool Ascii_Batch::DoSetBuf(bool enabled)
	{
	// Nothing to do.
	return true;
	}

bool Ascii_Batch::DoHeartbeat(double network_time, double current_time)
	{
	// Nothing to do.
	return true;
	}

string Ascii_Batch::LogExt()
	{
	const char* ext = zeekenv("ZEEK_LOG_SUFFIX");

	if ( ! ext )
		ext = "log";

	return ext;
	}

string Ascii_Batch::Timestamp(double t)
	{
	time_t teatime = time_t(t);

	if ( ! teatime )
		{
		// Use wall clock.
		struct timeval tv;
		if ( gettimeofday(&tv, 0) < 0 )
			Error("gettimeofday failed");
		else
			teatime = tv.tv_sec;
		}

	struct tm tmbuf;
	struct tm* tm = localtime_r(&teatime, &tmbuf);

	char tmp[128];
	const char* const date_fmt = "%Y-%m-%d-%H-%M-%S";
	strftime(tmp, sizeof(tmp), date_fmt, tm);

	return tmp;
	}

void Ascii_Batch::InternalWrite(int fd, const char* data, int len)
	{
#if BOBERT
		
		FORMAT 	Error(Fmt("error writing to %s: %s", fname.c_str(), Strerror(errno)));

		// Normal ASCII write. If 
		if ( ! gzfile ) {
			while ( len > 0 )
				{
				int n = write(fd, data, len);

				if ( n < 0 )
					{
					if ( errno == EINTR )
						continue;

						throw std::runtime_error(strerror(errno));
					}

				data += n;
				len -= n;
				}

			return;
		}

	while ( len > 0 )
		{
			SIMILAR TO ABOVE HERE
		int n = gzwrite(gzfile, data, len);

		if ( n <= 0 )
			{
			const char* err = gzerror(gzfile, &n);
			Error(Fmt("Ascii_Batch::InternalWrite error: %s\n", err));
			return false;
			}

		data += n;
		len -= n;
		}

	return true;
#endif
	}

bool Ascii_Batch::InternalClose(int fd)
	{
	if ( ! gzfile )
		{
		safe_close(fd);
		return true;
		}

	int res = gzclose(gzfile);

	if ( res == Z_OK )
		return true;

	switch ( res ) {
	case Z_STREAM_ERROR:
		Error("Ascii_Batch::InternalClose gzclose error: invalid file stream");
		break;
	case Z_BUF_ERROR:
		Error("Ascii_Batch::InternalClose gzclose error: "
		      "no compression progress possible during buffer flush");
		break;
	case Z_ERRNO:
		Error(Fmt("Ascii_Batch::InternalClose gzclose error: %s\n", Strerror(errno)));
		break;
	default:
		Error("Ascii_Batch::InternalClose invalid gzclose result");
		break;
	}

	return false;
	}

