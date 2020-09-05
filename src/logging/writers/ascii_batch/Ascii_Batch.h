// See the file "COPYING" in the main distribution directory for copyright.
//
// Batch log writer for delimiter-separated ASCII logs.

#pragma once

#include "logging/BatchWriterBackend.h"
#include "threading/formatters/Ascii.h"
#include "threading/formatters/JSON.h"
#include "Desc.h"
#include "zlib.h"

namespace logging
{
namespace writer
{

class Ascii_Batch : public BatchWriterBackend
	{
	public:
		explicit Ascii_Batch(WriterFrontend* frontend);
		~Ascii_Batch() override;

		static string LogExt();

		static BatchWriterBackend* Instantiate(WriterFrontend* frontend)
			{
			return new Ascii_Batch(frontend);
			}

	protected:
		bool DoInit(const WriterInfo& info, int num_fields,
		            const threading::Field* const* fields) override;
		WriteErrorInfoVector BatchWrite(const LogRecordBatch &records_to_write) override;
		bool DoSetBuf(bool enabled) override;
		bool DoRotate(const char* rotated_path, double open,
		              double close, bool terminating) override;
		bool DoFlush(double network_time) override;
		bool DoFinish(double network_time) override;
		bool DoHeartbeat(double network_time, double current_time) override;

	private:
		bool IsSpecial(const string &path)
			{
			return path.find("/dev/") == 0;
			}
		void WriteHeader(const string& path);
		void WriteHeaderField(const string& key, const string& value);
		void CloseFile(double t);
		string Timestamp(double t); // Uses current time if t is zero.
		void InitConfigOptions();
		bool InitFilterOptions();
		bool InitFormatter();
		void InternalWrite(int fd, const char* data, int len);
		bool InternalClose(int fd);

		void WriteOneRecord(threading::Value** vals);

		int fd;
		gzFile gzfile;
		string fname;
		ODesc desc;
		bool ascii_done;

		// Options set from the script-level.
		bool output_to_stdout;
		bool include_meta;
		bool tsv;

		string separator;
		string set_separator;
		string empty_field;
		string unset_field;
		string meta_prefix;

		int gzip_level; // level > 0 enables gzip compression
		string gzip_file_extension;
		bool use_json;
		bool enable_utf_8;
		string json_timestamps;

		threading::formatter::Formatter* formatter;
		bool init_options;
	};

}
}
