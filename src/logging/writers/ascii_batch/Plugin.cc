// See the file  in the main distribution directory for copyright.


#include "plugin/Plugin.h"

#include "Ascii_Batch.h"

namespace plugin {
namespace Zeek_Ascii_BatchWriter {

class Plugin : public plugin::Plugin {
public:
	plugin::Configuration Configure()
		{
		AddComponent(new ::logging::Component("Ascii_Batch", ::logging::writer::Ascii_Batch::Instantiate));

		plugin::Configuration config;
		config.name = "Zeek::Ascii_BatchWriter";
		config.description = "ASCII log writer";
		return config;
		}
} plugin;

}
}
