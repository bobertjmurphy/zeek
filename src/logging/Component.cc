// See the file "COPYING" in the main distribution directory for copyright.

#include "Component.h"
#include "Manager.h"
#include "../Desc.h"
#include "../util.h"
#include "BaseWriterBackend.h"
#include "WriterBackend.h"
#include "BatchWriterBackend.h"

using namespace logging;

Component::Component(const std::string& name, base_factory_callback arg_factory)
	: plugin::Component(plugin::component::WRITER, name)
	{
	factory = arg_factory;
	}
	
Component::Component(const std::string& name, factory_callback arg_factory)
	: Component(name, reinterpret_cast<base_factory_callback>(arg_factory))
	{
	}

Component::Component(const std::string& name, batch_factory_callback arg_factory)
	: Component(name, reinterpret_cast<base_factory_callback>(arg_factory))
	{
	}

void Component::Initialize()
	{
	InitializeTag();
	log_mgr->RegisterComponent(this, "WRITER_");
	}

Component::~Component()
	{
	}

void Component::DoDescribe(ODesc* d) const
	{
	d->Add("Log::WRITER_");
	d->Add(CanonicalName());
	}
