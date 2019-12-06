/***************************************************************************
 *   Copyright (C) by GFZ Potsdam                                          *
 *                                                                         *
 *   You can redistribute and/or modify this program under the             *
 *   terms of the SeisComP Public License.                                 *
 *                                                                         *
 *   This program is distributed in the hope that it will be useful,       *
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
 *   SeisComP Public License for more details.                             *
 ***************************************************************************/



#define SEISCOMP_COMPONENT Export2HMB
#include "export2hmb.h"

#include <vector>

#include <seiscomp3/logging/log.h>
#include <seiscomp3/core/strings.h>
#include <seiscomp3/core/system.h>
#include <seiscomp3/system/environment.h>
#include <seiscomp3/core/datamessage.h>
#include <seiscomp3/utils/timer.h>
#include <seiscomp3/core/datetime.h>
#include <seiscomp3/utils/files.h>
#include <seiscomp3/client/inventory.h>
#include <seiscomp3/datamodel/eventparameters.h>
#include <seiscomp3/datamodel/amplitude.h>
#include <seiscomp3/io/socket.h>
#include <seiscomp3/io/httpsocket.h>
#include <seiscomp3/io/httpsocket.ipp>

#include <seiscomp3/io/archive/bsonarchive.h>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

namespace Seiscomp {
namespace Applications {

const int BSON_SIZE_MAX = 16*1024*1024;
const int SOCKET_TIMEOUT = 60;

// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
Export2HMB::Export2HMB(int argc, char* argv[]) :
	Client::Application(argc, argv)
{
	setMessagingEnabled(true);
	setDatabaseEnabled(false, false);
	setPrimaryMessagingGroup(Client::Protocol::LISTENER_GROUP);
	addMessagingSubscription("IMPORT_GROUP");
	setMessagingUsername(Util::basename(argv[0]));
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
Export2HMB::~Export2HMB() {}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
bool Export2HMB::init() {
	if (!Application::init())
		return false;

	std::string sink = "hmb://localhost:8000/";

	if (commandline().hasOption("sink")) {
		sink = commandline().option<std::string>("sink");
	}
	else {
		try {
		sink = configGetString("sink");
		}
		catch (Config::Exception& ) {}
	}

	size_t pos = sink.find("://");

	if ( pos == std::string::npos || sink.substr(0, pos) != "hmb" ) {
		SEISCOMP_ERROR("invalid sink");
		return false;
	}

	std::string serverAddress = sink.substr(pos + 3);

	std::string host;
	pos = serverAddress.find('@');

	if ( pos != std::string::npos ) {
		std::string login = serverAddress.substr(0, pos);
		host = serverAddress.substr(pos + 1);
		pos = login.find(':');

		if ( pos != std::string::npos ) {
			_user = login.substr(0, pos);
			_password = login.substr(pos + 1);
		}
		else {
			_user = login;
			_password = "";
		}
	}
	else {
		host = serverAddress;
		_user = "";
		_password = "";
	}

	pos = host.find('/');

	if ( pos != std::string::npos ) {
		_serverHost = host.substr(0, pos);
		_serverPath = host.substr(pos);

		if ( *_serverPath.rbegin() != '/' )
			_serverPath += '/';
	}
	else {
		_serverHost = host;
		_serverPath = "/";
	}

	return true;
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
void Export2HMB::done() {
	Client::Application::done();
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
void Export2HMB::createCommandLineDescription() {
	Application::createCommandLineDescription();
	commandline().addGroup("export2hmb");
	commandline().addOption("export2hmb", "sink,o", "Sink HMB", static_cast<std::string*>(NULL));
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
std::string Export2HMB::bsonGetString(const bson_t *bson, const char *key)
{
	bson_iter_t iter;
	if ( bson_iter_init_find(&iter, bson, key) ) {
		if ( bson_iter_type(&iter) == BSON_TYPE_UTF8 ) {
			uint32_t value_len;
			const char *value = bson_iter_utf8(&iter, &value_len);
			return std::string(value, value_len);
		}

		throw Core::GeneralException((std::string("invalid ") + key).c_str());
	}

	throw Core::GeneralException((std::string("missing ") + key).c_str());

}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
void Export2HMB::initSession()
{
	IO::HttpSocket<IO::Socket> sock;

	try {
		bson_t req = BSON_INITIALIZER;

		sock.setTimeout(SOCKET_TIMEOUT);
		sock.startTimer();
		sock.open(_serverHost, _user, _password);
		sock.httpPost(_serverPath + "open", std::string((char *) bson_get_data(&req), req.len));
		sock.startTimer();
		std::string data = sock.httpRead(4);
		int size;
		memcpy(&size, data.c_str(), 4);
		size = BSON_UINT32_FROM_LE(size);

		SEISCOMP_DEBUG("BSON size (ack): %d", size);

		if ( size > BSON_SIZE_MAX )
			throw Core::GeneralException("invalid BSON size (ack)");

		sock.startTimer();
		data += sock.httpRead(size - 4);

		bson_t ack = BSON_INITIALIZER;

		if ( !bson_init_static(&ack, (const uint8_t *) data.data(), data.length()) )
			throw Core::GeneralException("invalid BSON data (ack)");

		_sid = bsonGetString(&ack, "sid");
		_cid = bsonGetString(&ack, "cid");

		SEISCOMP_INFO("HMB session opened with sid=%s, cid=%s", _sid.c_str(), _cid.c_str());
	}
	catch ( Core::GeneralException &e ) {
		if ( sock.isOpen() )
			sock.close();

		throw;
	}

	sock.close();
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
void Export2HMB::handleMessage(Core::Message* msg)
{
	std::string data;

	{
		boost::iostreams::stream_buffer<boost::iostreams::back_insert_device<std::string> > buf(data);
		IO::BSONArchive ar(&buf, false, -1);
		ar << msg;

		if ( !ar.success() )
			throw Core::GeneralException("failed to serialize message");
	}

	bson_t bdata = BSON_INITIALIZER;

	if ( !bson_init_static(&bdata, (const uint8_t*) data.data(), data.size()) )
		throw Core::GeneralException("failed to serialize message");

	bson_t bmsg = BSON_INITIALIZER;
	bson_append_utf8(&bmsg, "type", -1, "SC3", -1);
	bson_append_utf8(&bmsg, "queue", -1, "SC3MSG", -1);
	bson_append_utf8(&bmsg, "topic", -1, "IMPORT_GROUP", -1);
	bson_append_int32(&bmsg, "scMessageType", -1, 1);
	bson_append_int32(&bmsg, "scContentType", -1, 5);
	bson_append_document(&bmsg, "data", -1, &bdata);

	std::string msgdata((char *) bson_get_data(&bmsg), bmsg.len);
	bson_destroy(&bmsg);

	IO::HttpSocket<IO::Socket> sock;

	for ( int retry = 0; retry < 2; ++retry ) {
		try {
			if ( _sid.length() == 0 )
				initSession();

			sock.setTimeout(SOCKET_TIMEOUT);
			sock.startTimer();
			sock.open(_serverHost, _user, _password);
			sock.httpPost(_serverPath + "send/" + _sid, msgdata);
			sock.httpRead(1024);
			sock.close();
			break;
		}
		catch ( Core::GeneralException &e ) {
			SEISCOMP_ERROR("%s", e.what());

			if ( sock.isOpen() )
				sock.close();

			_sid = "";
		}
	}
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


} // namespace Applictions
} // namespace Seiscomp
