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



#define SEISCOMP_COMPONENT Pick2HMB
#include "pick2hmb.h"

#include <vector>

#include <seiscomp/logging/log.h>
#include <seiscomp/core/strings.h>
#include <seiscomp/core/system.h>
#include <seiscomp/system/environment.h>
#include <seiscomp/core/datamessage.h>
#include <seiscomp/utils/timer.h>
#include <seiscomp/core/datetime.h>
#include <seiscomp/utils/files.h>
#include <seiscomp/client/inventory.h>
#include <seiscomp/datamodel/eventparameters.h>
#include <seiscomp/datamodel/amplitude.h>
#include <seiscomp/io/socket.h>
#include <seiscomp/io/httpsocket.h>
#include <seiscomp/io/httpsocket.ipp>

#include <seiscomp/io/archive/bsonarchive.h>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

namespace Seiscomp {
namespace Applications {

const int BSON_SIZE_MAX = 16*1024*1024;
const int SOCKET_TIMEOUT = 60;

// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
Pick2HMB::Pick2HMB(int argc, char* argv[]) :
	Client::Application(argc, argv)
{
	setMessagingEnabled(true);
	setLoadStationsEnabled(true);
	setPrimaryMessagingGroup(Client::Protocol::LISTENER_GROUP);
	addMessagingSubscription("PICK");
	addMessagingSubscription("AMPLITUDE");
	setMessagingUsername(Util::basename(argv[0]));
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
Pick2HMB::~Pick2HMB() {}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
bool Pick2HMB::init() {
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
void Pick2HMB::done() {
	Client::Application::done();
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
void Pick2HMB::createCommandLineDescription() {
	Application::createCommandLineDescription();
	commandline().addGroup("pick2hmb");
	commandline().addOption("pick2hmb", "sink,o", "Sink HMB", static_cast<std::string*>(NULL));
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
std::string Pick2HMB::bsonGetString(const bson_t *bson, const char *key)
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
void Pick2HMB::initSession()
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




// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
void Pick2HMB::sendPick(DataModel::Pick* pick) {
	try {
		if ( pick->evaluationMode() == DataModel::MANUAL )
			return;
	}
	catch ( ... ) {
		return;
	}

	DataModel::Stream *stream = Client::Inventory::Instance()->getStream(
			pick->waveformID().networkCode(),
			pick->waveformID().stationCode(),
			pick->waveformID().locationCode(),
			pick->waveformID().channelCode(),
			pick->time().value());

	if ( stream == NULL ) {
		SEISCOMP_ERROR("cannot find stream %s.%s.%s.%s at %s",
				pick->waveformID().networkCode().c_str(),
				pick->waveformID().stationCode().c_str(),
				pick->waveformID().locationCode().c_str(),
				pick->waveformID().channelCode().c_str(),
				Core::toString(pick->time().value()).c_str());

		return;
	}

        try {
		if ( stream->restricted() )
			return;
	}
	catch ( ... ) {
		SEISCOMP_ERROR("failed to get restricted status of %s.%s.%s.%s at %s",
				pick->waveformID().networkCode().c_str(),
				pick->waveformID().stationCode().c_str(),
				pick->waveformID().locationCode().c_str(),
				pick->waveformID().channelCode().c_str(),
				Core::toString(pick->time().value()).c_str());

		return;
	}

	DataModel::SensorLocation* loc = Client::Inventory::Instance()->getSensorLocation(
			pick->waveformID().networkCode(),
			pick->waveformID().stationCode(),
			pick->waveformID().locationCode(),
			pick->time().value());

	if ( loc == NULL ) {
		SEISCOMP_ERROR("cannot find location %s.%s.%s at %s",
				pick->waveformID().networkCode().c_str(),
				pick->waveformID().stationCode().c_str(),
				pick->waveformID().locationCode().c_str(),
				Core::toString(pick->time().value()).c_str());

		return;
	}

	double lat, lon;

	try {
		lat = loc->latitude();
		lon = loc->longitude();
	}
	catch ( ... ) {
		SEISCOMP_ERROR("failed to get coordinates of %s.%s.%s at %s",
				pick->waveformID().networkCode().c_str(),
				pick->waveformID().stationCode().c_str(),
				pick->waveformID().locationCode().c_str(),
				Core::toString(pick->time().value()).c_str());

		return;
	}

	try {
		// if creation info is available, override author
		pick->creationInfo().setAuthor(pick->creationInfo().agencyID());
	}
	catch ( ... ) {
	}

	std::string data;

	{
		boost::iostreams::stream_buffer<boost::iostreams::back_insert_device<std::string> > buf(data);
		IO::BSONArchive ar(&buf, false, -1);
		ar & NAMED_OBJECT_HINT("latitude", lat, Core::Archive::STATIC_TYPE);
		ar & NAMED_OBJECT_HINT("longitude", lon, Core::Archive::STATIC_TYPE);
		ar & NAMED_OBJECT_HINT("pick", pick, Core::Archive::STATIC_TYPE);

		if ( !ar.success() )
			throw Core::GeneralException("failed to serialize pick");
	}

	bson_t bdata = BSON_INITIALIZER;

	if ( !bson_init_static(&bdata, (const uint8_t*) data.data(), data.size()) )
		throw Core::GeneralException("failed to serialize pick");

	std::string timestr = Core::toString(pick->time().value());

	bson_t bmsg = BSON_INITIALIZER;
	bson_append_utf8(&bmsg, "type", -1, "PICK", -1);
	bson_append_utf8(&bmsg, "queue", -1, "PICK", -1);
	bson_append_utf8(&bmsg, "starttime", -1, timestr.c_str(), -1);
	bson_append_utf8(&bmsg, "endtime", -1, timestr.c_str(), -1);
	bson_append_document(&bmsg, "data", -1, &bdata);

	std::string msg((char *) bson_get_data(&bmsg), bmsg.len);
	bson_destroy(&bmsg);

	IO::HttpSocket<IO::Socket> sock;

	for ( int retry = 0; retry < 2; ++retry ) {
		try {
			if ( _sid.length() == 0 )
				initSession();

			sock.setTimeout(SOCKET_TIMEOUT);
			sock.startTimer();
			sock.open(_serverHost, _user, _password);
			sock.httpPost(_serverPath + "send/" + _sid, msg);
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




// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
void Pick2HMB::sendAmplitude(DataModel::Amplitude* amp) {
	try {
		if ( amp->evaluationMode() == DataModel::MANUAL )
			return;
	}
	catch ( ... ) {
		/* return; */
	}

	DataModel::Stream *stream = Client::Inventory::Instance()->getStream(
			amp->waveformID().networkCode(),
			amp->waveformID().stationCode(),
			amp->waveformID().locationCode(),
			amp->waveformID().channelCode(),
			amp->timeWindow().reference());

	if ( stream == NULL ) {
		SEISCOMP_ERROR("cannot find stream %s.%s.%s.%s at %s",
				amp->waveformID().networkCode().c_str(),
				amp->waveformID().stationCode().c_str(),
				amp->waveformID().locationCode().c_str(),
				amp->waveformID().channelCode().c_str(),
				Core::toString(amp->timeWindow().reference()).c_str());

		return;
	}

        try {
		if ( stream->restricted() )
			return;
	}
	catch ( ... ) {
		SEISCOMP_ERROR("failed to get restricted status of %s.%s.%s.%s at %s",
				amp->waveformID().networkCode().c_str(),
				amp->waveformID().stationCode().c_str(),
				amp->waveformID().locationCode().c_str(),
				amp->waveformID().channelCode().c_str(),
				Core::toString(amp->timeWindow().reference()).c_str());

		return;
	}

	DataModel::SensorLocation* loc = Client::Inventory::Instance()->getSensorLocation(
			amp->waveformID().networkCode(),
			amp->waveformID().stationCode(),
			amp->waveformID().locationCode(),
			amp->timeWindow().reference());

	if ( loc == NULL ) {
		SEISCOMP_ERROR("cannot find location %s.%s.%s at %s",
				amp->waveformID().networkCode().c_str(),
				amp->waveformID().stationCode().c_str(),
				amp->waveformID().locationCode().c_str(),
				Core::toString(amp->timeWindow().reference()).c_str());

		return;
	}

	try {
		loc->latitude();
		loc->longitude();
	}
	catch ( ... ) {
		SEISCOMP_ERROR("failed to get coordinates of %s.%s.%s at %s",
				amp->waveformID().networkCode().c_str(),
				amp->waveformID().stationCode().c_str(),
				amp->waveformID().locationCode().c_str(),
				Core::toString(amp->timeWindow().reference()).c_str());

		return;
	}

	try {
		// if creation info is available, override author
		amp->creationInfo().setAuthor(amp->creationInfo().agencyID());
	}
	catch ( ... ) {
	}

	std::string data;

	{
		boost::iostreams::stream_buffer<boost::iostreams::back_insert_device<std::string> > buf(data);
		IO::BSONArchive ar(&buf, false, -1);
		ar & NAMED_OBJECT_HINT("amplitude", amp, Core::Archive::STATIC_TYPE);

		if ( !ar.success() )
			throw Core::GeneralException("failed to serialize amplitude");
	}

	bson_t bdata = BSON_INITIALIZER;

	if ( !bson_init_static(&bdata, (const uint8_t*) data.data(), data.size()) )
		throw Core::GeneralException("failed to serialize amplitude");

	std::string timestr = Core::toString(amp->timeWindow().reference());

	bson_t bmsg = BSON_INITIALIZER;
	bson_append_utf8(&bmsg, "type", -1, "AMPLITUDE", -1);
	bson_append_utf8(&bmsg, "queue", -1, "PICK", -1);
	bson_append_utf8(&bmsg, "starttime", -1, timestr.c_str(), -1);
	bson_append_utf8(&bmsg, "endtime", -1, timestr.c_str(), -1);
	bson_append_document(&bmsg, "data", -1, &bdata);

	std::string msg((char *) bson_get_data(&bmsg), bmsg.len);
	bson_destroy(&bmsg);

	IO::HttpSocket<IO::Socket> sock;

	for ( int retry = 0; retry < 2; ++retry ) {
		try {
			if ( _sid.length() == 0 )
				initSession();

			sock.setTimeout(SOCKET_TIMEOUT);
			sock.startTimer();
			sock.open(_serverHost, _user, _password);
			sock.httpPost(_serverPath + "send/" + _sid, msg);
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




// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
void Pick2HMB::handleMessage(Core::Message* msg)
{
	if ( Core::DataMessage* dataMessage = Core::DataMessage::Cast(msg) ) {
		for ( Core::DataMessage::iterator it = dataMessage->begin();
				it != dataMessage->end(); ++it ) {
			DataModel::Pick* pick = DataModel::Pick::Cast(*it);

			if ( pick ) {
				sendPick(pick);
			}
			else {
				DataModel::Amplitude* amp = DataModel::Amplitude::Cast(*it);

				if ( amp )
					sendAmplitude(amp);
			}
		}
	}
	else if ( DataModel::NotifierMessage* notifierMessage = DataModel::NotifierMessage::Cast(msg) ) {
		for ( DataModel::NotifierMessage::iterator it = notifierMessage->begin();
				it != notifierMessage->end(); ++it ) {
			DataModel::Notifier* notifier = DataModel::Notifier::Cast(*it);

			if ( notifier->operation() == DataModel::OP_ADD || notifier->operation() == DataModel::OP_UPDATE ) {
				DataModel::Pick* pick = DataModel::Pick::Cast(notifier->object());

				if ( pick ) {
					sendPick(pick);
				}
				else {
					DataModel::Amplitude* amp = DataModel::Amplitude::Cast(notifier->object());

					if ( amp )
						sendAmplitude(amp);
				}
			}
		}
	}
	else {
		// Unknown message
		return;
	}
}
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


} // namespace Applictions
} // namespace Seiscomp
