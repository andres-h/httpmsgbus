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



#ifndef __PICK2HMB_H__
#define __PICK2HMB_H__


#include <string>
#include <map>
#include <boost/thread/thread.hpp>

#include <seiscomp/client/application.h>

extern "C" {
	#include "bson/bson.h"
}


namespace Seiscomp {
namespace Applications {


// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
class Pick2HMB : public Client::Application {

	// ----------------------------------------------------------------------
	// X'struction
	// ----------------------------------------------------------------------
public:
	Pick2HMB(int argc, char* argv[]);
	~Pick2HMB();

	// ------------------------------------------------------------------
	// Private interface
	// ------------------------------------------------------------------
private:
	virtual bool init();
	virtual void createCommandLineDescription();
	virtual void handleMessage(Core::Message* msg);
	virtual void done();

	std::string bsonGetString(const bson_t *bson, const char *key);
	void initSession();
	void sendPick(DataModel::Pick* pick);
	void sendAmplitude(DataModel::Amplitude* amp);

	// ------------------------------------------------------------------
	// Private implementation
	// ------------------------------------------------------------------
private:
	std::string _serverHost;
	std::string _serverPath;
	std::string _user;
	std::string _password;
	std::string _sid;
	std::string _cid;
};
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


} // namepsace Applications
} // namespace Seiscomp

#endif
