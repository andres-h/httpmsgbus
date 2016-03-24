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



#ifndef __SEISCOMP_COMMUNICATION_IMPORT_H__
#define __SEISCOMP_COMMUNICATION_IMPORT_H__


#include <string>
#include <map>
#include <boost/thread/thread.hpp>

#include <seiscomp3/client/application.h>

#include "bson.h"


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
