from __future__ import print_function
import os, re, sys
import seiscomp3.Kernel, seiscomp3.Config, seiscomp3.System
import seiscomp3.DataModel, seiscomp3.IO
import bson
from hmb.client import HMB

def log(message, *args, **kwargs):
    print(" ", message, *args, **kwargs)
    sys.stdout.flush()

def loadDatabase(dbUrl):
    """
    Load inventory from a database, but only down to the station level.
    """
    m = re.match("(?P<dbDriverName>^.*):\/\/(?P<dbAddress>.+?:.+?@.+?\/.+$)", dbUrl)
    if not m:
        raise Exception("error in parsing SC3 DB URL")
    db = m.groupdict()
    try:
        registry = seiscomp3.Client.PluginRegistry.Instance()
        registry.addPluginName("db" + db["dbDriverName"])
        registry.loadPlugins()
    except Exception, e:
        raise(e) ### "Cannot load database driver: %s" % e)
    dbDriver = seiscomp3.IO.DatabaseInterface.Create(db["dbDriverName"])
    if dbDriver is None:
        raise Exception("Cannot find database driver " + db["dbDriverName"])
    if not dbDriver.connect(db["dbAddress"]):
        raise Exception("Cannot connect to database at " + db["dbAddress"])
    dbQuery = seiscomp3.DataModel.DatabaseQuery(dbDriver)
    if dbQuery is None:
        raise Exception("Cannot get DB query object")
    log("loading inventory from database ...", end="")
    inventory = seiscomp3.DataModel.Inventory()
    dbQuery.loadNetworks(inventory)
    for ni in xrange(inventory.networkCount()):
        dbQuery.loadStations(inventory.network(ni))
    log("done", sep="")
    return inventory

def loadStationDescriptions(inv):
    """From an inventory, prepare a dictionary of station code descriptions.

    In theory, we should only use stations with current time windows.

    """
    d = dict()

    for ni in xrange(inv.networkCount()):
        n = inv.network(ni)
        net = n.code()
        if not d.has_key(net):
            d[net] = {}

            for si in xrange(n.stationCount()):
                s = n.station(si)
                sta = s.code()
                d[net][sta] = s.description()

                try:
                    end = s.end()
                except:  # ValueException ???
                    end = None
                #print "Found in inventory:", net, sta, end, s.description()
    return d

def loadStationsHMB(hmbAddr):
    """
    Load station descriptions and IP access lists from HMB.
    """
    stations = {}

    hmbParam = {
        'queue': {
            'STATION_CONFIG': {
                'seq': 0
            }
        }
    }

    hmb = HMB(hmbAddr, hmbParam, log_fn=log)

    while True:
        for obj in hmb.recv():
            try:
                if obj['type'] == 'STATION_CONFIG':
                    data = obj['data']
                    stations[(data['networkCode'], data['stationCode'])] = (data['description'], tuple(data['access']))

                elif obj['type'] == 'EOF':
                    log("got", len(stations), "stations")
                    return stations, hmb

            except (KeyError, TypeError) as e:
                log("invalid data received:", str(e))

def collectParams(container):
    params = {}

    for i in range(container.groupCount()):
        params.update(collectParams(container.group(i)))

    for i in range(container.structureCount()):
        params.update(collectParams(container.structure(i)))

    for i in range(container.parameterCount()):
        p = container.parameter(i)

        if p.symbol.stage == seiscomp3.System.Environment.CS_UNDEFINED:
            continue

        params[p.variableName] = p.symbol.values

    return params

class Module(seiscomp3.Kernel.Module):
    def __init__(self, env):
        seiscomp3.Kernel.Module.__init__(self, env, env.moduleName(__file__))
        self.rcdir = os.path.join(self.env.SEISCOMP_ROOT, "var", "lib", "rc")
        self.seedlink_station_descr = {}

        # Default values
        self.hmbEnable = False
        self.hmbPort = 8000

        try: self.hmbEnable = self.env.getBool("hmb.enable")
        except: pass
        try: self.hmbPort = self.env.getInt("hmb.port")
        except: pass

    def start(self):
        if not self.hmbEnable:
            return 0

        seiscomp3.Kernel.Module.start(self)

    def _readConfig(self):
        cfg = seiscomp3.Config.Config()

        # Defaults Global + App Cfg
        cfg.readConfig(os.path.join(self.env.SEISCOMP_ROOT, "etc", "defaults", "global.cfg"))
        cfg.readConfig(os.path.join(self.env.SEISCOMP_ROOT, "etc", "defaults", self.name + ".cfg"))

        # Config Global + App Cfg
        cfg.readConfig(os.path.join(self.env.SEISCOMP_ROOT, "etc", "global.cfg"))
        cfg.readConfig(os.path.join(self.env.SEISCOMP_ROOT, "etc", self.name + ".cfg"))

        # User Global + App Cfg
        cfg.readConfig(os.path.join(os.environ['HOME'], ".seiscomp3", "global.cfg"))
        cfg.readConfig(os.path.join(os.environ['HOME'], ".seiscomp3", self.name + ".cfg"))

        return cfg

    def _run(self):
        cfg = self._readConfig()
        prog = "run_with_lock"
        params = self.env.lockFile(self.name) + ' ' + self.env.binaryFile(self.name)
        pbin = os.path.join(self.env.SEISCOMP_ROOT, "share", "plugins", "seedlink", "chain_plugin")
        pconf = os.path.join(self.env.SEISCOMP_ROOT, "var", "lib", "seedlink", "chain0.xml")
        psys = ' -D' if self.env.syslog else ''
        params += ' -C "%s%s -f %s chain0"' % (pbin, psys, pconf)
        params += ' -H http://localhost:%d/wave' % self.hmbPort
        try: params += ' -X "%s"' % cfg.getString('unreliableChannelsRegex')
        except: params += ' -X "_AE|_[^D]$"'
        try: params += ' -b %d' % cfg.getInt('bufferSize')
        except: pass
        try: params += ' -t %d' % cfg.getInt('timeout')
        except: pass
        if self.env.syslog: params += ' -s'
        return self.env.start(self.name, prog, params, True)

    def _getStationDescription(self, net, sta):
        # Supply station description:
        # 1. try getting station description from a database
        # 2. read station description from seiscomp3/var/lib/rc/station_NET_STA
        # 3. if not set, use the station code

        description = ""

        if len(self.seedlink_station_descr) > 0:
            try:
                description = self.seedlink_station_descr[net][sta]
            except KeyError:
                pass

        if len(description) == 0:
            try:
                rc = seiscomp3.Config.Config()
                rc.readConfig(os.path.join(self.rcdir, "station_%s_%s" % (net, sta)))
                description = rc.getString("description")
            except Exception, e:
                # Maybe the rc file doesn't exist, maybe there's no readable description.
                pass

        if len(description) == 0:
                description = sta

        return description

    def updateConfig(self):
        # If HMB is disabled, do not do anything
        if not self.hmbEnable:
            print("- HMB is disabled, nothing to do")
            return 0

        hmbAddr = "http://localhost:%d/wave" % self.hmbPort

        # Initialize the basic directories
        descdir = os.path.join(self.env.SEISCOMP_ROOT, "etc", "descriptions")
        keydir = os.path.join(self.env.SEISCOMP_ROOT, "etc", "key", "seedlink")

        # Load definitions of the configuration schema
        defs = seiscomp3.System.SchemaDefinitions()
        if defs.load(descdir) == False:
            log("could not read descriptions")
            return False

        if defs.moduleCount() == 0:
            log("no modules defined, nothing to do")
            return False

        # Create a model from the schema and read its configuration including
        # all bindings.
        model = seiscomp3.System.Model()
        model.create(defs)
        model.readConfig()

        mod = model.module("seedlink")

        if mod is None:
            return 0

        log("loading station config from hmb://localhost:%d/wave" % self.hmbPort)
        stations, hmb = loadStationsHMB(hmbAddr)

        # Load station descriptions from inventory
        cfg = self._readConfig()

        try: dbUrl = cfg.getString('inventory_connection')
        except: dbUrl = None

        if dbUrl:
            log("loading station descriptions from %s" % dbUrl)
            inv = loadDatabase(dbUrl)
            self.seedlink_station_descr = loadStationDescriptions(inv)

        # Loop over Seedlink bindings and generate a list of update messages
        configUpdate = []

        for staid in mod.bindings.keys():
            binding = mod.getBinding(staid)
            if not binding:
                continue

            params = {}
            for i in range(binding.sectionCount()):
                params.update(collectParams(binding.section(i)))

            description = self._getStationDescription(staid.networkCode, staid.stationCode)
            access = []

            for a in params.get('access', []):
                try:
                    access.append(bson.Binary(bytes(bytearray(int(x) for x in re.split('[./]', a)))))

                except ValueError:
                    log("invalid IP/mask in access list:", a)

            access.sort()

            s = stations.pop((staid.networkCode, staid.stationCode), None)

            if s is None:
                log("adding", description)

            elif s != (description, tuple(access)):
                log("updating", description)

            else:
                continue

            msg = {
                'type': 'STATION_CONFIG',
                'queue': 'STATION_CONFIG',
                'data': {
                    'networkCode': staid.networkCode,
                    'stationCode': staid.stationCode,
                    'description': description,
                    'access': access
                }
            }

            configUpdate.append(msg)

        # Mark remaining stations deleted using inexistent IP 0.0.0.0
        for ((networkCode, stationCode), (description, access)) in stations.items():
            if tuple(access) == (bson.Binary(bytes(bytearray((0, 0, 0, 0)))),):
                continue

            log("deleting", description)

            msg = {
                'type': 'STATION_CONFIG',
                'queue': 'STATION_CONFIG',
                'data': {
                    'networkCode': networkCode,
                    'stationCode': stationCode,
                    'description': description,
                    'access': [ bson.Binary(bytes(bytearray((0, 0, 0, 0)))) ]
                }
            }

            configUpdate.append(msg)

        if configUpdate:
            log("sending update to hmb://localhost:%d/wave" % self.hmbPort)
            hmb.send(configUpdate)

        else:
            log("no changes")

        log("done")
        return 0

