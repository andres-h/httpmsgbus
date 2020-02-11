import os
import seiscomp.kernel, seiscomp.config

class Module(seiscomp.kernel.Module):
    def __init__(self, env):
        seiscomp.kernel.Module.__init__(self, env, env.moduleName(__file__))

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

        seiscomp.kernel.Module.start(self)

    def _readConfig(self):
        cfg = seiscomp.config.Config()

        # Defaults Global + App Cfg
        cfg.readConfig(os.path.join(self.env.SEISCOMP_ROOT, "etc", "defaults", "global.cfg"))
        cfg.readConfig(os.path.join(self.env.SEISCOMP_ROOT, "etc", "defaults", self.name + ".cfg"))

        # Config Global + App Cfg
        cfg.readConfig(os.path.join(self.env.SEISCOMP_ROOT, "etc", "global.cfg"))
        cfg.readConfig(os.path.join(self.env.SEISCOMP_ROOT, "etc", self.name + ".cfg"))

        # User Global + App Cfg
        cfg.readConfig(os.path.join(os.environ['HOME'], ".seiscomp", "global.cfg"))
        cfg.readConfig(os.path.join(os.environ['HOME'], ".seiscomp", self.name + ".cfg"))

        return cfg

    def _run(self):
        cfg = self._readConfig()
        prog = "run_with_lock"
        params = self.env.lockFile(self.name) + ' ' + self.env.binaryFile(self.name)
        params += ' -H http://localhost:%d/wave' % self.hmbPort
        try: params += ' -O "%s"' % cfg.getString('organization')
        except: pass
        try: params += ' -P %s' % cfg.getString('port')
        except: pass
        try: params += ' -t %d' % cfg.getInt('timeout')
        except: pass
        try: params += ' -c %d' % cfg.getInt('connectionsPerIP')
        except: pass
        try: params += ' -q %d' % cfg.getInt('qlen')
        except: pass
        try: params += ' -w %d' % cfg.getInt('oowait')
        except: pass
        if self.env.syslog: params += ' -s'
        return self.env.start(self.name, prog, params, True)

