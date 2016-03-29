import os, resource
import seiscomp3.Kernel, seiscomp3.Config

class Module(seiscomp3.Kernel.CoreModule):
    def __init__(self, env):
        seiscomp3.Kernel.CoreModule.__init__(self, env, env.moduleName(__file__))

        # Increase kill timeout to 40 seconds
        self.killTimeout = 40

        # Default values
        self.hmbEnable = False
        self.hmbPort = 8000

        try: self.hmbEnable = self.env.getBool("hmb.enable")
        except: pass
        try: self.hmbPort = self.env.getInt("hmb.port")
        except: pass

    def start(self):
        if not self.hmbEnable:
            print "[kernel] HMB is disabled by config"
            return 0

        seiscomp3.Kernel.CoreModule.start(self)

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
        try:
            lim = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (lim[1], lim[1]))

            lim = resource.getrlimit(resource.RLIMIT_NOFILE)
            print " maximum number of open files set to", lim[0]

        except Exception, e:
            print " failed to raise the maximum number of open files:", str(e)

        cfg = self._readConfig()
        prog = "run_with_lock"
        params = self.env.lockFile(self.name) + ' ' + self.env.binaryFile(self.name)
        params += ' -P %d' % self.hmbPort
        try: params += ' -D "%s"' % cfg.getString('database')
        except: params += ' -D "filedb://%s/var/lib/httpmsgbus?blocksize=1024&blocksPerFile=16384"' % self.env.SEISCOMP_ROOT
        try: params += ' -b %d' % cfg.getInt('bufferSize')
        except: pass
        try: params += ' -q %d' % cfg.getInt('queueSize')
        except: pass
        try: params += ' -p %d' % cfg.getInt('postSize')
        except: pass
        try: params += ' -t %d' % cfg.getInt('sessionTimeout')
        except: pass
        try: params += ' -c %d' % cfg.getInt('sessionsPerIP')
        except: pass
        try: params += ' -d %d' % cfg.getInt('delta')
        except: pass
        try: params += ' -F' * cfg.getBool('useXFF')
        except: pass
        if self.env.syslog: params += ' -s'
        return self.env.start(self.name, prog, params, True)

