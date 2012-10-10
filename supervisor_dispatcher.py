

API_VERSION = 3
VERSION = 'dispatcher_0.1'
IDENTIFICATION = 'dispatcher'


def dispatch_by_name(method_name, postprocess=(lambda x, y: x)):
    """ Generic dispatch wrapper that chooses a child dependeng on the process name (first argument). """

    def wrapper(self, process_name, *args, **kwargs):
        
        child, base_name, host = self._decode_name(process_name)

        if child is None:
            raise Exception('invalid process name') # FIXME: Exception conventions?

        return postprocess(getattr(child, method_name)(base_name, *args, **kwargs), host)

    return wrapper


def dispatch_to_all(method_name, postprocess=(lambda x: True)):
    """ Generic dispatch wrapper that dispatches a call to all children. """

    def wrapper(self, *args, **kwargs):
        
        results = []
        for host, child in self.children:
            results.append((getattr(child, method_name)(*args, **kwargs), host))
            
        return postprocess(results)

    return wrapper

def postprocess_process_info(info, host):
    info2 = info.copy()
    info2['name'] = info['name'] + "@" + host
    return info2

def postprocess_results(results):
    return [ postprocess_process_info(info, host) for infos, host in results for info in infos ]


class Dispatcher(object):
    """ Supervisor proxy that forwards requests to a number of supervisor processes.

    Results of requests are aggregated, such that several supervisors
    can be controlled centrally, even if they run on different hosts."""

    def __init__(self, children):
        self.children = children
        self.delegate_dict = dict(children)


    def _decode_name(self, name):
        """Splits a compound name of the form "process@host" into a plain process name and the
        delegate supervisor corresponding to the host.

        @return a pair of a supervisor and a process name. When no suffix is present or the supervisor is not known, it is None"""

        split_name = name.split('@', 1)
        if len(split_name) < 2:
            return None, name

        plain_name, host = split_name
        delegate = self.delegate_dict.get(host)
        return (delegate, plain_name, host)


    def getAPIVersion(self):
        return API_VERSION

    def getSupervisorVersion(self):
        return VERSION

    def getIdentification(self):
        return IDENTIFICATION


    def getState(self):
        raise NotImplementedError() # FIXME 

    def getPID(self):
        raise NotImplementedError() # FIXME 
        
    def readLog(self, offset, length):
        """ FIXME: Conceptually, the logs should be merged here, but it is not obvious how to
        do this. """
        raise NotImplementedError()

    clearLog = dispatch_to_all("clearLog")

    def shutdown(self):
        """ FIXME: One could dispatch this to all servers or shut down just the dispatcher... """
        raise NotImplementedError()

    restart = dispatch_to_all("restart")
    reloadConfig = dispatch_to_all("reloadConfig")
    addProcessGroup = dispatch_to_all("addProcessGroup")
    removeProcessGroup = dispatch_to_all("removeProcessGroup")

    def startProcess(self, name, wait=True):

        child, base_name = self._decode_name(name)

        if child is not None:
            return child.startProcess(base_name, wait)
        else:
            for _, child in self.children:
                child.startProcess(base_name)
                return True

    def stopProcess(self, name, wait=True):

        child, base_name = self._decode_name(name)

        if child is not None:
            return child.stopProcess(base_name, wait)
        else:
            for _, child in self.children:
                child.stopProcess(base_name)
                return True


    ### Bulk start/stop ###

    startProcessGroup = dispatch_to_all("startProcessGroup", postprocess_results)
    startAllProcesses = dispatch_to_all("startAllProcesses", postprocess_results)
    stopProcessGroup = dispatch_to_all("stopProcessGroup", postprocess_results)
    stopAllProcesses = dispatch_to_all("stopAllProcesses", postprocess_results)


    ### Information Queries ###

    getAllConfigInfo = dispatch_to_all("getAllConfigInfo", postprocess_results)
    getProcessInfo = dispatch_by_name("getProcessInfo", postprocess_process_info)
    getAllProcessInfo = dispatch_to_all("getAllProcessInfo", postprocess_results)

    readProcessStdoutLog = dispatch_by_name("readProcessStdoutLog")
    readProcessStderrLog = dispatch_by_name("readProcessStderrLog")
    tailProcessStdoutLog = dispatch_by_name("tailProcessStdoutLog")
    tailProcessStderrLog = dispatch_by_name("tailProcessStderrLog")

    clearProcessLogs = dispatch_by_name("clearProcessLogs")
    clearAllProcessLogs = dispatch_to_all("clearAllProcessLogs")

    sendProcessStdin = dispatch_by_name("sendProcessStdin")
        
    def sendRemoteCommEvent(self, type, data):
        pass


        

        
    
