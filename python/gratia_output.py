import argparse
import datetime

from KAPELConfig import KAPELConfig
from prometheus_api_client import PrometheusConnect
from dirq.QueueSimple import QueueSimple
import os
from datetime import datetime, timezone 
import re

from gratia.common.Gratia import DebugPrint
from gratia.common.debug import DebugPrintTraceback
import gratia.common.GratiaCore as GratiaCore
import gratia.common.GratiaWrapper as GratiaWrapper
import gratia.common.Gratia as Gratia
import gratia.common.config as config

probe_version = "%%%RPMVERSION%%%"

probe_name = os.path.basename(os.path.dirname(os.path.abspath(__file__)))

# log levels
CRITICAL = 0
ERROR    = 1
WARNING  = 2
INFO     = 3
DEBUG    = 4

# Gratia record constants 
SECONDS = "Was entered in seconds"
USER = "user"
RESOURCE_TYPE = "Batch"

BATCH_SIZE = 1000

class ApelRecordConverter():
    apel_dict: dict

    def __init__(self, apel_record: str):
        self.apel_dict = self._parse_apel_str(apel_record)

    def _parse_apel_str(self, apel_record: str):
        lines = apel_record.split('\n')
        for line in lines:
            kv_pair = [v.strip() for v in line.split(':')]
            if len(kv_pair) == 2:
                self.apel_dict[kv_pair[0]] = kv_pair[1]

    def getint(self, key):
        return int(self.apel_dict.get(key, 0))

    def get(self, key):
        return self.apel_dict.get(key)

    def site_probe(self):
        # TODO osg-pilot-container prefix may not always be correct
        site_dns = re.sub(r'[^a-zA-Z0-9-]', '-', self.get('Site')).strip('-')  # sanitize site
        return "osg-pilot-container:%s.gratia.opensciencegrid.org" % site_dns

    def to_gratia_record(self):
        # TODO the following fields are not currently tracked:
        #      memory, machine name, grid
        r = Gratia.UsageRecord(RESOURCE_TYPE)
        r.StartTime(   self.getint('StartTime'), SECONDS)
        r.EndTime(     self.getint('LatestEndTime'), SECONDS)
        r.WallDuration(self.getint('WallDuration'), SECONDS)
        r.CpuDuration( self.getint('CpuDuration'), USER, SECONDS)
        r.Processors(  self.getint('Processors'), SECONDS)
        r.SiteName(    self.get('Site'))
        r.ProbeName(   self.site_probe())
        r.Grid(        self.get('InfrastructureType')) # Best guess
        r.VOName(      self.get('VO'))
        r.ReportableVOName(      self.get('VO'))

        return r


def send_gratia_records(records: list[ApelRecordConverter]):
    # TODO the assumption of uniform site/probe might not be true
    site = records[0].get('Site')
    probe = records[0].site_probe()

    # GratiaCore.Initialize(gratia_config)

    config.Config.setSiteName(site)
    config.Config.setMeterName(probe)

    GratiaCore.Handshake()

    try:
        GratiaCore.SearchOutstandingRecord()
    except Exception as e:
        DebugPrint(f"Failed to search outstanding records: {e}")
        raise

    GratiaCore.Reprocess()

    for record in records:
        GratiaCore.Send(record)

    GratiaCore.ProcessCurrentBundle()
            



def setup_gratia(config: KAPELConfig):

    if not config.gratia_config_path or not os.path.exists(config.gratia_config_path):
        raise Exception("No valid gratia config path given")
    GratiaCore.Config = GratiaCore.ProbeConfiguration(config.gratia_config_path)

    GratiaWrapper.CheckPreconditions()
    GratiaWrapper.ExclusiveLock()

    # Register gratia
    GratiaCore.RegisterReporter(config.gratia_reporter)
    GratiaCore.RegisterService(config.gratia_service)
    GratiaCore.setBatchProbeManager(config.gratia_probe_manager)

    GratiaCore.Initialize(config.gratia_config_path)

def main(envFile: str):
    print(f'Starting Gratia post-processor: {__file__} with envFile {envFile} at {datetime.now(tz=timezone.utc).isoformat()}')
    cfg = KAPELConfig(envFile)

    setup_gratia(cfg)

    dirq = QueueSimple(str(cfg.output_path))
    records = []
    for name in dirq:
        if not dirq.lock(name):
            continue
        records.append(ApelRecordConverter(dirq.get(name)).to_gratia_record())

        dirq.remove(name)

    send_gratia_records(records)

    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Kubernetes job accounting data from Prometheus and prepare it for APEL publishing.")
    # This should be the only CLI argument, since all other config should be specified via env.
    parser.add_argument("-e", "--env-file", default=None, help="name of file containing environment variables for configuration")
    args = parser.parse_args()
    main(args.env_file)
