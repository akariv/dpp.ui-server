import logging
import json
import os

from datapackage_pipelines.utilities.extended_json import LazyJsonLine

only_last = os.environ.get('DATAPIPES_DOWNLOAD') is not None


class Logger:

    def __init__(self, parameters):
        self.uuid = parameters['uuid']

    def _send(self, msg):
        msg['uuid'] = self.uuid
        if only_last and self.uuid == 'last':
            logging.info(json.dumps(msg))
        elif not only_last and self.uuid != 'last':
            logging.info(json.dumps(msg))

    def _event(self, ev, **kwargs):
        kwargs['e'] = ev
        self._send(kwargs)

    def start(self):
        self._event('start')

    def error(self, msg):
        self._event('err', msg=msg)

    def bad_value(self, res, idx, data, field, value):
        self._event('ve', res=res, idx=idx, data=data, field=field, value=value)

    def done(self):
        self._event('done')

    def log_rows(self, dp, res_iter):
        def res_logger(spec_, res_):
            last = []
            self._event('rs', data=spec_['schema']['fields'])
            for i, row in enumerate(res_):
                if i < 50 or i % 100 == 0 or only_last:
                    if isinstance(row, LazyJsonLine):
                        row = dict(row)
                    self._event('r', res=spec_['name'], idx=i, data=row)
                    last = []
                else:
                    last.append((i, row))
                    if len(last) > 5:
                        last.pop(0)
                yield row
            for i, row in last:
                if isinstance(row, LazyJsonLine):
                    row = dict(row)
                self._event('r', res=spec_['name'], idx=i, data=row)

        for spec, res in zip(dp['resources'], res_iter):
            yield res_logger(spec, res)
