from itertools import chain, islice
import time

import logging
import tabulator
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_datapipes.common import Logger


def slower(rows__):
    for row in rows__:
        # logging.info('SLOWER %r', row)
        yield row
        # time.sleep(0)


def load(logger, dp, url, res_name):
    try:
        s = tabulator.Stream(url)
        s.open()
        logging.info('OPENED %s', url)
        rows = slower(s.iter())
        # rows = s.iter()
        canary = list(islice(rows, 0, 5))
        maxlen = max(len(r) for r in canary)
        headers = ['Col%d' % i for i in range(1, maxlen+1)]
        dp['resources'].append({
            PROP_STREAMING: True,
            'name': res_name,
            'path': '.',
            'schema': {
                'fields': [
                    {'name': h, 'type': 'string'}
                    for h in headers
                ]
            }
        })
        rows = chain(canary, rows)
        rows = map(lambda row: dict(zip(headers, row)), rows)

        def aux(rows_):
            yield from rows_ # islice(rows_,10)

        return aux(rows)
    except:
        logger.error('Failed to load from source')
        raise


def main():
    parameters, dp, res_iter = ingest()
    logger = Logger(parameters)
    logger.start()

    logging.info('Will now spew')
    spew(dp, logger.log_rows(dp,
                             chain(res_iter,
                                   [load(logger, dp, parameters['url'], parameters['res_name'])]
                             )
             )
    )

    logger.done()

if __name__ == '__main__':
    main()