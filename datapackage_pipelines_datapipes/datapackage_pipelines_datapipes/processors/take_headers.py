from itertools import chain

import tableschema
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_datapipes.common import Logger


def process_datapackage(datapackage, headers):
    resource = datapackage['resources'][0]

    fields = resource.setdefault('schema', {}).setdefault('fields', [])
    for field in fields:
        field['name'] = headers[field['name']]

    return datapackage


def process_resource(rows, headers):
    for i, row in enumerate(rows):
        row = dict(
            (headers[k], v)
            for k, v
            in row.items()
        )
        yield row


def process_resources(resource_iterator, headers):
    for resource in resource_iterator:
        yield process_resource(resource, headers)


def main():
    parameters, dp, res_iter = ingest()
    logger = Logger(parameters)
    logger.start()

    first_res = next(res_iter)
    headers = next(first_res)

    dp = process_datapackage(dp, headers)

    spew(dp, logger.log_rows(dp,
                             process_resources(chain([first_res], res_iter), headers)))

    logger.done()

if __name__ == '__main__':
    main()