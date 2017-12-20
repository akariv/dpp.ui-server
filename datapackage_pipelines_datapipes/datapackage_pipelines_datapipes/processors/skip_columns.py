from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_datapipes.common import Logger

def process_one(rows, columns_to_remove):
    for row in rows:
        for col in columns_to_remove:
            if col in row:
                del row[col]
        yield row


def process_resources(res_iter, columns_to_remove):
    for res in res_iter:
        yield process_one(res, columns_to_remove)


def main():
    parameters, dp, res_iter = ingest()
    with Logger(parameters) as logger:
        logger.start()
        columns_to_remove = []
        for _ in range(parameters['amount']):
            f = dp['resources'][0]['schema']['fields'].pop(0)
            columns_to_remove.append(f['name'])
        spew(dp, logger.log_rows(dp, process_resources(res_iter, columns_to_remove)))

if __name__ == '__main__':
    main()