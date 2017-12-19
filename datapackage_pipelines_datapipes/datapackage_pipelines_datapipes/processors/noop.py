from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_datapipes.common import Logger


def main():
    parameters, dp, res_iter = ingest()
    logger = Logger(parameters)
    logger.start()

    spew(dp, logger.log_rows(dp, res_iter))

    logger.done()

if __name__ == '__main__':
    main()