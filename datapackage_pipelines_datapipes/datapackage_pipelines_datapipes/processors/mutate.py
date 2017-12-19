import tableschema
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_datapipes.common import Logger


def process_datapackage(datapackage, parameters):
    resource = datapackage['resources'][0]

    fields = resource.setdefault('schema', {}).get('fields', [])
    for field in fields:
        if field['name'] == parameters['field']:
            field.update(parameters['options'])

    resource['schema']['fields'] = fields

    return datapackage


def process_resource(spec, rows, parameters, logger: Logger):
    mutated_field = parameters['field']
    fields = spec['schema']['fields']
    schema = {
        'fields': list(filter(lambda f: f['name'] == mutated_field,
                              fields))
    }
    jts = tableschema.Schema(schema)
    for i, row in enumerate(rows):
        flattened_row = [row.get(mutated_field)]
        try:
            flattened_row = jts.cast_row(flattened_row)
            row[mutated_field] = flattened_row[0]
            yield row

        except Exception:
            logger.bad_value(spec['name'], i, dict(row), mutated_field, row.get(mutated_field))
            yield row


def process_resources(resource_iterator, parameters, logger):
    for resource in resource_iterator:
        yield process_resource(resource.spec, resource, parameters, logger)


def main():
    parameters, dp, res_iter = ingest()
    logger = Logger(parameters)
    logger.start()

    dp = process_datapackage(dp, parameters)

    spew(dp, logger.log_rows(dp,
                             process_resources(res_iter, parameters, logger)))

    logger.done()

if __name__ == '__main__':
    main()