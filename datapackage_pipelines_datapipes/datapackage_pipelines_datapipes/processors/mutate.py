import tableschema
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_datapipes.common import Logger, LoggerImpl


def process_datapackage(datapackage, parameters):
    resource = datapackage['resources'][0]

    fields = resource.setdefault('schema', {}).get('fields', [])
    for field in fields:
        if field['name'] == parameters['field']:
            field.update(parameters['options'])

    resource['schema']['fields'] = fields

    return datapackage


def process_resource(spec, rows, parameters, logger: LoggerImpl):
    mutated_field = parameters['field']
    fields = spec['schema']['fields']
    schema = {
        'fields': list(filter(lambda f: f['name'] == mutated_field,
                              fields))
    }
    jts = tableschema.Schema(schema).fields[0]
    bad_count = 0
    for i, row in enumerate(rows):
        if bad_count > 100:
            yield row
            continue

        value = row.get(mutated_field)
        try:
            value = jts.cast_value(value)
            row[mutated_field] = value
            yield row

        except Exception:
            bad_count += 1
            logger.bad_value(spec['name'], i, dict(row), mutated_field, row.get(mutated_field))
            yield row


def process_resources(resource_iterator, parameters, logger):
    for resource in resource_iterator:
        yield process_resource(resource.spec, resource, parameters, logger)


def main():
    parameters, dp, res_iter = ingest()
    with Logger(parameters) as logger:
        dp = process_datapackage(dp, parameters)
        spew(dp, logger.log_rows(dp,
                                 process_resources(res_iter, parameters, logger)))


if __name__ == '__main__':
    main()