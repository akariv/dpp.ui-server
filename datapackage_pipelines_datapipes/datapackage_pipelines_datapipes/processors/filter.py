import tableschema
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_datapipes.common import Logger

OPS = {
    'is': lambda a, v: a == v,
    'isnt': lambda a, v: a != v,
    'gt': lambda a, v: a is not None and v is not None and a < v,
    'lt': lambda a, v: a is not None and v is not None and a > v,
    'gte': lambda a, v: a is not None and v is not None and a <= v,
    'lte': lambda a, v: a is not None and v is not None and a >= v,
}

def process_one(rows, field, op, arg):
    spec = rows.spec
    mutated_field = field
    fields = spec['schema']['fields']
    schema = {
        'fields': list(filter(lambda f: f['name'] == mutated_field,
                              fields))
    }
    jts = tableschema.Schema(schema).fields[0]
    arg = jts.cast_value(arg)
    op = OPS[op]

    for row in rows:
        if op(arg, row.get(field)):
            yield row


def process_resources(res_iter, field, op, arg):
    for res in res_iter:
        yield process_one(res, field, op, arg)


def main():
    parameters, dp, res_iter = ingest()
    with Logger(parameters) as logger:
        field, op, arg = parameters['field'], parameters['op'], parameters['arg']
        spew(dp, logger.log_rows(dp, process_resources(res_iter, field, op, arg)))

if __name__ == '__main__':
    main()