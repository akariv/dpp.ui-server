import os
import json

from datapackage_pipelines.generators import (
    GeneratorBase,
    steps,
    slugify,
    SCHEDULE_DAILY
)

import logging
log = logging.getLogger(__name__)


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/generator_spec_schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source, base):
        pipeline = []
        for action in source.get('actions', []):
            uuid, verb, options = action['uuid'], action['verb'], action['options']

            def step(processor, params):
                params['uuid'] = uuid
                params['revision'] = options['revision']
                return (processor,
                        params,
                        True)

            if verb == 'source':
                pipeline.append(step('datapipes.load_source',
                                     {'url': options['url'], 'res_name': uuid}))
            elif verb == 'skip':
                if options['kind'] == 'rows':
                    pipeline.append(step('datapipes.skip_rows',
                                         {'amount': options['amount']}))
                elif options['kind'] == 'columns':
                    pipeline.append(step('datapipes.skip_columns',
                                         {'amount': options['amount']}))
            elif verb == 'mutate':
                pipeline.append(step('datapipes.mutate',
                                     {'field': options['field'],
                                      'options': options['options']}))
            elif verb == 'filter':
                pipeline.append(step('datapipes.filter',
                                     {
                                         'field': options['field'],
                                         'op': options['op'],
                                         'arg': options['arg'],
                                     }
                                ))
                pipeline.append(step('datapipes.noop', {}))
            elif verb == 'headers':
                pipeline.append(step('datapipes.take_headers', {}))
            elif verb == 'noop':
                pipeline.append(step('datapipes.noop', {}))

        pipeline.append(('datapipes.noop', {'uuid': 'last'}, False))

        yield os.path.join(base, 'dp'), {
            'pipeline':
                steps(
                    ('datapipes.init', ),
                    *pipeline,
                )
        }