import codecs
import csv
import asyncio
import uuid
import os

import yaml
import datetime
from aiohttp import web
from aiohttp_sse import sse_response
from copy import copy

from datapackage_pipelines.utilities.extended_json import json
from datapackage_pipelines.lib.dump.file_formats import CSVFormat

BASE_PATH = os.environ.get('BASE_PATH', '/var/datapip.es')


def path_for_id(id, *args):
    return os.path.join(BASE_PATH, id, *args)


class LineReader:
    def __init__(self, f):
        self.f = f

    def __aiter__(self):
        return self

    async def __anext__(self):
        line = await self.f.readline()
        if line == b'':
            raise StopAsyncIteration
        else:
            line : str = line.decode('utf8')
            s = line.find('{')
            if s >= 0:
                stripped = line[s:].strip()
                try:
                    json.loads(stripped)
                    return stripped
                except json.JSONDecodeError:
                    print('??', line)
                    pass
            else:
                print('>>', line)


class ProcessRunner:
    def __init__(self, loop, id, full=False):
        self.loop = loop
        self.id = id
        self.dppdb_filename = datetime.datetime.now().isoformat() + '.db'
        self.env = os.environ.copy()
        self.env['DPP_DB_FILENAME'] = self.dppdb_filename
        if full:
            self.env['DATAPIPES_DOWNLOAD'] = 'yes'

    async def __aenter__(self):
        self.process = \
            await asyncio.create_subprocess_exec('/usr/local/bin/dpp', 'run', '--verbose', './dp',
                                                 cwd=path_for_id(self.id),
                                                 loop=self.loop,
                                                 stdin=asyncio.subprocess.DEVNULL,
                                                 stderr=asyncio.subprocess.PIPE,
                                                 stdout=asyncio.subprocess.DEVNULL,
                                                 env=self.env)
        return self.process

    async def __aexit__(self, *args):
        try:
            self.process.kill()
        except ProcessLookupError:
            pass
        await self.process.wait()
        if self.process.returncode != 0:
            print('RETCODE', self.process.returncode)
            raise ChildProcessError(self.process.returncode)
        try:
            os.remove(os.path.join(path_for_id(self.id), self.dppdb_filename))
        except FileNotFoundError:
            pass
        return


CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
}

async def events(request: web.Request):
    loop = request.app.loop

    uuid = request.match_info['id']
    async with sse_response(request, headers=CORS_HEADERS) as resp:
        try:
            async with ProcessRunner(loop, uuid) as process:
                print('starting!', uuid)
                async for line in LineReader(process.stderr):
                    if line is None:
                        continue
                    await resp.send(line)
                print('done!', uuid)
                await resp.send('close')
        except Exception as e:
            msg = 'General Error %s' % e
            resp.send(json.dumps({'e': 'err', 'msg': msg, 'uuid': 'general'}))
    return resp


async def download(request: web.Request):
    loop = request.app.loop

    uuid = request.match_info['id']
    writer = None
    csvf = None
    fields = None
    tasks = []

    class encoder():
        def __init__(self, out):
            self.out = out
        
        def write(self, i):
            tasks.append(self.out.write(i.encode('utf-8')))

    headers = copy(CORS_HEADERS)
    headers.update({'Content-Type': 'text/csv',
                    'Content-Disposition': 'attachment; filename=dppui.csv'})
    resp = web.StreamResponse(status=200,
                              reason='OK',
                              headers=headers)
    await resp.prepare(request)

    async with ProcessRunner(loop, uuid, True) as process:
        print('downloading!', uuid)
        async for line in LineReader(process.stderr):
            if line is None:
                continue
            line = json.loads(line)
            if 'e' in line:
                if line['e'] == 'rs':
                    fields = dict(
                        (x['name'], x) for x in line['data']
                    )
                    writer = csv.DictWriter(encoder(resp),
                                            [x['name'] for x in line['data']])
                    writer.writeheader()
                    csvf = CSVFormat()
                    await asyncio.gather(*tasks)
                    tasks = []
                    await resp.drain()
                elif line['e'] == 'r' and writer is not None:
                    csvf.write_row(writer, line['data'], fields)
                    # writer.writerow(line['data'])
                    await asyncio.gather(*tasks)
                    tasks = []
                    await resp.drain()

    return resp


async def config(request: web.Request):
    body = await request.json()
    id = request.query.get('id')
    if id:
        if not os.path.exists(path_for_id(id)):
            id = None
    if not id:
        id = uuid.uuid4().hex
        if not os.path.exists(path_for_id(id)):
            os.mkdir(path_for_id(id))

    with open(path_for_id(id, 'datapipes.source-spec.yaml'), 'w') as conf:
        print(body)
        yaml.dump(body, conf)
    return web.json_response({'ok': True, 'id': id}, headers=CORS_HEADERS)


async def config_options(request: web.Request):
    return web.json_response({}, headers=CORS_HEADERS)


app = web.Application()
app.router.add_route('GET', '/events/{id}', events)
app.router.add_route('GET', '/download/{id}', download)
app.router.add_route('POST', '/config', config)
app.router.add_route('OPTIONS', '/config', config_options)

if __name__ == "__main__":
    web.run_app(app, host='127.0.0.1', port=8000)
