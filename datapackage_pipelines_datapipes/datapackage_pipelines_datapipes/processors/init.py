from datapackage_pipelines.wrapper import ingest, spew


def main():
    ingest()

    spew({'name': 'dp', 'resources': []}, [])

if __name__ == '__main__':
    main()