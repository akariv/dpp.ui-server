import os


def main(*args, **kwargs):
    original_directory = os.getcwd()
    try:
        os.chdir(os.path.join(os.path.dirname(__file__), 'workdir'))

        # We import after setting env vars because these are read on import.
        from datapackage_pipelines.cli import cli
        cli(*args, **kwargs)
    finally:
        os.chdir(original_directory)
