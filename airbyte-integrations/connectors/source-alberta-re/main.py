#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_alberta_re import SourceAlbertaRe

if __name__ == "__main__":
    source = SourceAlbertaRe()
    launch(source, sys.argv[1:])
