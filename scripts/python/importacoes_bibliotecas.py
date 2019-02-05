# -*- coding: utf-8 -*-

#IMPORTS

import io 
import pandas as pd
import numpy as np
import datetime
import shutil
import os
import re
import zipfile
import sys
import unicodedata
import subprocess

from subprocess import Popen, PIPE
from hdfs import InsecureClient
from time import gmtime, strftime
from unicodedata import normalize