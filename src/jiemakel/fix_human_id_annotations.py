# %%
from typing import Any
from sqlalchemy import text
from tqdm.auto import tqdm
from hereutil import here, add_to_sys_path
add_to_sys_path(here())  # noqa
from src.common_basis import *

# %%
import re
import json
import csv
with open(here("data/work/annotations/id_annotations.csv"),"w") as wf:
    cw = csv.writer(wf)
    cw.writerow(["permalink","annotator","aset","indirect_disagreement"])
    for f in here("data/work/annotations").glob("*.json"):
        aset = re.sub("[^123]*", "", f.name)
        annotator = re.sub(".json","",re.sub(".*-","",f.name))
        print(annotator, aset)
        with open(f,'r') as fp:
            d = json.load(fp)
            for i in d:
                if type(i['id']) is not int:
                    cw.writerow([i['permalink'],annotator,aset,i['id'][0]['rating']])
# %%
