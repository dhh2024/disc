from dagster import Definitions, load_assets_from_modules

from dagster_assets import aita, cmw, eli5

aita_assets = load_assets_from_modules([aita], group_name="aita")
eli5_assets = load_assets_from_modules([eli5], group_name="eli5")
cmw_assets = load_assets_from_modules([cmw], group_name="cmw")

defs = Definitions(
    assets=[*aita_assets, *eli5_assets, *cmw_assets]
)
