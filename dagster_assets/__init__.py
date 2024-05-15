from dagster import Definitions, load_assets_from_modules

from dagster_assets import aita, cmw, eli5, stop_arguing, random_sample, best_of_reddit, unpopularopinion

aita_assets = load_assets_from_modules([aita], group_name="aita")
eli5_assets = load_assets_from_modules([eli5], group_name="eli5")
cmw_assets = load_assets_from_modules([cmw], group_name="cmw")
stop_arguing_assets = load_assets_from_modules(
    [stop_arguing], group_name="stop_arguing")
unpopularopinion = load_assets_from_modules(
    [unpopularopinion], group_name="unpopularopinion")
random_sample_assets = load_assets_from_modules(
    [random_sample], group_name="random_sample")
best_of_reddit_assets = load_assets_from_modules(
    [best_of_reddit], group_name="best_of_reddit")

defs = Definitions(
    assets=[*aita_assets, *eli5_assets, *cmw_assets,
            *stop_arguing_assets, *random_sample_assets, *unpopularopinion, *best_of_reddit_assets],
)
