import dagster as dg

from extract import lichess


modules = [lichess]

# defs = dg.Definitions(
#     assets = dg.load_assets_from_modules(modules),
#     asset_checks = dg.load_asset_checks_from_modules(modules),
#     sensors = 
# )

defs = dg.Definitions.merge(
    dg.Definitions(
        assets = dg.load_assets_from_modules(modules),
        asset_checks = dg.load_asset_checks_from_modules(modules),
        jobs = [lichess.lichess_job],
        sensors = [lichess.partition_sensor]))
