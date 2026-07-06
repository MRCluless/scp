import fastf1

fastf1.Cache.enable_cache('./.f1cache')

session = fastf1.get_session(2026, 'BRITISH', 'R')

session.load()

laps = session.laps

ferrari_laps = laps.pick_drivers(["HAM", "LEC"])

ferrari_laps.to_csv("laps.csv")