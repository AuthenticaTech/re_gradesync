from re_gradesync.assets import m365_users

def test_daily_temperature_highs():
    df_users = m365_users()
    assert df_users is not None # It returned something
